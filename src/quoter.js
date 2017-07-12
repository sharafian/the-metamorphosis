const kafka = require('kafka-node')
const { Reader, Writer } = require('oer-utils')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const IlpPacket = require('ilp-packet')
const base64url = require('base64url')
const uint64 = require('ilp-packet/dist/src/utils/uint64')
const produce = util.promisify(producer.send.bind(producer))
const { getNextAmount, getPreviousAmount, getNextHop } = require('./lib/routing')
const BigNumber = require('bignumber.js')

const RATE_EXPIRY_DURATION = 360000

const incomingRequests = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'incomingRequests'
}, 'incoming-send-request')

const outgoingResponses = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'responseHandler'
}, 'outgoing-rpc-responses')

// TODO use a real KV store or other method to associate requests and responses
const outgoingQuoteRequests = {}

incomingRequests.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process incoming-send-request', id)
  const request = body[0]

  // respond to route broadcasts so we don't look like we're down
  if (request.custom && request.custom.method === 'broadcast_routes') {
    await produce([{
      topic: 'incoming-rpc-responses',
      messages: Buffer.from(JSON.stringify({ id, body: {
        to: request.to,
        from: request.from,
        ledger: prefix
      } })),
      timestamp: Date.now()
    }])
    return
  }

  const packetReader = new Reader(Buffer.from(request.ilp, 'base64'))
  const type = packetReader.readUInt8()
  const contentsReader = new Reader(packetReader.readVarOctetString())
  const destinationAccount = contentsReader.readVarOctetString().toString('ascii')
  const nextHop = await getNextHop(destinationAccount)

  console.log('quoter next hop:', nextHop)

  // TODO handle when rate is not found (send errors back)

  // Local quote
  if (nextHop.isLocal) {

    let responsePacket
    if (type === IlpPacket.Type.TYPE_ILQP_BY_SOURCE_REQUEST) {
      // apply our rate to the source amount
      const sourceAmount = uint64.twoNumbersToString(contentsReader.readUInt64())
      const destinationHoldDuration = contentsReader.readUInt32()
      const destinationAmount = await getNextAmount({
        sourceLedger: prefix,
        destinationLedger: nextHop.connectorLedger,
        sourceAmount
      })
      responsePacket = IlpPacket.serializeIlqpBySourceResponse({
        destinationAmount,
        sourceHoldDuration: destinationHoldDuration
      })
    } else if (type === IlpPacket.Type.TYPE_ILQP_BY_DESTINATION_REQUEST) {
      // apply our rate to the destination amount (because it's local)
      const destinationAmount = uint64.twoNumbersToString(contentsReader.readUInt64())
      const sourceAmount = await getPreviousAmount({
        sourceLedger: prefix,
        destinationLedger: nextHop.connectorLedger,
        destinationAmount
      })
      const destinationHoldDuration = contentsReader.readUInt32()
      responsePacket = IlpPacket.serializeIlqpByDestinationResponse({
        sourceAmount,
        sourceHoldDuration: destinationHoldDuration
      })
    } else if (type === IlpPacket.Type.TYPE_ILQP_LIQUIDITY_REQUEST) {
      // apply our rate to the curve
      const destinationHoldDuration = contentsReader.readUInt32()
      // TODO base max amount on max payment size
      const probeSourceAmount = new BigNumber(1000000000000)
      const sourceAmountHex = probeSourceAmount.toString(16)
      const probeDestinationAmount = await getNextAmount({
        sourceLedger: prefix,
        destinationLedger: nextHop.connectorLedger,
        sourceAmount: probeSourceAmount
      })
      const destinationAmountHex = probeDestinationAmount.toString(16)
      // TODO there's probably a more elegant way of working with liquidity curves
      const liquidityCurve = Buffer.concat([
        // TODO base minimum amount on min payment size
        Buffer.from('00000000000000000000000000000000', 'hex'), // [0,0]
        Buffer.from('0'.repeat(16 - sourceAmountHex.length) + sourceAmountHex, 'hex'),
        Buffer.from('0'.repeat(16 - destinationAmountHex.length) + destinationAmountHex, 'hex')
      ])
      responsePacket = IlpPacket.serializeIlqpLiquidityResponse({
        liquidityCurve,
        appliesToPrefix: nextHop.connectorLedger,
        sourceHoldDuration: destinationHoldDuration,
        expiresAt: new Date(Date.now() + RATE_EXPIRY_DURATION)
      })
    }

    const response = {
      to: nextHop.connectorAccount,
      ledger: nextHop.connectorLedger,
      ilp: base64url(responsePacket)
    }

    console.log('returning local quote for', id)
    await produce([{
      topic: 'incoming-rpc-responses',
      messages: Buffer.from(JSON.stringify({ id, body: response })),
      timestamp: Date.now()
    }])

    return
  } else {
    // Remote quotes

    // When remote quoting by source amount we need to adjust
    // the amount we ask our peer for by our rate
    let nextIlpPacket
    if (type === IlpPacket.Type.TYPE_ILQP_BY_SOURCE_REQUEST) {
      const sourceAmount = uint64.twoNumbersToString(contentsReader.readUInt64())
      const destinationHoldDuration = contentsReader.readUInt32()
      const nextAmount = await getNextAmount({
        sourceLedger: prefix,
        destinationLedger: nextHop.connectorLedger,
        sourceAmount
      })
      nextIlpPacket = IlpPacket.serializeIlqpBySourceRequest({
        destinationAccount,
        sourceAmount: nextAmount,
        destinationHoldDuration
      })
    } else {
      outgoingQuoteRequests[id] = { sourceLedger: prefix }
      nextIlpPacket = request.ilp
    }

    // Remote quote
    const nextRequest = [ {
      ledger: nextHop.connectorLedger,
      to: nextHop.connectorAccount,
      from: nextHop.connectorLedger + 'client',
      ilp: nextIlpPacket
    } ]

    try {
      await produce([{
        topic: 'outgoing-rpc-requests',
        messages: Buffer.from(JSON.stringify({
          id,
          method,
          prefix: nextHop.connectorLedger,
          fromPrefix: prefix,
          body: nextRequest
        })),
        timestamp: Date.now()
      }])
    } catch (err) {
      console.error('error producing to outgoing-rpc-request', id, err)
    }
  }
})

outgoingResponses.on('message', async (message) => {
  const { id, method, body, prefix } = JSON.parse(message.value)
  console.log('process outgoing-rpc-responses', id)

  if (method !== 'send_request') return

  const packet = message.value
  const packetReader = new Reader(Buffer.from(packet, 'base64'))
  const type = packetReader.readUInt8()
  const contentsReader = new Reader(packetReader.readVarOctetString())

  let responseIlpPacket
  // When responding to quote by destination amount requests
  // we need to adjust the return value by our rate
  if (type === IlpPacket.Type.TYPE_ILQP_BY_DESTINATION_RESPONSE) {
    const quoteRequestDetails = outgoingQuoteRequests[id]
    if (!quoteRequestDetails) {
      // TODO make it so the messages are partitioned and each instance only gets
      // responses to the quote requests it sent out
      console.log('got quote response for quote we do not know about', message)
      return
    }
    const sourceLedger = quoteResponseDetails.sourceLedger

    const destinationAmount = uint64.twoNumbersToString(contentsReader.readUInt64())
    const nextHoldDuration = contentsReader.readUInt32()
    const sourceAmount = await getPreviousAmount({
      sourceLedger,
      destinationLedger: prefix,
      destinationAmount
    })
    responseIlpPacket = IlpPacket.serializeIlqpByDestinationResponse({
      sourceAmount,
      sourceHoldDuration: nextHoldDuration
    })
  } else if (type === IlpPacket.Type.TYPE_ILQP_LIQUIDITY_RESPONSE) {
    // apply our rate to the liquidity curve we got back from the quote
    // TODO do we really need to support liquidity curve quoting?

    const quoteRequestDetails = outgoingQuoteRequests[id]
    if (!quoteRequestDetails) {
      // TODO make it so the messages are partitioned and each instance only gets
      // responses to the quote requests it sent out
      console.log('got quote response for quote we do not know about', message)
      return
    }
    const sourceLedger = quoteResponseDetails.sourceLedger

    const numPoints = packetReader.readVarUInt()
    const liquidityCurve = packetReader.readOctetString(numPoints * 16)
    const rate = await getRate({
      sourceLedger,
      destinationLedger: prefix
    })
    const appliesToPrefix = packetReader.readVarOctetString()
    // TODO add time to the hold duration
    const sourceHoldDuration = packetReader.readUInt32()
    const expiresAt = Date.parse(packetReader.readVarOctetString().toString('ascii'))
    const ourQuoteExpiry = Date.parse(Date.now() + RATE_EXPIRY_DURATION)

    const newCurve = applyRateToLiquidityCurve({ rate, liquidityCurve })
    responseIlpPacket = IlpPacket.serializeIlqpLiquidityResponse({
      liquidityCurve: newCurve,
      appliesToPrefix,
      sourceHoldDuration,
      expiresAt: new Date(Math.min(expiresAt, ourQuoteExpiry))
    })

  } else {
    responseIlpPacket = packet
  }

  try {
    await produce([{
      topic: 'incoming-rpc-responses',
      messages: responseIlpPacket,
      timestamp: Date.now()
    }])
  } catch (err) {
    console.error('error producing to incoming-send-request-responses', id, err)
  }
})

function applyRateToLiquidityCurve ({ rate, liquidityCurve }) {
  let index
  const writer = new Writer()
  const reader = Reader.from(liquidityCurve)
  while (index < liquidityCurve.length) {
    // divide source amount by rate
    // TODO avoid translating the format so many times, it's probably pretty slow
    const sourceAmount = uint64.twoNumbersToString(reader.readUInt64())
    const newSourceAmount = new BigNumber(sourceAmount)
      .div(rate)
      .round()
      .toString(10)
    const newSourceAmount64 =
    writer.writeUInt64(uint64.stringToTwoNumbers(newSourceAmount))

    // leave destination amount unchanged
    writer.writeUInt64(reader.readUInt64())
  }
  return writer.getBuffer()
}

client.once('ready', () => {
  console.log('listening for incoming-send-request')
  console.log('listening for outgoing-rpc-responses')
})

incomingRequests.on('error', error => console.error(error))
outgoingResponses.on('error', error => console.error(error))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
