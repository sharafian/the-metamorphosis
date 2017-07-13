const kafka = require('kafka-node')
const { Reader, Writer } = require('oer-utils')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const IlpPacket = require('ilp-packet')
const base64url = require('base64url')
const uint64 = require('ilp-packet/dist/src/utils/uint64')
const produce = util.promisify(producer.send.bind(producer))
const { getNextAmount, getPreviousAmount, getNextHop, applyRateToLiquidityCurve } = require('./lib/routing')
const BigNumber = require('bignumber.js')

const RATE_EXPIRY_DURATION = 360000
const MIN_MESSAGE_WINDOW = 1000

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

client.once('ready', () => {
  console.log('listening for incoming-send-request')
  console.log('listening for outgoing-rpc-responses')
})
client.on('error', error => console.error(error))
incomingRequests.on('message', handleIncomingRequests)
incomingRequests.on('error', error => console.error(error))
outgoingResponses.on('message', handleOutgoingResponses)
outgoingResponses.on('error', error => console.error(error))
producer.on('error', error => console.error(error))

async function handleIncomingRequests (message) {
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
  const nextHop = getNextHop(destinationAccount)

  console.log('quoter next hop:', nextHop)

  // TODO handle when rate is not found (send errors back)

  // Local quote
  if (nextHop.isLocal) {

    let responsePacket
    if (type === IlpPacket.Type.TYPE_ILQP_BY_SOURCE_REQUEST) {
      // Apply our rate to the source amount

      const previousAmount = uint64.twoNumbersToString(contentsReader.readUInt64())
      const destinationHoldDuration = contentsReader.readUInt32()
      const destinationAmount = getNextAmount({
        previousLedger: prefix,
        nextLedger: nextHop.connectorLedger,
        previousAmount
      })
      responsePacket = IlpPacket.serializeIlqpBySourceResponse({
        destinationAmount,
        sourceHoldDuration: destinationHoldDuration + MIN_MESSAGE_WINDOW
      })
    } else if (type === IlpPacket.Type.TYPE_ILQP_BY_DESTINATION_REQUEST) {
      // Apply our rate to the destination amount (because it's local)

      const destinationAmount = uint64.twoNumbersToString(contentsReader.readUInt64())
      const previousAmount = getPreviousAmount({
        previousLedger: prefix,
        nextLedger: nextHop.connectorLedger,
        nextAmount
      })
      const destinationHoldDuration = contentsReader.readUInt32()
      responsePacket = IlpPacket.serializeIlqpByDestinationResponse({
        sourceAmount: previousAmount,
        sourceHoldDuration: destinationHoldDuration + MIN_MESSAGE_WINDOW
      })
    } else if (type === IlpPacket.Type.TYPE_ILQP_LIQUIDITY_REQUEST) {
      // Apply our rate to the curve

      const destinationHoldDuration = contentsReader.readUInt32()
      // TODO base max amount on max payment size
      const probePreviousAmount = new BigNumber(1000000000000)
      const previousAmountHex = probePreviousAmount.toString(16)
      const probeDestinationAmount = getNextAmount({
        previousLedger: prefix,
        nextLedger: nextHop.connectorLedger,
        previousAmount: probePreviousAmount
      })
      const destinationAmountHex = probeDestinationAmount.toString(16)
      // TODO there's probably a more elegant way of working with liquidity curves
      const liquidityCurve = Buffer.concat([
        // TODO base minimum amount on min payment size
        Buffer.from('00000000000000000000000000000000', 'hex'), // [0,0]
        Buffer.from('0'.repeat(16 - previousAmountHex.length) + previousAmountHex, 'hex'),
        Buffer.from('0'.repeat(16 - destinationAmountHex.length) + destinationAmountHex, 'hex')
      ])
      responsePacket = IlpPacket.serializeIlqpLiquidityResponse({
        liquidityCurve,
        appliesToPrefix: nextHop.connectorLedger,
        sourceHoldDuration: destinationHoldDuration + MIN_MESSAGE_WINDOW,
        expiresAt: new Date(Date.now() + RATE_EXPIRY_DURATION)
      })
    }

    // Respond with local quote
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
  } else {
    // Remote quotes

    // When remote quoting by source amount we need to adjust
    // the amount we ask our peer for by our rate
    let nextIlpPacket
    if (type === IlpPacket.Type.TYPE_ILQP_BY_SOURCE_REQUEST) {
      const previousAmount = uint64.twoNumbersToString(contentsReader.readUInt64())
      const destinationHoldDuration = contentsReader.readUInt32()
      const nextAmount = getNextAmount({
        previousLedger: prefix,
        nextLedger: nextHop.connectorLedger,
        previousAmount
      })
      nextIlpPacket = IlpPacket.serializeIlqpBySourceRequest({
        destinationAccount,
        sourceAmount: nextAmount,
        destinationHoldDuration
      })
    } else {
      // If it's a fixed destination amount or liquidity quote we'll apply our rate to the response

      outgoingQuoteRequests[id] = { previousLedger: prefix }
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
}

async function handleOutgoingResponses (message) {
  const { id, method, prefix, body, error } = JSON.parse(message.value)
  console.log('process outgoing-rpc-responses', id)

  if (method !== 'send_request') return

  if (error) {
    try {
      await produce([{
        topic: 'incoming-rpc-responses',
        messages: Buffer.from(JSON.stringify({
          error
        }), 'utf8'),
        timestamp: Date.now()
      }])
    } catch (err) {
      console.error('error producing to incoming-send-request-responses', id, err)
    }
  }

  const packet = message.value
  const packetReader = new Reader(Buffer.from(packet, 'base64'))
  const type = packetReader.readUInt8()
  const contentsReader = new Reader(packetReader.readVarOctetString())

  // When responding to quote by destination amount requests
  // we need to adjust the return value by our rate
  let responseIlpPacket
  if (type === IlpPacket.Type.TYPE_ILQP_BY_DESTINATION_RESPONSE) {
    // Apply our rate to adjust the source amount

    const quoteRequestDetails = outgoingQuoteRequests[id]
    if (!quoteRequestDetails) {
      // TODO make it so the messages are partitioned and each instance only gets
      // responses to the quote requests it sent out
      console.log('got quote response for quote we do not know about', message)
      return
    }
    const previousLedger = quoteRequestDetails.previousLedger

    const nextAmount = uint64.twoNumbersToString(contentsReader.readUInt64())
    const nextHoldDuration = contentsReader.readUInt32()
    const previousAmount = getPreviousAmount({
      previousLedger,
      nextLedger: prefix,
      nextAmount
    })
    responseIlpPacket = IlpPacket.serializeIlqpByDestinationResponse({
      sourceAmount: previousAmount,
      sourceHoldDuration: nextHoldDuration + MIN_MESSAGE_WINDOW
    })
  } else if (type === IlpPacket.Type.TYPE_ILQP_LIQUIDITY_RESPONSE) {
    // Apply our rate to the liquidity curve we got back from the quote

    const quoteRequestDetails = outgoingQuoteRequests[id]
    if (!quoteRequestDetails) {
      // TODO make it so the messages are partitioned and each instance only gets
      // responses to the quote requests it sent out
      console.log('got quote response for quote we do not know about', message)
      return
    }
    const previousLedger = quoteRequestDetails.previousLedger

    const numPoints = packetReader.readVarUInt()
    const liquidityCurve = packetReader.readOctetString(numPoints * 16)
    const rate = getRate({
      previousLedger,
      nextLedger: prefix
    })
    const appliesToPrefix = packetReader.readVarOctetString()
    // TODO add time to the hold duration
    const nextHoldDuration = packetReader.readUInt32()
    const expiresAt = Date.parse(packetReader.readVarOctetString().toString('ascii'))
    const ourQuoteExpiry = Date.parse(Date.now() + RATE_EXPIRY_DURATION)

    const newCurve = applyRateToLiquidityCurve({ rate, liquidityCurve })
    responseIlpPacket = IlpPacket.serializeIlqpLiquidityResponse({
      liquidityCurve: newCurve,
      appliesToPrefix,
      sourceHoldDuration: nextHoldDuration + MIN_MESSAGE_WINDOW,
      expiresAt: new Date(Math.min(expiresAt, ourQuoteExpiry))
    })

  } else {
    responseIlpPacket = packet
  }

  try {
    await produce([{
      topic: 'incoming-rpc-responses',
      messages: Buffer.from(JSON.stringify({
        body: responseIlpPacket.toString('base64')
      }), 'utf8'),
      timestamp: Date.now()
    }])
  } catch (err) {
    console.error('error producing to incoming-send-request-responses', id, err)
  }
}

