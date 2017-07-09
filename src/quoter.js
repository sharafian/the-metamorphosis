const kafka = require('kafka-node')
const { Reader } = require('oer-utils')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const IlpPacket = require('ilp-packet')
const base64url = require('base64url')
const uint64 = require('ilp-packet/dist/src/utils/uint64')
const produce = util.promisify(producer.send.bind(producer))
const { getNextAmount, getNextHop } = require('./lib/routing')

const incomingRequests = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'incomingRequests'
}, 'incoming-send-request')

const outgoingResponses = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'responseHandler'
}, 'outgoing-rpc-responses')

incomingRequests.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process incoming-send-request', id)
  const request = body[0]

  const packetReader = new Reader(Buffer.from(request.ilp, 'base64'))
  const type = packetReader.readUInt8()
  const contentsReader = new Reader(packetReader.readVarOctetString())
  const account = contentsReader.readVarOctetString().toString('ascii')
  const nextHop = getNextHop(account)

  if (nextHop.isLocal) {
    const response = {
      to: nextHop.connectorAccount,
      ledger: nextHop.connectorLedger
    }

    if (type === IlpPacket.Type.TYPE_ILQP_BY_SOURCE_REQUEST) {
      const amount = uint64.twoNumbersToString(contentsReader.readUInt64())
      const hold = contentsReader.readUInt32()
      response.ilp = IlpPacket.serializeIlqpBySourcResponse({
        destinationAmount: amount,
        sourceHoldDuration: hold
      })
    } else if (type === IlpPacket.Type.TYPE_ILQP_BY_DESTINATION_REQUEST) {
      const amount = uint64.twoNumbersToString(contentsReader.readUInt64())
      const hold = contentsReader.readUInt32()
      response.ilp = IlpPacket.serializeIlqpBySourcResponse({
        sourceAmount: amount,
        sourceHoldDuration: hold
      })
    } else if (type === IlpPacket.Type.TYPE_ILQP_LIQUIDITY_REQUEST) {
      const hold = contentsReader.readUInt32()
      response.ilp = IlpPacket.serializeIlqpLiquidityResponse({
        liquidityCurve: Buffer.from('00000000000000000000000000000000ffffffffffffffffffffffffffffffff', 'hex'),
        appliesToPrefix: nextHop.connectorLedger,
        sourceHoldDuration: hold,
        expiresAt: new Date(Date.now() + 10000)
      })
    }

    console.log('returning local quote for', id)
    response.ilp = base64url(response.ilp)

    await produce([{
      topic: 'incoming-rpc-responses',
      messages: Buffer.from(JSON.stringify({ id, body: response })),
      timestamp: Date.now()
    }])

    return
  }

  const nextRequest = [ {
    ledger: nextHop.connectorLedger,
    to: nextHop.connectorAccount,
    from: nextHop.connectorLedger + 'client',
    ilp: request.ilp
  } ]

  try {
    await produce([{
      topic: 'outgoing-rpc-requests',
      messages: Buffer.from(JSON.stringify({
        id,
        method,
        prefix: nextHop.connectorLedger,
        body: nextRequest
      })),
      timestamp: Date.now()
    }])
  } catch (err) {
    console.error('error producing to outgoing-rpc-request', id, err)
  }
})

outgoingResponses.on('message', async (message) => {
  const { id, method, body } = JSON.parse(message.value)
  console.log('process outgoing-rpc-responses', id)

  if (method !== 'send_request') return

  try {
    await produce([{
      topic: 'incoming-rpc-responses',
      messages: Buffer.from(message.value, 'binary'),
      timestamp: Date.now()
    }])
  } catch (err) {
    console.error('error producing to incoming-send-request-responses', id, err)
  }
})

client.once('ready', () => {
  console.log('listening for incoming-send-request')
  console.log('listening for outgoing-rpc-responses')
})

incomingRequests.on('error', error => console.error(error))
outgoingResponses.on('error', error => console.error(error))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
