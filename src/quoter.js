const kafka = require('kafka-node')
const { Reader } = require('oer-utils')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const IlpPacket = require('ilp-packet')
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
  packetReader.readUInt8()
  packetReader.readLengthPrefix()
  const account = packetReader.readVarOctetString().toString()
  const nextHop = getNextHop(account)

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
