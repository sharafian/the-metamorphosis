const kafka = require('kafka-node')
const { Reader } = require('oer-utils')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const IlpPacket = require('ilp-packet')
const produce = util.promisify(producer.send.bind(producer))

// TODO: split this shit up
const incomingTransfers = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'incomingTransfers'
}, 'incoming-send-transfer')

const incomingRequests = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'incomingRequests'
}, 'incoming-send-request')

function getNextHop (destination) {
  return { connectorLedger: 'test.east.', connectorAccount: 'test.east.server' }
}

function getNextAmount (sourceLedger, destinationLedger, amount) {
  return amount
}

function routeTransfer (prefix, packet) {
  const packetBuffer = Buffer.from(packet, 'base64')
  const { account, amount } = IlpPacket.deserializeIlpPayment(packetBuffer)

  const nextHop = getNextHop(account)
  const nextAmount = getNextAmount({
    sourceLedger: prefix,
    destinationLedger: nextHop,
    amount: amount
  })

  return { nextHop, nextAmount }
}

incomingTransfers.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process incoming-send-transfer', id)

  const transfer = body[0]

  const { nextHop, nextAmount } = routeTransfer(prefix, transfer.ilp)
  const nextExpiry = new Date(Date.parse(transfer.expiresAt) - 1000).toISOString()

  const nextTransfer = [ {
    id: transfer.id, // TODO: unwise
    ledger: nextHop.connectorLedger,
    to: nextHop.connectorAccount,
    amount: nextAmount,
    expiresAt: nextExpiry,
    executionCondition: transfer.executionCondition,
    ilp: transfer.ilp
  } ]

  await produce([{
    topic: 'outgoing-rpc-requests',
    messages: Buffer.from(JSON.stringify({
      id,
      method,
      prefix: nextHop.connectorLedger,
      body: nextTransfer
    })),
    timestamp: Date.now()
  }])
})

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
    ilp: request.ilp
  } ]

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
})

client.once('ready', () => {
  console.log('listening for incoming-send-transfer')
  console.log('listening for incoming-send-request')
})

incomingTransfers.on('error', error => console.error(error))
incomingRequests.on('error', error => console.error(error))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
