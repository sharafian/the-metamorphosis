const kafka = require('kafka-node')
const { Reader } = require('oer-utils')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const IlpPacket = require('ilp-packet')
const produce = util.promisify(producer.send.bind(producer))
const { getNextAmount, getNextHop } = require('./lib/routing')

const incomingTransfers = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'incomingTransfers'
}, 'incoming-send-transfer')

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
    from: nextHop.connectorLedger + 'client',
    amount: nextAmount,
    expiresAt: nextExpiry,
    executionCondition: transfer.executionCondition,
    ilp: transfer.ilp
  } ]

  try {
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
  } catch (err) {
    console.error('error producing to outgoing-rpc-request', id, err)
  }
})

client.once('ready', () => {
  console.log('listening for incoming-send-transfer')
})

incomingTransfers.on('error', error => console.error(error))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
