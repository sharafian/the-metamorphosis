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

async function routeTransfer (sourceTransfer) {
  const packetBuffer = Buffer.from(sourceTransfer.ilp, 'base64')
  const { account, amount } = IlpPacket.deserializeIlpPayment(packetBuffer)

  const nextHop = await getNextHop(account)
  let nextAmount
  if (nextHop.isLocal) {
    nextAmount = amount
  } else {
    nextAmount = await getNextAmount({
      sourceLedger: sourceTransfer.ledger,
      sourceAmount: sourceTransfer.amount,
      destinationLedger: nextHop.connectorLedger,
    })
  }

  return { nextHop, nextAmount }
}

incomingTransfers.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process incoming-send-transfer', id)

  const transfer = body[0]
  let nextHop
  let nextAmount
  try {
    const routeResult = await routeTransfer(transfer)
    nextHop = routeResult.nextHop
    nextAmount = routeResult.nextAmount
  } catch (err) {
    // TODO send a reject message back
    console.error('error routing transfer', id, err)
    return
  }
  const nextExpiry = new Date(Date.parse(transfer.expiresAt) - 1000).toISOString()

  const nextTransfer = {
    id: transfer.id, // TODO: unwise
    ledger: nextHop.connectorLedger,
    to: nextHop.connectorAccount,
    from: nextHop.connectorLedger + 'client',
    amount: nextAmount,
    expiresAt: nextExpiry,
    executionCondition: transfer.executionCondition,
    ilp: transfer.ilp
  }

  console.log('constructed next transfer', id, nextTransfer)

  try {
    await produce([{
      topic: 'outgoing-rpc-requests',
      messages: Buffer.from(JSON.stringify({
        id,
        method,
        prefix: nextHop.connectorLedger,
        body: [nextTransfer]
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
