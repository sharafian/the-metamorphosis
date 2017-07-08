const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const incomingFulfill = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'authorizer'
}, 'incoming-fulfill-condition')
const incomingTransfer = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'authorizer'
}, 'incoming-send-transfer')
const crypto = require('crypto')

// TODO put them in a better KV store
const sourceTransfers = {}

incomingTransfer.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  const transfer = body[0]
  sourceTransfers[transfer.id] = transfer
})

incomingFulfill.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  const transferId = body && body[0]
  console.log('process incoming-fulfill-condition', id)

  const transfer = sourceTransfers[transferId]
  if (!transfer) {
    console.error('no transfer found for fulfillment', id, 'transferId:', transferId)
  }

  const fulfillment = body && body[1]

  if (!transfer || !fulfillment) {
    console.log('got incoming-fulfill-condition with no fulfillment. fulfillment: ', fulfillment)
    return
  }

  console.log('found transfer', id, transfer, 'for fulfillment:', fulfillment)

  if (!hash(fulfillment).equals(Buffer.from(transfer.executionCondition, 'base64'))) {
    console.log(`got incoming-fulfill-condition where fulfillment doesn't match condition. transfer: ${transferId}, fulfillment: ${fulfillment}, condition: ${transfer.executionCondition}`)
  }

  try {
    await util.promisify(producer.send.bind(producer))([{
      topic: 'outgoing-rpc-requests',
      messages: Buffer.from(JSON.stringify({
        id,
        prefix: transfer.ledger,
        body: [transferId, fulfillment],
        method
      })),
      timestamp: Date.now()
    }])
  } catch (err) {
    console.error('error producing to outgoing-fulfill-condition', id, err)
  }

  delete sourceTransfers[transferId]
})

client.once('ready', () => console.log('listening for incoming-fulfill-condition'))
incomingTransfer.on('error', error => console.error(error))
incomingFulfill.on('error', error => console.error(error))
producer.on('error', error => console.error(error))

function hash (preimage) {
  const h = crypto.createHash('sha256')
  h.update(Buffer.from(preimage, 'base64'))
  return h.digest()
}
