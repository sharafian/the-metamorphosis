const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const consumer = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'authorizer'
}, 'incoming-fulfill-condition')
const offset = new kafka.Offset(client)
const offsetFetch = util.promisify(offset.fetch.bind(offset))
const crypto = require('crypto')

consumer.on('message', async (message) => {
  const transferId = message.partition
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process incoming-fulfill-condition', id)

  // TODO should we just use a plain key-value store for this?
  const transfer = await offsetFetch([{
    topic: 'incoming-send-transfer',
    partition: transferId,
    maxNum: 1,
    time: -1
  }])

  const fulfillment = body && body[1]

  if (!tranfer || !fulfillment) {
    console.log('got incoming-fulfill-condition with no transfer or fulfillment. fulfillment: ', fulfillment, 'transfer:', transfer)
    // TODO do something
    return
  }

  if (!hash(fulfillment).equals(Buffer.from(transfer.executionCondition, 'base64'))) {
    console.log(`got incoming-fulfill-condition where fulfillment doesn't match condition. transfer: ${transferId}, fulfillment: ${fulfillment}, condition: ${transfer.executionCondition}`)
  }

  await util.promisify(producer.send.bind(producer))([{
    topic: 'outgoing-fulfill-condition',
    messages: Buffer.from(JSON.stringify({
      id,
      transferId,
      fulfillment,
      prefix: transfer.prefix,
      method
    })),
    timestamp: Date.now()
  }])
})

client.once('ready', () => console.log('listening for incoming-fulfill-condition'))
consumer.on('error', error => console.error(error))
producer.on('error', error => console.error(error))

function hash (preimage) {
  const h = crypto.createHash('sha256')
  h.update(Buffer.from(preimage, 'base64'))
  return h.digest()
}
