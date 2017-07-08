const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client, {
  partitionerType: 3 // keyed partitioner
})
const produce = util.promisify(producer.send.bind(producer))
const consumer = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'authorizer'
}, 'incoming-rpc-requests')

const allowedMethods = {
  'send_transfer': 'incoming-send-transfer',
  'fulfill_condition': 'incoming-fulfill-condition',
  'send_request': 'incoming-send-request'
}

consumer.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process incoming-rpc-requests', id)

  switch (method) {
    case 'send_transfer':
      const sendTransferId = body && body[0] && body[0].id
      await produce([{
        topic: 'incoming-send-transfer',
        messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
        timestamp: Date.now(),
        key: sendTransferId
      }])
      break
    case 'fulfill_condition':
      const fulfillTransferId = body && body[0]
      await produce([{
        topic: 'incoming-fulfill-condition',
        messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
        timestamp: Date.now(),
        key: fulfillTransferId
      }])
      break
    case 'send_request':
      await produce([{
        topic: 'incoming-send-request',
        messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
        timestamp: Date.now(),
        key: id
        // TODO is there an intelligent way to partiion requests?
      }])
      break

    default:
      return
  }
})

client.once('ready', () => console.log('listening for incoming-rpc-requests'))
consumer.on('error', error => console.error(error))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
