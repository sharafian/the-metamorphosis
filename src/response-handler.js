const agent = require('superagent')
const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const produce = util.promisify(producer.send.bind(producer))
const consumer = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'responseHandler'
}, 'outgoing-rpc-responses')

consumer.on('message', async (message) => {
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

client.once('ready', () => console.log('listening for outgoing-rpc-responses'))
consumer.on('error', error => console.error(error))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
