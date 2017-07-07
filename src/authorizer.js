const kafka = require('kafka-node')
const util = require('util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const consumer = new kafka.ConsumerGroup({
  host: 'localhost:2181'
}, 'incoming-rpc-requests')

const allowedMethods = {
  'send_transfer': 'incoming-send-transfer',
  'fulfill_condition': 'incoming-fulfill-condition',
  'send_request': 'incoming-send-request'
}

consumer.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  const topic = allowedMethods[method]

  if (!topic) {
    //TODO: do soemthing
    return
  }

  await util.promisify(producer.send.bind(producer))([{
    topic,
    messages: Buffer.from(JSON.stringify({ id, body, prefix })),
    timestamp: Date.now()
  }])
})
