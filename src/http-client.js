const agent = require('superagent')
const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const produce = util.promisify(producer.send.bind(producer))
const consumer = new kafka.ConsumerGroup({
  host: 'localhost:2181'
}, 'outgoing-rpc-requests')

function getPeerRpcUri (prefix) {
  return 'http://localhost:8090/rpc/bob'
}

consumer.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process outgoing-rpc-requests', id)

  const { uri, token } = getPeerRpcInfo(prefix)

  const response = await agent({
    method: 'post',
    uri: `${uri}?method=${method}&prefix=${prefix}`,
    auth: { bearer: token },
    json: true,
    body
  })

  await produce([{
    topic: 'outgoing-rpc-responses',
    messages: Buffer.from(JSON.stringify({ id, method, prefix, body: response })),
    timestamp: Date.now()
  }])
})

console.log('listening for outgoing-rpc-requests')
consumer.on('error', error => console.error(error))
