const agent = require('superagent')
const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const produce = util.promisify(producer.send.bind(producer))
const outgoingRpcRequests = new kafka.ConsumerGroup({
  host: 'localhost:2181'
}, 'outgoing-rpc-requests')

const outgoingFulfillCondition = new kafka.ConsumerGroup({
  host: 'localhost:2181'
}, 'outgoing-fulfill-condition')

function getPeerRpcInfo (prefix) {
  return { uri: 'http://localhost:8090/rpc/bob', token: 'token' }
}

outgoingRpcRequests.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process outgoing-rpc-requests', id)

  const { uri, token } = getPeerRpcInfo(prefix)

  const response = await agent
    .post(uri)
    .query({ method, prefix })
    .set('Authorization', 'Bearer ' + token)
    .send(body)

  await produce([{
    topic: 'outgoing-rpc-responses',
    messages: Buffer.from(JSON.stringify({ id, method, prefix, body: response })),
    timestamp: Date.now()
  }])
})

console.log('listening for outgoing-rpc-requests')
outgoingRpcRequests.on('error', error => console.error(error))

outgoingFulfillCondition.on('message', async (message) => {
  const { id, fulfillment, transferId, method, prefix } = JSON.parse(message.value)
  console.log('process outgoing-rpc-requests', id)

  const { uri, token } = getPeerRpcInfo(prefix)

  const response = await agent
    .post(uri)
    .query({ method, prefix })
    .set('Authorization', 'Bearer ' + token)
    .send([transferId, fulfillment])
})

client.once('ready', () => console.log('listening for outgoing-rpc-requests'))
outgoingFulfillCondition.on('error', error => console.error(error))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
