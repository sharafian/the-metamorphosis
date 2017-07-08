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
  return { 'test.east.': { uri: 'http://ilp-kit2:4010/api/peers/rpc', token: 'token' },
    'test.west.': { uri: 'http://ilp-kit1:3010/api/peers/rpc', token: 'token' }}[prefix]
}

outgoingRpcRequests.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process outgoing-rpc-requests', id)

  const { uri, token } = getPeerRpcInfo(prefix)

  let response
  try {
    response = await agent
      .post(uri)
      .query({ method, prefix })
      .set('Authorization', 'Bearer ' + token)
      .send(body)
  } catch (err) {
    console.error(`error sending rpc request ${id} to ${prefix} (${uri})`, err)
  }

  try {
    await produce([{
      topic: 'outgoing-rpc-responses',
      messages: Buffer.from(JSON.stringify({ id, method, prefix, body: response.body })),
      timestamp: Date.now()
    }])
  } catch (err) {
    console.error('error producing to outgoing-rpc-responses', id, err)
  }
})

console.log('listening for outgoing-rpc-requests')
outgoingRpcRequests.on('error', error => console.error(error))

client.once('ready', () => console.log('listening for outgoing-rpc-requests'))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
