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

const peers = require('../config/peers.json')
function getPeerRpcInfo (prefix) {
  return peers[prefix]
}

outgoingRpcRequests.on('message', async (message) => {
  const { id, body, method, prefix } = JSON.parse(message.value)
  console.log('process outgoing-rpc-requests', id)

  const peerInfo = getPeerRpcInfo(prefix)
  let error
  let responseBody
  if (!peerInfo) {
    console.error(`unknown peer ${id}: ${prefix}`)
    error = {
      status: 422,
      responseBody: 'Unknown peer: ' + prefix
    }
  } else {
    const { uri, token } = peerInfo
    try {
      const response = await agent
        .post(uri)
        .query({ method, prefix })
        .set('Authorization', 'Bearer ' + token)
        .send(body)
      responseBody = response.body
    } catch (err) {
      console.error(`error sending rpc request ${id} to ${prefix} (${uri})`, err)
      error = {
        status: 502,
        responseBody: 'error sending rpc request'
      }
    }
  }

  try {
    await produce([{
      topic: 'outgoing-rpc-responses',
      messages: Buffer.from(JSON.stringify({
        id,
        method,
        prefix,
        body: responseBody,
        error
      })),
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
