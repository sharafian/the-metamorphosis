const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const produce = util.promisify(producer.send.bind(producer))
const consumer = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'authorizer'
}, 'incoming-rpc-requests')

const peers = require('../config/peers')
const allowedMethods = {
  'send_transfer': 'incoming-send-transfer',
  'fulfill_condition': 'incoming-fulfill-condition',
  'send_request': 'incoming-send-request'
}

consumer.on('message', async (message) => {
  const { id, body, method, prefix, auth } = JSON.parse(message.value)
  console.log('process incoming-rpc-requests', id)
  
  const peer = peers[prefix]
  if (!peer || auth !== 'Bearer ' + peer.token) {
    console.log('denied unauthorized request for', prefix, 'with', auth)
    await produce([{
      topic: 'incoming-rpc-responses',
      messages: Buffer.from(JSON.stringify({ id, error: {
        status: 401,
        body: 'Unauthorized'
      } })),
      timestamp: Date.now()
    }])
    return
  }

  try {
    switch (method) {
      case 'send_transfer':
        const sendTransferId = body && body[0] && body[0].id
        await Promise.all([
          produce([{
            topic: 'incoming-send-transfer',
            messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
            timestamp: Date.now()
          }]),
          produce([{
            topic: 'incoming-rpc-responses',
            messages: Buffer.from(JSON.stringify({ id, body: true })),
            timestamp: Date.now()
          }])
        ])
        break
      case 'fulfill_condition':
        const fulfillTransferId = body && body[0]
        await Promise.all([
          produce([{
            topic: 'incoming-fulfill-condition',
            messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
            timestamp: Date.now()
          }]),
          produce([{
            topic: 'incoming-rpc-responses',
            messages: Buffer.from(JSON.stringify({ id, body: true })),
            timestamp: Date.now()
          }])
        ])
        break
      case 'send_request':
        await produce([{
          topic: 'incoming-send-request',
          messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
          timestamp: Date.now()
          // TODO is there an intelligent way to partiion requests?
        }])
        break
      case 'get_info':
        const info = {
          currencyCode: 'USD',
          currencyScale: 9,
          prefix: prefix,
          connectors: [ prefix + 'server' ]
        }
        await produce([{
          topic: 'incoming-rpc-responses',
          messages: Buffer.from(JSON.stringify({ id, body: info })),
          timestamp: Date.now()
        }])
        break

      default:
        return
    }
  } catch (err) {
    console.error('error producing message from incoming-rpc-requests', id, err)
  }
})

client.once('ready', () => console.log('listening for incoming-rpc-requests'))
consumer.on('error', error => console.error(error))
client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))
