const WebSocket = require('ws')
const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const offset = kafka.Offset(client)
const offsetFetch = util.promisify(offset.fetch.bind(offset))

const port = 8081
const server = new WebSocket.Server({ port })

server.on('connection', (ws) => {
  // TODO check auth, filter by user
  console.log('got websocket connection')
  const client = new kafka.Client('localhost:2181')
  // TODO listen for fulfillments
  client.connect()
  const offsetResponse = await offsetFetch([{
    topic: 'incoming-send-transfer',
    partition: 0,
    time: -1,
    maxNum: 10
  }])
  const offsetToStartFrom = offsetResponse['incoming-send-transfer']['0'][0]
  const consumer = new kafka.Consumer(client, [{
    topic: 'incoming-send-transfer',
    partition: 0,
    offset: offsetToStartFrom
  }], {
    fromOffset: true
  })
  ws.on('close', () => {
    consumer.close()
  })
  ws.on('end', () => {
    consumer.close()
  })
  consumer.on('message', (message) => {
    console.log('sending transfer to websocket client')
    const transfer = JSON.parse(message.value).body[0]
    try {
      ws.send(JSON.stringify(transfer))
    } catch (err) {
      console.error('error sending to websocket client', err)
    }
  })
  consumer.on('error', (err) => {
    ws.close()
    console.error(err)
  })
  client.on('error', (err) => {
    ws.close()
    console.error(err)
  })
})

console.log('websocket server listening on:', port)
