const WebSocket = require('ws')
const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')

const port = 8081
const server = new WebSocket.Server({ port })

server.on('connection', (ws) => {
  // TODO check auth, filter by user
  console.log('got websocket connection')
  const client = new kafka.Client('localhost:2181')
  // TODO listen for fulfillments
  client.connect()
  const consumer = new kafka.Consumer(client, [{
    topic: 'incoming-send-transfer',
    partition: 0,
    offset: 0
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
    ws.send(JSON.stringify(transfer))
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
