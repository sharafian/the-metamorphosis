const Koa = require('koa')
const Router = require('koa-router')
const Parser = require('koa-bodyparser')
const kafka = require('kafka-node')
const EventEmitter = require('events')
const uuid = require('uuid')
const util = require('./util')

const app = new Koa()
const router = Router()
const parser = Parser()

const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const consumer = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'rpcServer'
}, 'incoming-send-request-responses')
client.once('ready', () => console.log('listening for incoming-send-request-responses'))
client.on('error', error => console.error(error))
consumer.on('error', error => console.error(error))
producer.on('error', error => console.error(error))

const responder = new EventEmitter()
consumer.on('message', (message) => {
  const data = JSON.parse(message.value)
  console.log('process incoming-send-request-responses', data.id)

  responder.emit(data.id, data)
})

router.post('/rpc', async (ctx) => {
  const { method, prefix } = ctx.query
  const body = ctx.request.body
  const auth = ctx.headers.authorization
  const id = uuid()

  if (!method || !prefix) {
    ctx.body = 'both method and prefix must be defined'
    ctx.status = 400
    return
  }

  await util.promisify(producer.send.bind(producer))([{
    topic: 'incoming-rpc-requests',
    messages: Buffer.from(JSON.stringify({ id, body, method, prefix, auth })),
    timestamp: Date.now()
  }])

  if (method === 'send_request') {
    const response = await new Promise((resolve) => {
      responder.on(id, data => resolve(data))
    })

    ctx.body = response.body
    return
  }

  ctx.body = true
})

app
  .use(parser)
  .use(router.routes())
  .use(router.allowedMethods())
  .listen(process.env.PORT || 8080)
