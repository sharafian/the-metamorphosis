const Koa = require('koa')
const Router = require('koa-router')
const Parser = require('koa-bodyparser')
const Cors = require('koa-cors')
const kafka = require('kafka-node')
const EventEmitter = require('events')
const uuid = require('uuid')
const util = require('./util')

const app = new Koa()
const router = Router()
const parser = Parser()
const cors = Cors({
  origin: '*',
  methods: ['GET','POST']
})

const peers = require('../config/peers')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const produce = util.promisify(producer.send.bind(producer))
const consumer = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'rpcServer'
}, 'incoming-rpc-responses')

client.once('ready', () => console.log('listening for incoming-send-request-responses'))
client.on('error', error => console.error(error))
consumer.on('error', error => console.error(error))
producer.on('error', error => console.error(error))

const responder = new EventEmitter()
consumer.on('message', (message) => {
  const data = JSON.parse(message.value)
  console.log('process incoming-rpc-responses', data.id)

  responder.emit(data.id, data)
})

function produceSendTransfer ({ id, method, body, prefix }) {
  return produce([{
    topic: 'incoming-send-transfer',
    messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
    timestamp: Date.now()
  }])
}

function produceFulfillCondition ({ id, method, body, prefix }) {
  return produce([{
    topic: 'incoming-fulfill-condition',
    messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
    timestamp: Date.now()
  }])
}

function produceSendRequest ({ id, method, body, prefix }) {
  return produce([{
    topic: 'incoming-send-request',
    messages: Buffer.from(JSON.stringify({ id, body, prefix, method })),
    timestamp: Date.now()
  }])
}

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

  const peer = peers[prefix]
  if (!peer || auth !== 'Bearer ' + peer.token) {
    console.log('denied unauthorized request for', prefix, 'with', auth)
    ctx.status = 401
    ctx.body = 'Unauthorized'
    return
  }

  ctx.set('Access-Control-Allow-Origin', '*')

  switch (method) {
    case 'send_transfer':
      await produceSendTransfer({ id, body, prefix, method })
      break
    case 'fulfill_condition':
      await produceFulfillCondition({ id, body, prefix, method })
      ctx.body = true
      break
    case 'send_request':
      await produceSendRequest({ id, body, prefix, method })
      const response = await new Promise((resolve) => responder.once(id, data => resolve(data)))
      if (response.error) {
        ctx.status = response.error.status
        ctx.body = response.error.body
        return
      }
      ctx.body = response.body
      break
    case 'get_info':
      ctx.body = {
        currencyCode: 'USD',
        currencyScale: 9,
        prefix: prefix,
        connectors: [ prefix + 'server' ]
      }
      break
    default:
      ctx.body = 'Unknown method ' + method
      ctx.status = 422
      return
  }
})

const port = process.env.PORT || 8080
app
  .use(cors)
  .use(parser)
  .use(router.routes())
  .use(router.allowedMethods())
  .listen(port)
console.log('rpc server listening on: ' + port)
