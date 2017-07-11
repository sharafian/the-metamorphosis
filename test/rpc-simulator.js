const kafka = require('kafka-node')
const EventEmitter = require('events')
const uuid = require('uuid')
const util = require('../src/util')

const peers = require('../config/peers')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const produce = util.promisify(producer.send.bind(producer))

client.on('error', error => console.error(error))
producer.on('error', error => console.error(error))

function produceSendTransfer ({ id, body, prefix }) {
  return produce([{
    topic: 'incoming-send-transfer',
    messages: Buffer.from(JSON.stringify({ id, body, prefix, method: 'send_transfer' })),
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

function produceRejectIncomingTransfer ({ id, method, body, prefix }) {
  return produce([{
    topic: 'incoming-reject-incoming-transfer',
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

client.once('ready', async () => {
  console.log('starting')
  while (1) {
    await produceSendTransfer({
      id: '9f7c7375-881e-a577-2d44-39efc941a770',
      prefix: 'test.east.',
      body: [ {
        id: "8ef123cb-84cb-0724-153e-f23c1af5e997",
        executionCondition: "U/M41WQx4Xn/kyLRFm/7WaMrHInUBORdawXDMTGw8QA=",
        to: "test.east.client",
        amount: "10",
        expiresAt: "2017-07-31T00:00:00Z",
        ilp: "ARYAAAAAAAAAAQt0ZXN0LnNvdXRoLgAA"
      } ]
    }).catch((e) => console.log(e))
    await produceFulfillCondition({
      id: '9f7c7375-881e-a577-2d44-39efc941a770',
      prefix: 'test.east.',
      method: 'fulfill_condition',
      body: [
        "8ef123cb-84cb-0724-153e-f23c1af5e997",
        "C+uUsltMkbKT0XPCgekOKeLajvMlvrI0vaSmqhWQzTY="
      ]
    }).catch((e) => console.log(e))
  }
})
