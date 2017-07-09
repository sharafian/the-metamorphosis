const kafka = require('kafka-node')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const BigNumber = require('bignumber.js')
const producer = new kafka.HighLevelProducer(client)
const produce = util.promisify(producer.send.bind(producer))
const transfers = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'authorizer'
}, 'incoming-rpc-requests')
const fulfillments = new kafka.ConsumerGroup({
  host: 'localhost:2181',
  groupId: 'authorizer'
}, 'incoming-rpc-requests')

// TODO: persistence
// TODO: discard from id cache
const idCache = {}
const balances = {}

transfers.on('message', async (message) => {
  const { id, body, prefix } = JSON.parse(message.value)
  const transfer = body[0]
  const transferId = transfer.id

  if (idCache[transferId]) return
  idCache[transferId] = true

  const balance = balances[prefix]
  if (!balance) balances[prefix] = new BigNumber(0)
  balances[prefix] = balance.add(transfer.amount)
})

transfers.on('error'
