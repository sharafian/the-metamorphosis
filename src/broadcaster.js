const kafka = require('kafka-node')
const uuid = require('uuid')
const util = require('./util')
const client = new kafka.Client('localhost:2181')
const producer = new kafka.HighLevelProducer(client)
const produce = util.promisify(producer.send.bind(producer))
const ROUTE_BROADCAST_INTERVAL = 30000

const routes = require('../config/routing-table.json')
const peers = require('../config/peers.json')

setInterval(async () => {
  const peerPrefixes = Object.keys(peers)
  const requests = []

  for (const peer of peerPrefixes) {
    if (!peers[peer].broadcast) continue
    console.log('broadcast', routes.length, 'routes to', peer)

    const newRoutes = routes.map((route) => ({
      source_ledger: peer,
      destination_ledger: route.target,
      min_message_window: 1,
      paths: [ [] ],
      source_account: peer +  'client'
    }))

    const broadcast = [{
      ledger: peer,
      from: peer + 'client',
      to: peer + 'server',
      custom: {
        method: 'broadcast_routes',
        data: {
          hold_down_time: 999999999,
          unreachable_through_me: [],
          new_routes: newRoutes
        }
      }
    }]

    const request = {
      id: uuid(),
      method: 'send_request',
      prefix: peer,
      body: broadcast
    }

    requests.push({
      topic: 'outgoing-rpc-requests',
      messages: Buffer.from(JSON.stringify(request)),
      timestamp: Date.now()
    })
  }

  try {
    await produce(requests)
  } catch (e) {
    console.error('error creating broadcast rpc messages:', e)
  }
}, ROUTE_BROADCAST_INTERVAL)
