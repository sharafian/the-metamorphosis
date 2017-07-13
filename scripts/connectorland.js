const fs = require('fs')
const crypto = require('crypto')

const CONNECTORLAND_LEDGER_PREFIX = 'g.dns.land.connector.'
const CONNECTORLAND_RPC_URI = 'https://connector.land/rpc'
const PEERS_FILENAME = 'config/peers.json'
const CONNECTORLAND_ILP_SECRET_FILENAME = 'config/connectorland-ilp-secret.txt'

function base64url(buff) {
  return buff.toString('base64').replace(/\//g, '_').replace(/\+/g, '-').replace(/=/g, '')
}

function readPeers() {
  try {
    return JSON.parse(fs.readFileSync(PEERS_FILENAME))
  } catch(e) {
    console.error('FATAL: could not read ' + PEERS_FILENAME)
    throw e
  }
}

function writePeers(peers) {
  try {
    fs.writeFileSync(PEERS_FILENAME, JSON.stringify(peers, null, 2))
  } catch(e) {
    console.error('FATAL: could not write ' + PEERS_FILENAME)
    throw e
  }
}

function writeIlpSecret(ilpSecret) {
  try {
    fs.writeFileSync(CONNECTORLAND_ILP_SECRET_FILENAME, ilpSecret)
  } catch(e) {
    console.error('FATAL: could not write ' + CONNECTORLAND_ILP_SECRET_FILENAME)
    throw e
  }
}

function run(ilpDomain) {
  console.log('hostname for this the-metamorphosis server:', ilpDomain)
  if (typeof ilpDomain !== 'string' || ilpDomain.length === 0) {
    throw new Error('Please run as node ./scripts/connectorland.js example.com')
  }

  let peers = readPeers()

  if (peers[CONNECTORLAND_LEDGER_PREFIX]) {
    console.log('Already have connector.land as a peer (see ' + PEERS_FILENAME + ')')
  } else {
    const token = base64url(crypto.randomBytes(32))
    const ilpSecret = 'ilp_secret:'+base64url(Buffer.from('https://' + CONNECTORLAND_LEDGER_PREFIX + ':' + token + '@' + ilpDomain + '/rpc', 'ascii'))

    peers[CONNECTORLAND_LEDGER_PREFIX] = { uri: CONNECTORLAND_RPC_URI, token }
    writePeers(peers)
    writeIlpSecret(ilpSecret)
    console.log('Wrote ' + CONNECTORLAND_ILP_SECRET_FILENAME, ilpSecret)
  }
}

// ...
run(process.argv[2])
