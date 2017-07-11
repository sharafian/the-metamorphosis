const routingTable = require('../../config/routing-table.json')
const peers = require('../../config/peers.json')
const BigNumber = require('bignumber.js')
const request = require('superagent')

const FIXERIO_URL = 'https://api.fixer.io/latest'

let rates = {}
// TODO put this in a real data store

for (let peer of Object.keys(peers)) {
  if (!peers[peer].currency) {
    throw new Error('Currency must be specified for peer: ' + peer)
  }
}
loadRates()

async function loadRates () {
  if (!rates.base) {
    const rateResponse = (await request.get(FIXERIO_URL)).body
    rates = rateResponse.rates
    rates[rateResponse.base] = 1
  }
}

async function getNextHop (destination) {
  for (const route of routingTable) {
    if (destination.startsWith(route.target)) {
      return {
        connectorLedger: route.ledger,
        connectorAccount: route.connector,
        isLocal: !!route.local
      }
    }
  }
  throw new Error('No route found to ledger: ' + destination)
}

async function getNextAmount ({ sourceLedger, destinationLedger, sourceAmount }) {
  const rate = await getRate({ sourceLedger, destinationLedger })
  const nextAmount = rate.times(sourceAmount)
    .round() // TODO figure out right rounding direction
    .toString()
  return nextAmount
}

async function getPreviousAmount ({ sourceLedger, destinationLedger, destinationAmount }) {
  const rate = await getRate({ sourceLedger, destinationLedger })
  const previousAmount = new BigNumber(destinationAmount)
    .div(rate)
    .round() // TODO figure out right rounding direction
    .toString()
  return previousAmount
}

async function getRate ({ sourceLedger, destinationLedger }) {
  console.log('get rate', sourceLedger, destinationLedger)
  const sourceCurrency = peers[sourceLedger].currency.toUpperCase()
  const destinationCurrency = peers[destinationLedger].currency.toUpperCase()

  await loadRates()

  const destinationRate = rates[destinationCurrency]
  const sourceRate = rates[sourceCurrency]

  if (!sourceRate) {
    throw new Error(`No rate found for currency ${sourceCurrency} (ledger: ${sourceLedger})`)
  }
  if (!destinationRate) {
    throw new Error(`No rate found for currency ${destinationCurrency} (ledger: ${destinationLedger})`)
  }

  const rate = new BigNumber(destinationRate)
    .div(sourceRate)
  return rate
}

module.exports = {
  getNextHop,
  getNextAmount,
  getPreviousAmount
}
