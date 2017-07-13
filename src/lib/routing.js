const routingTable = require('../../config/routing-table.json')
const peers = require('../../config/peers.json')
const BigNumber = require('bignumber.js')
const request = require('superagent')

const FIXERIO_URL = 'https://api.fixer.io/latest'
const COINMARKETCAP_URL = 'https://api.coinmarketcap.com/v1/ticker/'

let rates = {}
// TODO put this in a real data store

for (let peer of Object.keys(peers)) {
  if (!peers[peer].currencyCode) {
    throw new Error('currencyCode must be specified for peer: ' + peer)
  }
  if (!peers[peer].currencyScale) {
    throw new Error('currencyScale must be specified for peer: ' + peer)
  }
}
loadRates()

async function loadRates () {
  if (!rates.base) {
    const fixerioResponse = (await request.get(FIXERIO_URL)).body
    rates = fixerioResponse.rates
    rates[fixerioResponse.base] = 1

    const coinmarketResponse = (await request.get(COINMARKETCAP_URL)).body
    for (let coinmarketRate of coinmarketResponse) {
      if (!coinmarketRate.price_usd) {
        continue
      }
      const eurRate = new BigNumber(coinmarketRate.price_usd)
        .times(rates.USD)
        .round(8)
        .toString()

      rates[coinmarketRate.symbol] = eurRate
    }
    console.log('loaded rates', rates)
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

async function getNextAmount ({ previousLedger, nextLedger, previousAmount }) {
  const rate = await getRate({ previousLedger, nextLedger })
  const nextAmount = rate.times(previousAmount)
    .round() // TODO figure out right rounding direction
    .toString()
  return nextAmount
}

async function getPreviousAmount ({ previousLedger, nextLedger, nextAmount }) {
  const rate = await getRate({ previousLedger, nextLedger })
  const previousAmount = new BigNumber(nextAmount)
    .div(rate)
    .round() // TODO figure out right rounding direction
    .toString()
  return previousAmount
}

async function getRate ({ previousLedger, nextLedger }) {
  console.log('get rate', previousLedger, nextLedger)
  const previousCurrency = peers[previousLedger].currencyCode.toUpperCase()
  const nextCurrency = peers[nextLedger].currencyCode.toUpperCase()

  await loadRates()

  const nextRate = rates[nextCurrency]
  const previousRate = rates[previousCurrency]

  if (!previousRate) {
    throw new Error(`No rate found for currency ${previousCurrency} (ledger: ${previousLedger})`)
  }
  if (!nextRate) {
    throw new Error(`No rate found for currency ${nextCurrency} (ledger: ${nextLedger})`)
  }

  const scaleToShiftRateBy = new BigNumber(peers[nextLedger].currencyScale)
    .minus(peers[previousLedger].currencyScale)

  const rate = new BigNumber(nextRate)
    .div(previousRate)
    .shift(scaleToShiftRateBy)

  return rate
}

module.exports = {
  getNextHop,
  getNextAmount,
  getPreviousAmount,
  getRate
}
