const routingTable = require('../../config/routing-table.json')
function getNextHop (destination) {
  for (const route of routingTable) {
    if (destination.startsWith(route.target)) {
      return {
        connectorLedger: route.ledger,
        connectorAccount: route.connector,
        isLocal: !!route.local
      }
    }
  }
  throw new Error('no route found to', destination)
}

function getNextAmount ({ sourceLedger, destinationLedger, amount }) {
  return amount
}

module.exports = {
  getNextHop,
  getNextAmount
}
