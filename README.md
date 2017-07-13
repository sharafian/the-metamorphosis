# The Metamorphosis

## Current Architecture

![Stream diagram](./assets/streams.png)

## Roadmap

### Back end

- [x] Combine RPC Server and Authorizer
- [x] Add rejector (passes back error messages)
- [x] Add fixer.io + coinmarketcap for exchange rates (and tag ledgers with currency codes)
- [x] Implement route broadcaster for CCP
- [ ] Dynamic routing table (Kafka? Zookeeper? Redis?)
- [ ] Implement route receiver for CCP
- [ ] Dynamic user and peer registration (probably need KV store)
- [ ] Add support for multiple ledger plugins
- [ ] Add balance checker (internal ledger)
- [ ] Add transfer expirer (produces to rejector's topic)
- [ ] Check transfer state through RPC (get fulfillment)
- [ ] Admin SPSP fulfiller (listens on send transfer topic)

### Front end

- [ ] Make butterfly prettier
- [ ] CLI for adding users
- [ ] CLI for configuring routes (?)
- [ ] UI for user registration
- [ ] UI for user API key management
- [ ] UI for viewing trustline balances
- [ ] Metrics
- [ ] Transaction history
- [ ] Metrics for Cicada

### Open Questions

* Should there be a standard API for non-payment related functions (e.g. realtime activity feeds, transaction history, API token management, etc)?
* Should the routing table contain rate information as well or should that be separate?
* What kind of (Key Value?) store do we need aside from Kafka? (for routing tables, users and peers, etc)
* What serialization format should the components use between one another? (Right now it uses JSON)
* Is Kafka the best stream technology to use?
* How micro should the microservices be?
* Should configuration be with files or UI?

### Deploy

If you're running ilp-kit through docker compose, an easy way to switch to the-metamorphosis is to ssh into your server, and run:
```sh
git clone https://github.com/sharafian/the-metamorphosis
cd the-metamorphosis
docker build -t interledgerjs/ilp-kit . # this will replace the interledgerjs/ilp-kit image on your server
cd ..
cd ilp-kit-docker-compose
export ILP_EMAIL=you@ilp.example.com
export ILP_DOMAIN=ilp.example.com 
docker-compose up -d
docker ps
docker stop postgres
docker exec ilp-kit cat config/connectorland-ilp-secret.txt; echo
curl https://connector.land/test?peer=`docker exec ilp-kit cat config/connectorland-ilp-secret.txt`
```

You can of course also just run the Dockerfile and then set up a TLS proxy in front of the port 3010 it exposes,
or you could have a look inside [this repos's Dockerfile](https://github.com/sharafian/the-metamorphosis/blob/master/Dockerfile) to learn how to set up Zookeeper + Kafka + Node.js + TheMetamorphosis
