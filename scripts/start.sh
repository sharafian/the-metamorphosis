#!/bin/bash
source $NVM_DIR/nvm.sh
node scripts/connectorland.js $ILP_DOMAIN
curl "https://connector.land/stats?test=`cat config/connectorland-ilp-secret.txt`"
/etc/init.d/zookeeper start
sleep 10
(/bin/kafka-server-start.sh /config/server.properties &)
sleep 10
node index.js
