#!/bin/bash
source $NVM_DIR/nvm.sh
echo SETTING UP CONNECTORLAND PEERING $API_HOSTNAME
node scripts/connectorland.js $API_HOSTNAME
wget "https://connector.land/test?peer=`cat config/connectorland-ilp-secret.txt`"
/etc/init.d/zookeeper start
sleep 10
(/bin/kafka-server-start.sh /config/server.properties &)
sleep 10
node index.js
