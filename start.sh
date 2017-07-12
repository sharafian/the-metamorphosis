#!/bin/bash
source $NVM_DIR/nvm.sh
/etc/init.d/zookeeper start
sleep 10
(/bin/kafka-server-start.sh /config/server.properties &)
sleep 10
node index.js
