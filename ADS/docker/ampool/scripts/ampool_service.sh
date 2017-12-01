#!/bin/bash
/*
* Copyright (c) 2017 Ampool, Inc. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you
* may not use this file except in compliance with the License. You
* may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License. See accompanying
* LICENSE file.
*/
if [ -n "$AMPOOL_SERVICE_HOST" ]; then
  echo "Service host provided via env..."
else
  AMPOOL_SERVICE_HOST=$HOSTNAME
fi

echo "Using service host - "$AMPOOL_SERVICE_HOST
# search /etc/hosts for the line with the ip address which will look like this:
#     172.18.0.4    8886629d38e6
MY_IP_LINE=`cat /etc/hosts | grep ${HOSTNAME}`
# take the ip address from this:
MY_IP=`(echo $MY_IP_LINE | grep -o '[0-9]\+[.][0-9]\+[.][0-9]\+[.][0-9]\+')`
echo "Container ip addr. - "$MY_IP

if [ "$1" = "locator" ]; then
    # Locator specific code
    # Create working directory for Locator with name "locator"
    mkdir -p /data/locator
    cd /data/locator

    # Start Locator with name "locator"
    java -cp $AMPOOL_HOME/lib/monarch-dependencies.jar \
     -Dgemfire.home=$AMPOOL_HOME \
     -DgemfirePropertyFile=$AMPOOL_HOME/config/ampool_locator.properties \
     -DgemfireSecurityPropertyFile=$AMPOOL_HOME/config/ampool_security.properties \
     -Dlog4j.configurationFile=$AMPOOL_HOME/config/ampool-log4j2.xml \
     -Dgemfire.cache-xml-file=$AMPOOL_HOME/config/cache.xml \
     -XX:OnOutOfMemoryError="kill -KILL %p" \
     -Dgemfire.launcher.registerSignalHandlers=true \
     -Djava.awt.headless=true \
     -Dsun.rmi.dgc.server.gcInterval=9223372036854775806 \
     -Djava.rmi.server.hostname=localhost \
     -Dgemfire.membership-port-range=$AMPOOL_PORTS_LOW-$AMPOOL_PORTS_HIGH \
     com.gemstone.gemfire.distributed.LocatorLauncher start locator --hostname-for-clients=$AMPOOL_SERVICE_HOST
else
    if [ -n "$AMPOOL_SERVER_NAME" ]; then
      echo "Server Name provided via env..."
    else
      AMPOOL_SERVER_NAME=$HOSTNAME
    fi
    echo "Using Server Name - "$AMPOOL_SERVER_NAME
    if [ -n "$SERVER_HEAP_PERCENTAGE" ]; then
        echo "Server heap percentage set by user to "$SERVER_HEAP_PERCENTAGE
    else
        echo "Server heap percentage not set Setting it to default 90.0"
        SERVER_HEAP_PERCENTAGE=90.0
    fi
    if [ -n "$SERVER_EVICTION_PERCENTAGE" ]; then
        echo "Server eviction percentage set by user to "$SERVER_EVICTION_PERCENTAGE
    else
        echo "Server eviction percentage not set Setting it to default 60.0"
        SERVER_EVICTION_PERCENTAGE=60.0
    fi
    # Server specific code
    # Create working directory for Server with server name
    mkdir -p /data/$AMPOOL_SERVER_NAME
    cd /data/$AMPOOL_SERVER_NAME
    sleep 10; # Added manual 10s delay for docker-compose
    # Start Server with name "HOSTNAME"; passed to properties via system variable
    java -cp $AMPOOL_HOME/lib/monarch-dependencies.jar \
     -Dgemfire.home=$AMPOOL_HOME \
     -DgemfirePropertyFile=$AMPOOL_HOME/config/ampool_server.properties \
     -DgemfireSecurityPropertyFile=$AMPOOL_HOME/config/ampool_security.properties \
     -Dlog4j.configurationFile=$AMPOOL_HOME/config/ampool-log4j2.xml \
     -Dgemfire.cache-xml-file=$AMPOOL_HOME/config/cache.xml \
     -Xms$AMPOOL_INITIAL_HEAP -Xmx$AMPOOL_MAX_HEAP -XX:+UseConcMarkSweepGC \
     -XX:CMSInitiatingOccupancyFraction=50 \
     -XX:OnOutOfMemoryError="kill -KILL %p" \
     -Dgemfire.launcher.registerSignalHandlers=true \
     -Djava.awt.headless=true \
     -Dsun.rmi.dgc.server.gcInterval=9223372036854775806 \
     -Dgemfire.locators=$AMPOOL_LOCATOR_HOST[10334] \
     -Dgemfire.bind-address=$MY_IP \
     -Dgemfire.http-service-bind-address=$MY_IP \
     -Dgemfire.start-dev-rest-api=$AMPOOL_START_REST \
     -Dgemfire.http-service-port=9090 \
     -Dgemfire.membership-port-range=$AMPOOL_PORTS_LOW-$AMPOOL_PORTS_HIGH \
     -Dgemfire.HeapLRUCapacityController.evictHighEntryCountBucketsFirst=false \
     -Dgemfire.heapPollerInterval=50 -Dgemfire.HeapLRUCapacityController.evictionBurstPercentage=1.62 \
     -Dgemfire.eviction-thresholdThickness=8.1 -Dgemfire.thresholdThickness=4.5 -Dgemfire.tombstone-timeout=8000 \
     -Dgemfire.tombstone-scan-interval=10000 -Dgemfire.non-replicated-tombstone-timeout=10000 \
     com.gemstone.gemfire.distributed.ServerLauncher start $AMPOOL_SERVER_NAME --force \
     --hostname-for-clients=$AMPOOL_SERVICE_HOST \
     --critical-heap-percentage=$SERVER_HEAP_PERCENTAGE --eviction-heap-percentage=$SERVER_EVICTION_PERCENTAGE
fi
