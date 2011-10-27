#!/bin/bash

# make sure the stargate (HBase REST) interface is up
# sudo -u hbase /usr/lib/hbase/bin/hbase-daemon.sh start rest

TARGET_HOST="localhost:8080"

#To delete HBase tables:
curl -X DELETE -H "Content-Type: text/xml" http://$TARGET_HOST/example/schema
