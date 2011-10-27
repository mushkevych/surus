#!/bin/bash

# make sure the stargate (HBase REST) interface is up
# sudo -u hbase /usr/lib/hbase/bin/hbase-daemon.sh start rest

TARGET_HOST="localhost:8080"

#To create HBase tables use commands:
curl -X PUT -H "Content-Type: text/xml" -H "Accept: text/xml" -d @hbase_table_example.xml http://$TARGET_HOST/example/schema/
