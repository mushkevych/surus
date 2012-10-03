#!/bin/bash

# make sure the stargate (HBase REST) interface is up
# sudo -u hbase /usr/lib/hbase/bin/hbase-daemon.sh start rest -p 8081

TARGET_HOST="localhost:8081"

#To create HBase tables use commands:
curl -X PUT -H "Content-Type: text/xml" -H "Accept: text/xml" -d @hbase_table_example.xml http://$TARGET_HOST/example/schema/
curl -X PUT -H "Content-Type: text/xml" -H "Accept: text/xml" -d @hbase_table_bucket.xml http://$TARGET_HOST/bucket/schema/
