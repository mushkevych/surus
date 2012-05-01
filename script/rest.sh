#!/bin/bash
# set -x

# @author Bohdan Mushkevych
# date: 15 Feb 2012
# description: script to start Surus REST interface

if [ -d /mnt/log/synergy-data ]; then
    PREFIX=/mnt/log/synergy-data
else
    PREFIX=/var/log/synergy-data
fi

PIDFILE=${PREFIX}/rest.pid
GC_LOG=${PREFIX}/rest-gc.log
JVM_OUT=${PREFIX}/rest.out
JVM_ERR=${PREFIX}/rest.err

JETTY_VERSION="8.0.4.v20111024"
HBASE_VERSION="0.90.4-cdh3u3"

#[ -x /usr/lib/hbase/bin/hbase ] && export HADOOP_CLASSPATH=`/usr/lib/hbase/bin/hbase classpath`
SYNERGY_JAVA_OPT="-Xloggc:$GC_LOG -XX:+PrintGCDetails -Xms256M -Xmx4096M -Dlog4j.configuration=log4j.rest.properties"

# classpath Hadoop section 
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:/usr/lib/hadoop/hadoop-core.jar:/usr/lib/hbase/hbase-${HBASE_VERSION}.jar:/usr/lib/zookeeper/zookeeper.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:/usr/lib/hadoop/lib/log4j-1.2.15.jar:/usr/lib/hadoop/lib/commons-logging-1.0.4.jar:/usr/lib/hadoop/lib/commons-logging-api-1.0.4.jar"

# synergy specific classpath
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/gson-1.7.1.jar:synergy-hadoop-02.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/jetty-security-${JETTY_VERSION}.jar:lib/jetty-continuation-${JETTY_VERSION}.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/jetty-servlet-${JETTY_VERSION}.jar:lib/jetty-http-${JETTY_VERSION}.jar:lib/jetty-io-${JETTY_VERSION}.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/servlet-api-3.0.jar:lib/jetty-util-${JETTY_VERSION}.jar:lib/jetty-server-${JETTY_VERSION}.jar"

case "$1" in
  start)
    java $SYNERGY_JAVA_OPT -cp $SYNERGY_CLASSPATH com.reinvent.synergy.data.rest.RestServer 1>${JVM_OUT} 2>${JVM_ERR} &
    echo $! > $PIDFILE
    ;;
  stop)
    kill `jps | grep RestServer | awk '{print $1}'`
    ;;
  *)
    echo $"Usage: $0 {start|stop}"
    exit 1
esac
