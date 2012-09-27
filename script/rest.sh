#!/bin/bash
# set -x

# @author Bohdan Mushkevych
# description: script to start Synergy REST interface

if [ -d /mnt/log/synergy-data ]; then
    PREFIX=/mnt/log/rece
else
    PREFIX=/var/log/rece
fi

PIDFILE=${PREFIX}/rest.pid
GC_LOG=${PREFIX}/rest-gc.log
JVM_OUT=${PREFIX}/rest.out
JVM_ERR=${PREFIX}/rest.err

JETTY_VERSION="8.0.4.v20111024"
ZOOKEEPER_LIB="/usr/lib/zookeeper/lib"
HADOOP_LIB="/usr/lib/hadoop/lib"

#[ -x /usr/lib/hbase/bin/hbase ] && export HADOOP_CLASSPATH=`/usr/lib/hbase/bin/hbase classpath`
SYNERGY_JAVA_OPT="-Xloggc:$GC_LOG -XX:+PrintGCDetails -Xms256M -Xmx2048M -Dlog4j.configuration=log4j.rest.properties"

# classpath Hadoop section 
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:/usr/lib/hadoop/hadoop-common.jar:/usr/lib/hbase/hbase.jar:/usr/lib/zookeeper/zookeeper.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:/usr/lib/hadoop/hadoop-auth.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:$ZOOKEEPER_LIB/log4j-1.2.15.jar:$ZOOKEEPER_LIB/slf4j-api-1.6.1.jar:$ZOOKEEPER_LIB/slf4j-log4j12-1.6.1.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:$HADOOP_LIB/commons-logging-api-1.1.jar:$HADOOP_LIB/commons-logging-1.1.1.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:$HADOOP_LIB/commons-beanutils-1.7.0.jar:$HADOOP_LIB/commons-collections-3.2.1.jar:$HADOOP_LIB/commons-httpclient-3.1.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:$HADOOP_LIB/commons-beanutils-core-1.8.0.jar:$HADOOP_LIB/commons-configuration-1.6.jar:$HADOOP_LIB/commons-io-2.1.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:$HADOOP_LIB/commons-cli-1.2.jar:$HADOOP_LIB/commons-digester-1.8.jar:$HADOOP_LIB/commons-lang-2.5.jar:$HADOOP_LIB/commons-net-3.1.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:$HADOOP_LIB/commons-codec-1.4.jar:$HADOOP_LIB/commons-el-1.0.jar"

# synergy specific classpath
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/gson-1.7.1.jar:rece-hadoop-02.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/guava-11.0.2.jar:lib/guava-r09-jarjar.jar:lib/joda-time-2.1.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/jetty-security-$JETTY_VERSION.jar:lib/jetty-continuation-$JETTY_VERSION.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/jetty-servlet-$JETTY_VERSION.jar:lib/jetty-http-$JETTY_VERSION.jar:lib/jetty-io-$JETTY_VERSION.jar"
SYNERGY_CLASSPATH="$SYNERGY_CLASSPATH:lib/servlet-api-3.0.jar:lib/jetty-util-$JETTY_VERSION.jar:lib/jetty-server-$JETTY_VERSION.jar"

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
