#!/bin/bash
# set -x

# @author Bohdan Mushkevych
# description: script to start Synergy Python-to-HBase tunnel and bring it to background

if [ -d /mnt/log/synergy-data ]; then
    PREFIX=/mnt/log/synergy-data
else
    PREFIX=/var/log/synergy-data
fi

PIDFILE=${PREFIX}/tunnel.pid
GC_LOG=${PREFIX}/tunnel-gc.log
JVM_OUT=${PREFIX}/tunnel.out
JVM_ERR=${PREFIX}/tunnel.err

[ -x /usr/lib/hbase/bin/hbase ] && export HADOOP_CLASSPATH=`/usr/lib/hbase/bin/hbase classpath`
SYNERGY_CLASSPATH="$HADOOP_CLASSPATH:lib/gson-1.7.1.jar:synergy-hadoop-01.jar"
SYNERGY_JAVA_OPT="-Xloggc:$GC_LOG -XX:+PrintGCDetails -Xms256M -Xmx5192M -Dlog4j.configuration=log4j.tunnel.properties"

case "$1" in
  start)
    java $SYNERGY_JAVA_OPT -cp $SYNERGY_CLASSPATH com.reinvent.synergy.data.tunnel.TunnelServer 1>${JVM_OUT} 2>${JVM_ERR} &
    echo $! > $PIDFILE
    ;;
  stop)
    kill `jps | grep TunnelServer | awk '{print $1}'`
    ;;
  *)
    echo $"Usage: $0 {start|stop}"
    exit 1
esac

