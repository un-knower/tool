#!/bin/bash
bin=`dirname "$0"`
. "$bin/hcat-env.sh"

export APP_HOME=`cd $bin/..; pwd`

main_class=com.hiido.hcat.service.ServiceMain

lib="${APP_HOME}/lib"
libserver="${APP_HOME}/libserver"
conf="${APP_HOME}/conf"
HADOOP_CONF="${HADOOP_HOME}/etc/hadoop"
HADOOP_SP="${HADOOP_HOME}/sp"
auxlib="${APP_HOME}/udf"
ssl="${APP_HOME}/ssl"
spring_conf="${APP_HOME}/spring_conf"
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"


class_path=$spring_conf:$ssl:$conf:$HADOOP_CONF

for f in ${libserver}/*.jar; do
    class_path=${class_path}:$f
done

for f in ${HADOOP_SP}/*.jar; do
    class_path=${class_path}:$f
done

for f in ${lib}/*.jar; do
    class_path=${class_path}:$f
done

for f in ${auxlib}/*.jar; do
    class_path=${class_path}:$f
done

java_opt="-Xmx5120M -XX:PermSize=128M -XX:MaxPermSize=256M -XX:-UseGCOverheadLimit -XX:-HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/web/logs/dump/ -Duser.timezone=GMT+08 -Dfile.encoding=UTF-8 -Djava.library.path=${JAVA_LIBRARY_PATH}"

${JAVA_HOME}/bin/java ${java_opt} -cp ${class_path} ${main_class} eth0 "$@"