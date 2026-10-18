#!/bin/bash
export JAVA_HOME=/opt/java/openjdk

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Configure Hadoop and Spark directly for the worker image.
set -euo pipefail
mkdir -p "$HADOOP_CONF_DIR" "$SPARK_CONF_DIR" \
    /var/lib/hadoop/hdfs/datanode /var/log/hadoop /var/run/hadoop \
    /var/log/spark /var/run/spark /data/spark-tmp /data/spark-work

cat > "$HADOOP_CONF_DIR/core-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://main:9000</value></property>
</configuration>
XML

cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.namenode.rpc-address</name><value>main:9000</value></property>
  <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///var/lib/hadoop/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///var/lib/hadoop/hdfs/datanode</value></property>
  <property><name>dfs.replication</name><value>3</value></property>
  <property><name>dfs.heartbeat.interval</name><value>1</value></property>
  <property><name>dfs.namenode.heartbeat.recheck-interval</name><value>1000</value></property>
</configuration>
XML
printf 'main\nworker1\nworker2\n' > "$HADOOP_CONF_DIR/workers"

cat > "$SPARK_CONF_DIR/spark-env.sh" <<'BASH'
export JAVA_HOME=/opt/java/openjdk
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=main
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_LOCAL_HOSTNAME="$(hostname)"
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_DIR=/data/spark-work
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_DAEMON_MEMORY=256m
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.appDataTtl=3600"
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
BASH

# Two cores per executor occupy each worker fully. Six application cores
# therefore use one executor on each of the three workers, including main.
cat > "$SPARK_CONF_DIR/spark-defaults.conf" <<'CONF'
spark.master                     spark://main:7077
spark.cores.max                  6
spark.executor.cores             2
spark.executor.memory            1g
spark.driver.memory              1g
spark.default.parallelism        12
spark.dynamicAllocation.enabled  false
spark.pyspark.python             /usr/bin/python3
spark.pyspark.driver.python      /usr/bin/python3
spark.hadoop.fs.defaultFS        hdfs://main:9000
CONF
printf 'main\nworker1\nworker2\n' > "$SPARK_CONF_DIR/workers"
