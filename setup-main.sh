#!/bin/bash
export JAVA_HOME=/opt/java/openjdk

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Setup HDFS/Spark main here
set -euo pipefail
: "${HADOOP_HOME:=/opt/hadoop}"
: "${HADOOP_CONF_DIR:=$HADOOP_HOME/etc/hadoop}"
mkdir -p "$HADOOP_CONF_DIR" /var/lib/hadoop/hdfs/datanode \
    /var/log/hadoop /var/run/hadoop

cat > "$HADOOP_CONF_DIR/core-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://main:9000</value>
  </property>
</configuration>
XML

cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>main:9000</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-bind-host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///var/lib/hadoop/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///var/lib/hadoop/hdfs/datanode</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>
  <property>
    <name>dfs.heartbeat.interval</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.heartbeat.recheck-interval</name>
    <value>1000</value>
  </property>
</configuration>
XML

# Hadoop's SSH helpers read this list; main is also a DataNode.
printf 'main\nworker1\nworker2\n' > "$HADOOP_CONF_DIR/workers"

mkdir -p /var/lib/hadoop/hdfs/namenode