#!/bin/bash

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

# Exchange SSH keys.
/etc/init.d/ssh start
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/shared_rsa
for node in worker1 worker2; do
    ready=false
    for attempt in {1..60}; do
        if ssh-copy-id -i ~/.ssh/id_rsa.pub -o 'IdentityFile ~/.ssh/shared_rsa' \
                -o StrictHostKeyChecking=no -o ConnectTimeout=2 -f "$node"; then
            ready=true
            break
        fi
        sleep 1
    done
    if [ "$ready" = false ]; then
        echo "Could not exchange SSH keys with $node" >&2
        exit 1
    fi
done

# Apply the supplied timing before the student service-start commands below.
# No Hadoop installation is expected in the unfinished Part 0 starter.
if command -v hdfs >/dev/null 2>&1; then
    python3 /configure-heartbeats.py || exit 1
fi

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Start HDFS/Spark main here

# Keep MP1's existing namespace when this container is restarted.
set -euo pipefail
mkdir -p /var/lib/hadoop/hdfs/namenode /var/lib/hadoop/hdfs/datanode \
    /var/log/hadoop /var/run/hadoop /var/log/spark /var/run/spark
if [ ! -f /var/lib/hadoop/hdfs/namenode/current/VERSION ]; then
    hdfs namenode -format -nonInteractive
fi
hdfs --daemon start namenode
hdfs --daemon start datanode
"$SPARK_HOME/sbin/start-master.sh"

# Readiness requires writable HDFS, not just a listening NameNode socket.
deadline=$((SECONDS + 600))
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'Safe mode is OFF'; do
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "HDFS did not leave safe mode; see /var/log/hadoop." >&2
        exit 1
    fi
    sleep 2
done
hdfs dfs -mkdir -p /spark/checkpoints
hdfs dfs -chmod 1777 /spark/checkpoints

# Each container starts exactly one Spark worker. Its foreground process
# keeps the container alive; executors are started when an application runs.
export SPARK_NO_DAEMONIZE=1
exec "$SPARK_HOME/sbin/start-worker.sh" spark://main:7077

bash
