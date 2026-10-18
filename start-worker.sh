#!/bin/bash

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

/etc/init.d/ssh start
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/shared_rsa

# Apply the supplied timing before the student service-start commands below.
# No Hadoop installation is expected in the unfinished Part 0 starter.
if command -v hdfs >/dev/null 2>&1; then
    python3 /configure-heartbeats.py || exit 1
fi

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Start HDFS/Spark worker here
set -euo pipefail
mkdir -p /var/lib/hadoop/hdfs/datanode /var/log/hadoop /var/run/hadoop \
    /var/log/spark /var/run/spark
hdfs --daemon start datanode

# Wait for main's RPC socket before registering the Spark worker. This also
# avoids exhausting Spark's registration retries during a slow first boot.
deadline=$((SECONDS + 600))
until timeout 2 bash -c 'exec 3<>/dev/tcp/main/7077' 2>/dev/null; do
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "Spark master main:7077 did not start." >&2
        exit 1
    fi
    sleep 2
done
export SPARK_NO_DAEMONIZE=1
exec "$SPARK_HOME/sbin/start-worker.sh" spark://main:7077
