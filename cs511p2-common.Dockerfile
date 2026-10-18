####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM eclipse-temurin:8-jdk-jammy

RUN apt update && \
    apt upgrade --yes && \
    apt install ssh openssh-server python3 --yes

COPY resources/configure-heartbeats.py /configure-heartbeats.py

# Setup common SSH key.
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/shared_rsa -C common && \
    cat ~/.ssh/shared_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Setup HDFS/Spark resources here
# (the PySpark skeletons of Parts 1-4 also need python3 on every node; Spark 3.4.1
#  supports Python 3.7-3.11, so do not install a newer interpreter)

RUN apt-get update && \
    apt-get install --yes --no-install-recommends \
        ca-certificates curl tar gzip procps util-linux sqlite3 && \
    rm -rf /var/lib/apt/lists/*

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.4.1

RUN set -eux; \
    curl --fail --location --retry 3 \
        "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        --output /tmp/hadoop.tar.gz; \
    curl --fail --location --retry 3 \
        "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512" \
        --output /tmp/hadoop.sha512; \
    expected="$(grep -Eo '[[:xdigit:]]{128}' /tmp/hadoop.sha512 | head -n 1)"; \
    test -n "$expected"; \
    printf '%s  /tmp/hadoop.tar.gz\n' "$expected" | sha512sum --check -; \
    mkdir -p /opt/hadoop; \
    tar -xzf /tmp/hadoop.tar.gz --strip-components=1 -C /opt/hadoop; \
    rm /tmp/hadoop.tar.gz /tmp/hadoop.sha512

RUN set -eux; \
    curl --fail --location --retry 3 \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        --output /tmp/spark.tgz; \
    curl --fail --location --retry 3 \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512" \
        --output /tmp/spark.sha512; \
    expected="$(grep -Eo '[[:xdigit:]]{128}' /tmp/spark.sha512 | head -n 1)"; \
    test -n "$expected"; \
    printf '%s  /tmp/spark.tgz\n' "$expected" | sha512sum --check -; \
    mkdir -p /opt/spark; \
    tar -xzf /tmp/spark.tgz --strip-components=1 -C /opt/spark; \
    rm /tmp/spark.tgz /tmp/spark.sha512

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/var/log/hadoop \
    HADOOP_PID_DIR=/var/run/hadoop \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    SPARK_HOME=/opt/spark \
    SPARK_CONF_DIR=/opt/spark/conf \
    SPARK_LOG_DIR=/var/log/spark \
    SPARK_PID_DIR=/var/run/spark \
    SPARK_LOCAL_DIRS=/data/spark-tmp \
    PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
    LANG=C.UTF-8
ENV PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN printf '\nexport JAVA_HOME=/opt/java/openjdk\nexport HADOOP_HEAPSIZE_MAX=512\n' \
        >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    mkdir -p /var/log/hadoop /var/run/hadoop /var/log/spark /var/run/spark \
        /data/spark-tmp /data/spark-work
