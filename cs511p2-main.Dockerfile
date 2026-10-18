####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM cs511p2-common

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################
RUN mkdir -p /var/lib/hadoop/hdfs/namenode /var/lib/hadoop/hdfs/datanode /data

COPY ./setup-main.sh ./setup-main.sh
RUN /bin/bash setup-main.sh

COPY ./start-main.sh ./start-main.sh
CMD ["/bin/bash", "start-main.sh"]
