#!/bin/bash

sed -i "s/localhost/$(hostname -f)/" /opt/hadoop/etc/hadoop/core-site.xml

sleep 2

service ssh restart

slee 2

# start hadoop namenode
/opt/hadoop/sbin/hadoop-daemon.sh --config /opt/hadoop/etc/hadoop/ --script hdfs start namenode

#/opt/hadoop/sbin/start-all.sh

sleep 5

# start spark worker
/opt/hadoop/sbin/hadoop-daemon.sh --config /opt/hadoop/etc/hadoop/ --script hdfs start datanode

sleep 5

tail -f /opt/hadoop/logs/*
