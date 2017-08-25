#!/bin/bash

service ssh restart

slee 5

# start hadoop namenode
/opt/hadoop/sbin/hadoop-daemon.sh --config /opt/hadoop/etc/hadoop/ --script hdfs start namenode

sleep 5

# start spark worker
/opt/hadoop/sbin/hadoop-daemon.sh --config /opt/hadoop/etc/hadoop/ --script hdfs start datanode

sleep 5

tail -f /opt/hadoop/logs/*
