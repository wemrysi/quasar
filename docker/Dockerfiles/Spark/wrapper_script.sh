#!/bin/bash

# start spark master
/opt/spark/sbin/start-master.sh -i $(hostname -i)

sleep 5

# start spark worker
/opt/spark/sbin/start-slave.sh spark://$(hostname -i):7077

sleep 5

tail -f /opt/spark/logs/*


