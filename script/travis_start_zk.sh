#! /bin/bash

while true; do
  ./zk/bin/zkServer.sh start ./zk/conf/zoo.cfg
  sleep 3
  if echo stat |nc localhost 2181 |grep -q Mode; then
    break
  fi
  echo zk did not start properly, retrying...
  ./zk/bin/zkServer.sh stop
done
