#!/bin/bash

if [  $# -gt 0 ] ; then
    ZK_VERSION="$1"
else
    ZK_VERSION="3.5.5"
fi

wget "http://apache.cs.utah.edu/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz"
tar -xvf "apache-zookeeper-${ZK_VERSION}-bin.tar.gz"
mv apache-zookeeper-$ZK_VERSION-bin zk
mv ./zk/conf/zoo_sample.cfg ./zk/conf/zoo.cfg
