#!/bin/bash

if [  $# -gt 0 ] ; then
    REDIS_VERSION="$1"
else
    REDIS_VERSION="3.2.6"
fi

# install redis

curl -L http://download.redis.io/releases/redis-$REDIS_VERSION.tar.gz -o redis.tar.gz
tar xzf redis.tar.gz && mv redis-$REDIS_VERSION redis && cd redis && make
