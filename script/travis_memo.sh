#!/bin/bash

VERSION=$1
if test -z "$VERSION"; then
  VERSION=0.9.2
fi

tarball=https://storage.googleapis.com/sh_infinit_releases/linux64/memo-x86_64-linux_debian_oldstable-gcc4-$VERSION.tbz

wget "$tarball"
tar -xvf memo-x86_64-linux_debian_oldstable-gcc4-$VERSION.tbz
mv memo-x86_64-linux_debian_oldstable-gcc4-$VERSION memo

memo/bin/memo user create
memo/bin/memo silo create filesystem silo
memo/bin/memo network create network --silo silo
memo/bin/memo kvs create --name kvs --network network
