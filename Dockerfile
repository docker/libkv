FROM golang:latest

RUN apt-get update && apt-get install zookeeperd unzip -y

RUN mkdir -p /opt

ENV ETCD_RELEASE v2.1.0-rc.0
RUN curl -L https://github.com/coreos/etcd/releases/download/${ETCD_RELEASE}/etcd-${ETCD_RELEASE}-linux-amd64.tar.gz  | tar -vxz -C /opt

ENV CONSUL_RELEASE 0.5.2
RUN curl -L https://dl.bintray.com/mitchellh/consul/${CONSUL_RELEASE}_linux_amd64.zip -o /tmp/consul.zip
RUN unzip -a /tmp/consul.zip -d /opt/consul
RUN echo "{\"session_ttl_min\": \"1s\"}" >> /tmp/consul_config.json

ENV PATH ${PATH}:/opt/etcd-${ETCD_RELEASE}-linux-amd64:/opt/consul
ENV GOPATH /go
ENV KVDIR /go/src/github.com/docker/libkv
RUN mkdir -p ${KVDIR}
WORKDIR ${KVDIR}

CMD (etcd --data-dir /tmp/etcd &) && service zookeeper start && (consul agent -config-file /tmp/consul_config.json -server -bootstrap -data-dir /tmp/consul &) && go get -d ./... && go test -v ./...
