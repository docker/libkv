test: build
	docker run -it -v ${PWD}:/go/src/github.com/docker/libkv libkv

build:
	docker build -t libkv .
