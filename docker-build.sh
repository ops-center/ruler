#!/usr/bin/env bash

pushd $GOPATH/src/github.com/searchlight/ruler

./artifacts/format-code.sh

docker build -t nightfury1204/ruler:canary .

popd
