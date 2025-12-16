#!/bin/bash

tar -xf 

while true; do
    cid=$(docker run --detach bazel:assemble-linux-x86_64)
    sleep 10 # give time to start up
    typedb-all-linux-x86_64/typedb console --username=admin --password=password --address=localhost:1729 --tls-disabled --script=tests/assembly/script.tql
    sleep 30
    docker kill $cid

    cid=$(docker run --detach bazel:assemble-linux-x86_64)
    sleep 40 # give time to start up and start checkpointing
    docker kill $cid
done
