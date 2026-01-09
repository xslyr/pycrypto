#!/bin/bash

docker compose stop
time act --pull=false --rm
docker ps -a -q --filter "label=com.github.act" | xargs -r docker rm -f