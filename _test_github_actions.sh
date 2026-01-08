#!/bin/bash

time act --pull=false --rm
docker ps -a -q --filter "label=com.github.act" | xargs -r docker rm -f