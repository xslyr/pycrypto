#!/bin/bash

docker compose stop

env -i PATH="$PATH" HOME="$HOME" \
  act --pull=false --rm \
  --env-file /dev/null 

docker ps -a -q --filter "label=com.github.act" | xargs -r docker rm -f