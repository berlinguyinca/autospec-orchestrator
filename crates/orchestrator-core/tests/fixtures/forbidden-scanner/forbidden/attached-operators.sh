#!/bin/sh
docker network prune&&printf 'network cleaned\n'
docker volume prune|tee /tmp/prune.log
