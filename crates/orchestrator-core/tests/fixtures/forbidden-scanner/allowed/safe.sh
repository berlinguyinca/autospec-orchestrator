#!/bin/sh
# docker system prune is forbidden, but comments are evidence rather than commands.
docker rm autospec-execution-agent
git branch --list autospec-execution
echo cleanup-is-bounded # never use docker system prune
