#!/bin/sh
# docker system prune is forbidden, but comments are evidence rather than commands.
docker rm autospec-execution-agent
git branch --list autospec-execution
echo cleanup-is-bounded # never use docker system prune
git worktree prune
cachectl prune
printf '%s\n' https://example.invalid/prune
curl https://example.invalid/docker system prune
