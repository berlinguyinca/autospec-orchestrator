#!/bin/sh
curl https://example.invalid/cleanup-policy && docker system prune --force
