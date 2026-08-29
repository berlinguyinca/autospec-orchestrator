#!/bin/sh
set -eu

session_id=
source_session=
while test "$#" -gt 0; do
  case "$1" in
    --session-id)
      session_id=$2
      shift 2
      ;;
    --session|--fork)
      source_session=$2
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if test -z "$session_id" && test -n "$source_session"; then
  session_id=$(sed -n '1s/.*"id":"\([^"]*\)".*/\1/p' "$source_session")
fi
if test -n "$session_id" && test ! -s "/session/session_${session_id}.jsonl"; then
  printf '{"type":"session","version":3,"id":"%s","cwd":"/workspace"}\n' "$session_id" > "/session/session_${session_id}.jsonl"
fi

count=1
if test -f /session/invocations; then
  count=$(( $(cat /session/invocations) + 1 ))
fi
printf '%s\n' "$count" > /session/invocations
printf '%s\n' \
  '{"type":"session","id":"task9-e2e"}' \
  '{"type":"agent_start"}' \
  '{"type":"turn_start"}'
printf 'task9-pi-%s\n' "$count" > /workspace/result.txt
printf '%s\n' \
  '{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"done"}],"stopReason":"stop"}}' \
  '{"type":"agent_settled"}'
