#!/bin/sh
set -eu

session_id=
source_session=
task_packet=
task_packet_count=0
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
    @*)
      task_packet=${1#@}
      task_packet_count=$((task_packet_count + 1))
      shift
      ;;
    *)
      shift
      ;;
  esac
done

if test "$task_packet_count" -ne 1 || test ! -r "$task_packet"; then
  printf 'expected exactly one readable @task-packet argument\n' >&2
  exit 64
fi
goal=$(sed -n 's/.*"goal"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$task_packet")
if test -z "$goal"; then
  printf 'task packet has no distinctive goal\n' >&2
  exit 65
fi

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
printf 'task9-result:%s\n' "$goal" > /workspace/result.txt
printf '%s\n' \
  '{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"done"}],"stopReason":"stop"}}' \
  '{"type":"agent_settled"}'
