#!/bin/sh
set -eu

LABEL="${RESTART_LABEL:-app}"
DELAY="${RESTART_DELAY_SEC:-5}"

echo "[$LABEL] entrypoint: $*"
while true; do
  echo "[$LABEL] start $(date -Is)"
  "$@" || code=$?
  code=${code:-0}
  echo "[$LABEL] exit $code, sleep ${DELAY}s"
  sleep "$DELAY"
done
