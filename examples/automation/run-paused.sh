#!/bin/sh
# Run a foreground command while holding an independent wallpaper pause reason.
# Usage: sh run-paused.sh command [arguments...]
set -u

if [ "$#" -eq 0 ]; then
    printf '%s\n' 'usage: run-paused.sh command [arguments...]' >&2
    exit 2
fi

reason="command-$$"
if ! kldctl inhibit "$reason"; then
    printf '%s\n' 'Could not pause Kaleidux; starting command without an inhibitor.' >&2
    exec "$@"
fi

child=
cleanup() {
    kldctl uninhibit "$reason" >/dev/null 2>&1 || :
}
interrupt() {
    signal=$1
    status=$2
    trap '' INT TERM HUP
    if [ -n "$child" ]; then
        kill -"$signal" "$child" 2>/dev/null || :
        wait "$child" 2>/dev/null || :
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'interrupt INT 130' INT
trap 'interrupt TERM 143' TERM
trap 'interrupt HUP 129' HUP

# A non-interactive shell makes asynchronous children ignore INT/QUIT.
# Restore their defaults before exec so forwarded Ctrl-C reaches the command.
env --default-signal=INT,QUIT -- "$@" <&0 &
child=$!
wait "$child"
status=$?
exit "$status"
