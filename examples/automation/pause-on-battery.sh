#!/bin/sh
# Optional user-session hook; does not change system power settings.
# Usage: sh pause-on-battery.sh [/sys/class/power_supply/BAT0] [threshold-percent]
set -u

battery=${1:-/sys/class/power_supply/BAT0}
threshold=${2:-20}
case "$threshold" in
    ''|*[!0-9]*) printf '%s\n' 'threshold must be an integer from 0 to 100' >&2; exit 2 ;;
esac
if ! [ "$threshold" -le 100 ] 2>/dev/null; then
    printf '%s\n' 'threshold must be an integer from 0 to 100' >&2
    exit 2
fi
if [ ! -r "$battery/status" ] || [ ! -r "$battery/capacity" ]; then
    printf 'No readable battery status/capacity under %s\n' "$battery" >&2
    exit 1
fi

reason="battery-$$"
sleeper=
cleanup() {
    if [ -n "$sleeper" ]; then
        kill "$sleeper" 2>/dev/null || :
        wait "$sleeper" 2>/dev/null || :
    fi
    kldctl uninhibit "$reason" >/dev/null 2>&1 || :
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

while :; do
    status=$(cat "$battery/status" 2>/dev/null) || status=
    capacity=$(cat "$battery/capacity" 2>/dev/null) || capacity=
    low=false
    case "$capacity" in
        ''|*[!0-9]*) ;;
        *) if [ "$status" = Discharging ] && [ "$capacity" -le "$threshold" ] 2>/dev/null; then
               low=true
           fi ;;
    esac
    if [ "$low" = true ]; then
        kldctl inhibit "$reason" >/dev/null || :
    else
        # Release our own reason on AC, sufficient charge, or a failed read.
        kldctl uninhibit "$reason" >/dev/null || :
    fi
    sleep 30 &
    sleeper=$!
    wait "$sleeper" || :
    sleeper=
done
