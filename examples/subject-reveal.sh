#!/bin/sh
# Render a subject or colony reveal before switching to a local wallpaper.
set -eu

if [ "$#" -lt 2 ]; then
    printf '%s\n' 'usage: subject-reveal.sh outline|contagion SOURCE [kldctl set options]' >&2
    exit 2
fi

mode=$1
source=$2
shift 2
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

case "$mode" in
    outline)
        kldctl frame --first --source "$source" --max-size 768 >"$work/first.png"
        rembg i -m birefnet-general "$work/first.png" "$work/subject.png"
        python3 "$script_dir/reveal-map.py" outline "$work/subject.png" "$work/reveal.png"
        ;;
    contagion)
        python3 "$script_dir/reveal-map.py" contagion "$work/reveal.png" --seed "$(date +%s)"
        ;;
    *)
        printf 'Unknown reveal mode %s\n' "$mode" >&2
        exit 2
        ;;
esac

kldctl set "$source" --reveal-map "$work/reveal.png" "$@"
