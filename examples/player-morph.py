#!/usr/bin/env python3
"""Draw MPRIS track info, local album art and timed lyrics as Kaleidux morphs."""

import argparse
from bisect import bisect_right
import json
import os
from pathlib import Path
import re
import secrets
import selectors
import subprocess
from urllib.parse import unquote, urlparse


def lyrics(path):
    if path is None:
        return [], []
    rows = []
    offset = 0
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if shift := re.fullmatch(r"\[offset:([+-]?\d+)\]", line):
            offset = int(shift[1]) / 1000
        stamps = list(re.finditer(r"\[(\d+):(\d+(?:\.\d+)?)\]", line))
        for stamp in stamps:
            rows.append((60 * int(stamp[1]) + float(stamp[2]), line[stamps[-1].end():].strip()))
    rows.sort()
    return [when + offset for when, _ in rows], [text for _, text in rows]


def media(player, *args):
    command = ["playerctl"]
    if player:
        command.extend(["--player", player])
    return subprocess.run([*command, *args], text=True, capture_output=True, timeout=4)


def snapshot(player):
    keys = ("artist", "title", "mpris:artUrl", "status", "position")
    pattern = "\x1f".join("{{" + key + "}}" for key in keys)
    reply = media(player, "metadata", "--format", pattern)
    if reply.returncode:
        return None
    fields = reply.stdout.strip("\n").split("\x1f")
    if len(fields) != len(keys):
        return None
    artist, title, album, state, position = fields
    try:
        # playerctl exposes MPRIS time as microseconds in a format string.
        position = float(position) / 1_000_000
    except ValueError:
        position = 0
    url = urlparse(album)
    art = Path(unquote(url.path)) if url.scheme == "file" and not url.netloc else None
    return artist, title, art, state, position


def rect_arg(value):
    try:
        rect = [float(n) for n in value.split(",")]
        x, y, width, height = rect
        if not (0 <= x < 1 and 0 <= y < 1 and 0 < width <= 1 - x and 0 < height <= 1 - y):
            raise ValueError
        return rect
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use a rectangle x,y,width,height within the output") from error


def layers(args, track, when, lines):
    if not track:
        return []
    artist, title, art, state, position = track
    left, top, width, height = args.rect

    def rect(x, y, w, h):
        values = left + x * width, top + y * height, w * width, h * height
        return ",".join(f"{value:.6f}" for value in values)

    def label(value, box, colour, action=None):
        text = " ".join(value.replace("|", " ").split())[:120]
        if not text:
            return None
        layer = f"text={text}|font={args.font}|colour={colour}|rect={box}"
        return f"{layer}|action={args.action_prefix}{action}" if action else layer

    art = args.art or art
    layers = []
    if art and art.is_file() and "|" not in str(art):
        layers.append(f"image={art.resolve()}|rect={rect(0, 0.34, .25, .54)}")
    line = bisect_right(when, position) - 1
    previous = lines[line - 1] if line > 0 else ""
    current = lines[line] if line >= 0 else ""
    labels = (
        label(previous, rect(0, 0, 1, .12), args.dim),
        label(current, rect(0, .14, 1, .17), args.colour),
        label(artist, rect(.28, .34, .72, .13), args.dim),
        label(title, rect(.28, .49, .72, .19), args.colour),
        label("◀", rect(.34, .74, .12, .13), args.colour, "previous"),
        label("Ⅱ" if state == "Playing" else "▶", rect(.49, .74, .12, .13), args.colour, "play-pause"),
        label("▶▏", rect(.64, .74, .12, .13), args.colour, "next"),
    )
    return layers + [value for value in labels if value is not None]


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--player")
    cli.add_argument("-o", "--output")
    cli.add_argument("--lyrics", type=Path, help="Timed LRC file for the current track")
    cli.add_argument("--art", type=Path, help="Override the local album art")
    cli.add_argument("--font", default="Sans 22")
    cli.add_argument("--colour", default="#ffffff")
    cli.add_argument("--dim", default="#aeb3c2")
    cli.add_argument("--rect", type=rect_arg, default="0.06,0.55,0.33,0.35")
    actions = ("previous", "play-pause", "next")
    cli.add_argument("action", choices=("show", *actions), default="show", nargs="?")
    args = cli.parse_args()
    if "|" in args.font:
        cli.error("The font description cannot contain |")
    if args.action != "show":
        return media(args.player, args.action).returncode
    kldctl = os.environ.get("KLDCTL", "kldctl")
    command = [kldctl, "morph"]
    watch = [kldctl, "morph-watch"]
    if args.output:
        command.extend(["-o", args.output])
        watch.extend(["-o", args.output])
    args.action_prefix = "player-" + secrets.token_hex(4) + "."
    when, lines = lyrics(args.lyrics)
    previous = None
    with subprocess.Popen(watch, stdout=subprocess.PIPE) as watcher, selectors.DefaultSelector() as events:
        events.register(watcher.stdout, selectors.EVENT_READ)
        pending = b""
        try:
            while True:
                current = layers(args, snapshot(args.player), when, lines)
                if current != previous:
                    subprocess.run([*command, *current], check=True, timeout=9)
                    previous = current
                if not events.select(.35):
                    continue
                chunk = os.read(watcher.stdout.fileno(), 4096)
                if not chunk:
                    break
                *complete, pending = (pending + chunk).split(b"\n")
                for line in complete:
                    event = json.loads(line)
                    action = event["action"]
                    if action.startswith(args.action_prefix):
                        action = action.removeprefix(args.action_prefix)
                        if action in actions:
                            media(args.player, action)
        except KeyboardInterrupt:
            pass
        finally:
            if watcher.poll() is None:
                watcher.terminate()
            if previous:
                subprocess.run(command, timeout=4)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
