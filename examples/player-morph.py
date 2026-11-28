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
import sys


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


def media(kldctl, player, *args):
    command = [kldctl, "player"]
    if player:
        command.extend(["--player", player])
    return subprocess.run([*command, *args], text=True, capture_output=True, timeout=4)


def snapshot(kldctl, player):
    reply = media(kldctl, player)
    if reply.returncode:
        raise ValueError(reply.stderr.strip().split("\n", 1)[0] or "Cannot read desktop player")
    status = json.loads(reply.stdout)
    if status is not None and not isinstance(status, dict):
        raise ValueError("Unexpected desktop player response")
    return status


def track_lyrics(directory, title, artists):
    if not directory or not title:
        return [], []
    artist = ", ".join(artists)
    names = [f"{artist} - {title}", title] if artist else [title]
    for name in names:
        if "/" in name or "\x00" in name:
            continue
        try:
            return lyrics(directory / f"{name}.lrc")
        except (OSError, UnicodeError):
            continue
    return [], []


def rect_arg(value):
    try:
        rect = [float(n) for n in value.split(",")]
        x, y, width, height = rect
        if not (0 <= x < 1 and 0 <= y < 1 and 0 < width <= 1 - x and 0 < height <= 1 - y):
            raise ValueError
        return rect
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use a rectangle x,y,width,height within the output") from error


def layers(args, track, when, lines, action_prefix):
    if not track:
        return []
    artist = ", ".join(track["artists"])
    title = track["title"] or ""
    playing = track["state"] == "playing"
    position = (track["position_us"] or 0) / 1_000_000
    art = args.art or (Path(track["artwork_path"]) if track["artwork_path"] else None)
    left, top, width, height = args.rect

    def rect(x, y, w, h):
        values = left + x * width, top + y * height, w * width, h * height
        return ",".join(f"{value:.6f}" for value in values)

    def label(value, box, colour, action=None, font=None):
        text = " ".join(value.replace("|", " ").split())[:120]
        if not text:
            return None
        layer = f"text={text}|font={font or args.font}|colour={colour}|rect={box}"
        return f"{layer}|action={action_prefix}{action}" if action else layer

    layers = []
    if art and art.is_file() and "|" not in str(art):
        layers.append(f"image={art.resolve()}|rect={rect(0, .44, .25, .48)}")
    line = bisect_right(when, position) - 1
    previous = lines[line - 1] if line > 0 else ""
    current = lines[line] if line >= 0 else ""
    upcoming = lines[line + 1] if line + 1 < len(lines) else ""
    toggle = None
    if playing and track["can_pause"]:
        toggle = label("Ⅱ", rect(.49, .81, .12, .13), args.colour, "pause")
    elif not playing and track["can_play"]:
        toggle = label("▶", rect(.49, .81, .12, .13), args.colour, "play")
    labels = (
        label(previous, rect(0, 0, 1, .1), args.dim, font=args.lyrics_font),
        label(current, rect(0, .11, 1, .16), args.colour, font=args.lyrics_font),
        label(upcoming, rect(0, .28, 1, .1), args.dim, font=args.lyrics_font),
        label(artist, rect(.28, .44, .72, .13), args.dim),
        label(title, rect(.28, .58, .72, .19), args.colour),
        label("◀", rect(.34, .81, .12, .13), args.colour, "previous") if track["can_previous"] else None,
        toggle,
        label("▶▏", rect(.64, .81, .12, .13), args.colour, "next") if track["can_next"] else None,
    )
    return layers + [value for value in labels if value is not None]


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--player")
    cli.add_argument("-o", "--output")
    lyric_source = cli.add_mutually_exclusive_group()
    lyric_source.add_argument("--lyrics", type=Path, help="Timed LRC file for the current track")
    lyric_source.add_argument("--lyrics-dir", type=Path, help="Directory of Artist - Title.lrc or Title.lrc files")
    cli.add_argument("--art", type=Path, help="Override the local album art")
    cli.add_argument("--font", default="Sans 22")
    cli.add_argument("--lyrics-font", help="Use a different font for lyrics")
    cli.add_argument("--group", default="player", help="Independent name for the player morphs")
    cli.add_argument("--colour", default="#ffffff")
    cli.add_argument("--dim", default="#aeb3c2")
    cli.add_argument("--rect", type=rect_arg, default="0.06,0.55,0.33,0.35")
    actions = ("previous", "play-pause", "next", "play", "pause")
    cli.add_argument("action", choices=("show", *actions), default="show", nargs="?")
    args = cli.parse_args()
    if "|" in args.font or "|" in (args.lyrics_font or ""):
        cli.error("The font description cannot contain |")
    if args.lyrics_dir and not args.lyrics_dir.is_dir():
        cli.error(f"Lyrics directory does not exist: {args.lyrics_dir}")
    kldctl = os.environ.get("KLDCTL", "kldctl")
    if args.action != "show":
        reply = media(kldctl, args.player, args.action)
        if reply.returncode:
            cli.exit(reply.returncode, reply.stderr)
        return 0
    command = [kldctl, "morph", "--group", args.group]
    watch = [kldctl, "morph-watch"]
    if args.output:
        command.extend(["-o", args.output])
        watch.extend(["-o", args.output])
    try:
        when, lines = lyrics(args.lyrics)
    except (OSError, UnicodeError) as error:
        cli.error(str(error))
    previous = None
    player = None
    track_key = None
    action_prefix = ""
    track = None
    status_error = None
    with subprocess.Popen(watch, stdout=subprocess.PIPE) as watcher, selectors.DefaultSelector() as events:
        events.register(watcher.stdout, selectors.EVENT_READ)
        pending = b""
        try:
            while True:
                try:
                    track = snapshot(kldctl, args.player)
                    status_error = None
                except (ValueError, subprocess.TimeoutExpired) as error:
                    if str(error) != status_error:
                        status_error = str(error)
                        print(status_error, file=sys.stderr)
                next_player = track["player"] if track else None
                if next_player != player:
                    player = next_player
                    action_prefix = "player-" + secrets.token_hex(4) + "."
                if args.lyrics_dir and track:
                    key = player, track["track_id"], track["title"], track["artists"]
                    if key != track_key:
                        track_key = key
                        when, lines = track_lyrics(args.lyrics_dir, track["title"], track["artists"])
                current = layers(args, track, when, lines, action_prefix)
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
                    if player and action.startswith(action_prefix):
                        action = action.removeprefix(action_prefix)
                        if action in actions:
                            media(kldctl, player, action)
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
