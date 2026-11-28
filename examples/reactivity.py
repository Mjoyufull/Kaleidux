#!/usr/bin/env python3
"""Send date, pointer and optional desktop-audio spectrum to a Kaleidux output."""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import warnings


def exchange(path, request):
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(1)
        connection.connect(str(path))
        connection.sendall(request)
        with connection.makefile("rb") as response:
            return response.read(128 * 1024)


def cursor_provider(runtime, output):
    signature = os.environ.get("HYPRLAND_INSTANCE_SIGNATURE")
    if not signature:
        raise ValueError("--pointer needs a Hyprland session")
    control = runtime / "hypr" / signature / ".socket.sock"
    monitors = json.loads(exchange(control, b"j/monitors"))
    monitor = next((m for m in monitors if m["name"] == output), None)
    if monitor is None and output is None:
        monitor = next((m for m in monitors if m["focused"]), None)
    if monitor is None:
        raise ValueError("Select the monitor to track with --output")
    width, height = monitor["width"], monitor["height"]
    if monitor["transform"] in (1, 3, 5, 7):
        width, height = height, width

    def pointer():
        cursor = json.loads(exchange(control, b"j/cursorpos"))
        coordinates = (
            (cursor["x"] - monitor["x"]) * monitor["scale"] / width,
            (cursor["y"] - monitor["y"]) * monitor["scale"] / height,
        )
        return [round(max(0.0, min(1.0, value)), 4) for value in coordinates] + [0, 0]

    return monitor["name"], pointer


def audio_provider(source, fps):
    import gi
    gi.require_version("Gst", "1.0")
    from gi.repository import Gst
    Gst.init(None)
    if source is None:
        source = subprocess.check_output(["pactl", "get-default-sink"], text=True, timeout=3).strip() + ".monitor"
    pipeline = Gst.parse_launch(
        f"pulsesrc name=desktop ! audioconvert ! audioresample ! "
        f"spectrum name=analysis bands=64 threshold=-65 interval={round(1e9 / fps)} ! fakesink"
    )
    pipeline.get_by_name("desktop").set_property("device", source)
    if pipeline.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
        raise ValueError("Could not open the desktop audio monitor")
    bus = pipeline.get_bus()

    def bands():
        latest = None
        while message := bus.pop():
            if message.type == Gst.MessageType.ERROR:
                error, _ = message.parse_error()
                raise ValueError(str(error))
            data = message.get_structure()
            if data and data.has_name("spectrum"):
                ok, values = data.get_list("magnitude")
                if ok:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", DeprecationWarning)
                        latest = [values.get_nth(i) for i in range(values.n_values)]
        if latest is None:
            return None
        return [round(max(0.0, min(1.0, (value + 65) / 65)), 3) for value in latest]

    return bands, lambda: pipeline.set_state(Gst.State.NULL)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("-o", "--output")
    cli.add_argument("--socket", type=Path)
    cli.add_argument("--fps", type=int, default=24)
    cli.add_argument("--audio", action="store_true")
    cli.add_argument("--date", action="store_true")
    cli.add_argument("--monitor", help="PulseAudio monitor source, defaults to the default sink")
    cli.add_argument("--pointer", action="store_true", help="Track the selected Hyprland output")
    args = cli.parse_args()
    if not 1 <= args.fps <= 120:
        cli.error("--fps must be 1–120")
    runtime = Path(os.environ.get("XDG_RUNTIME_DIR", "/tmp"))
    control = args.socket or runtime / "kaleidux.sock"
    read_date = args.date or not (args.audio or args.pointer)
    read_pointer = lambda: None
    read_audio = lambda: None
    stop_audio = lambda: None
    try:
        def stop(*_):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, stop)
        if args.pointer:
            args.output, read_pointer = cursor_provider(runtime, args.output)
        if args.audio:
            read_audio, stop_audio = audio_provider(args.monitor, args.fps)
        previous = {}
        period = 1 / args.fps if args.pointer or args.audio else 1
        while True:
            started = time.monotonic()
            now = datetime.now()
            current = {
                "date": [now.year, now.month, now.day, now.hour * 3600 + now.minute * 60 + now.second] if read_date else None,
                "audio": read_audio(),
                "pointer": read_pointer(),
            }
            changes = {key: value for key, value in current.items()
                       if value is not None and value != previous.get(key)}
            if changes:
                request = {"method": "layer_inputs", "params": {"output": args.output, **changes}}
                response = json.loads(exchange(control, json.dumps(request).encode() + b"\n"))
                if response != "Ok":
                    raise ValueError(str(response))
                previous.update(changes)
            time.sleep(max(0, period - (time.monotonic() - started)))
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError, ImportError) as error:
        cli.exit(1, f"reactivity: {error}\n")
    finally:
        stop_audio()
        if args.audio:
            request = {"method": "layer_inputs", "params": {"output": args.output, "audio": []}}
            try:
                exchange(control, json.dumps(request).encode() + b"\n")
            except OSError:
                pass


if __name__ == "__main__":
    main()
