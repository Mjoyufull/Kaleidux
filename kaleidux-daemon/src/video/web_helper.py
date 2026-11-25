"""Render a local HTML page into offscreen RGBA frames requested on stdin."""

import json
import os
from pathlib import Path
import sys

import gi

gi.require_version("Gtk", "3.0")
gi.require_version("WebKit2", "4.1")
gi.require_version("GdkPixbuf", "2.0")
from gi.repository import GdkPixbuf, GLib, Gtk, WebKit2


def main():
    path, width, height, fps, bridge = sys.argv[1:]
    width, height, fps = int(width), int(height), int(fps)
    window = Gtk.OffscreenWindow()
    window.set_default_size(width, height)
    view = WebKit2.WebView.new_with_context(WebKit2.WebContext.new_ephemeral())
    view.get_user_content_manager().add_script(WebKit2.UserScript.new(
        bridge, WebKit2.UserContentInjectedFrames.TOP_FRAME,
        WebKit2.UserScriptInjectionTime.START, None, None
    ))
    view.set_is_muted(True)
    settings = view.get_settings()
    settings.set_hardware_acceleration_policy(WebKit2.HardwareAccelerationPolicy.NEVER)
    settings.set_enable_write_console_messages_to_stdout(False)
    window.add(view)
    window.show_all()

    loaded = False
    requested = False
    dirty = True
    pending = b""

    def damaged(*_):
        nonlocal dirty
        dirty = True
        return False

    def capture():
        nonlocal dirty, requested
        if not dirty:
            sys.stdout.buffer.write(b"I")
            sys.stdout.buffer.flush()
            requested = False
            return False
        pixbuf = window.get_pixbuf()
        if pixbuf is None:
            return True
        if (pixbuf.get_width(), pixbuf.get_height()) != (width, height):
            pixbuf = pixbuf.scale_simple(width, height, GdkPixbuf.InterpType.BILINEAR)
        if not pixbuf.get_has_alpha():
            pixbuf = pixbuf.add_alpha(False, 0, 0, 0)
        pixels = pixbuf.get_pixels()
        stride = pixbuf.get_rowstride()
        sys.stdout.buffer.write(b"F")
        if stride == width * 4:
            sys.stdout.buffer.write(pixels)
        else:
            for offset in range(0, stride * height, stride):
                sys.stdout.buffer.write(pixels[offset:offset + width * 4])
        sys.stdout.buffer.flush()
        dirty = False
        requested = False
        return False

    def send_to_page(method, value):
        js = f"globalThis.__kaleiduxWallpaper.{method}({json.dumps(value, separators=(',', ':'))})"
        view.evaluate_javascript(js, -1, None, None, None, None, None)

    def dispatch_request(message):
        nonlocal requested
        if message in (b"P", b"C"):
            if loaded:
                send_to_page("pause", message == b"P")
            return
        requested = True
        if not loaded:
            return
        if message == b"R":
            GLib.timeout_add(8, capture)
        else:
            send_to_page("input", json.loads(message))
            GLib.timeout_add(8, capture)

    def request(_fd, condition):
        nonlocal pending
        chunk = b"" if condition & GLib.IO_HUP else os.read(0, 4096)
        if not chunk:
            Gtk.main_quit()
            return False
        *lines, pending = (pending + chunk).split(b"\n")
        if len(pending) > 4096:
            Gtk.main_quit()
            return False
        for line in lines:
            dispatch_request(line)
        return True

    def on_load(_view, event):
        nonlocal loaded
        if event == WebKit2.LoadEvent.FINISHED:
            loaded = True
            send_to_page("start", {"fps": fps, "properties": project_properties(Path(path))})
            if requested:
                GLib.timeout_add(30, capture)

    def load_failed(*_):
        Gtk.main_quit()
        return True

    view.connect("load-changed", on_load)
    view.connect("load-failed", load_failed)
    view.connect("web-process-terminated", load_failed)
    window.connect("damage-event", damaged)
    GLib.io_add_watch(0, GLib.PRIORITY_DEFAULT, GLib.IO_IN | GLib.IO_HUP, request)
    with Path(path).open("rb") as markup:
        data = markup.read(8 * 1024 * 1024 + 1)
    if len(data) > 8 * 1024 * 1024:
        raise ValueError("HTML wallpaper exceeds 8 MiB")
    view.load_html(data.decode("utf-8-sig"), Path(path).as_uri())
    Gtk.main()


def project_properties(path):
    project = path.parent / "project.json"
    if not project.is_file():
        return {}
    try:
        with project.open("rb") as source:
            data = source.read(1024 * 1024 + 1)
        if len(data) <= 1024 * 1024:
            properties = json.loads(data).get("general", {}).get("properties", {})
            if isinstance(properties, dict):
                return {
                    key: {"value": item["value"]}
                    for key, item in properties.items()
                    if isinstance(item, dict) and "value" in item
                }
    except (ValueError, OSError, AttributeError):
        pass
    return {}


if __name__ == "__main__":
    main()
