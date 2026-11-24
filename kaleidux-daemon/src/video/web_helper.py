"""Render a local HTML page into offscreen RGBA frames requested on stdin."""

import os
from pathlib import Path
import sys

import gi

gi.require_version("Gtk", "3.0")
gi.require_version("WebKit2", "4.1")
from gi.repository import GLib, Gtk, WebKit2


def main():
    path, width, height = sys.argv[1:]
    width, height = int(width), int(height)
    window = Gtk.OffscreenWindow()
    window.set_default_size(width, height)
    view = WebKit2.WebView.new_with_context(WebKit2.WebContext.new_ephemeral())
    view.set_is_muted(True)
    settings = view.get_settings()
    settings.set_hardware_acceleration_policy(WebKit2.HardwareAccelerationPolicy.NEVER)
    settings.set_enable_write_console_messages_to_stdout(False)
    window.add(view)
    window.show_all()

    loaded = False
    requested = False
    dirty = True

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
            raise RuntimeError("Browser canvas changed size")
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

    def request(_fd, condition):
        nonlocal requested
        if condition & GLib.IO_HUP or os.read(0, 1) != b"R":
            Gtk.main_quit()
            return False
        requested = True
        if loaded:
            GLib.timeout_add(8, capture)
        return True

    def on_load(_view, event):
        nonlocal loaded
        if event == WebKit2.LoadEvent.FINISHED:
            loaded = True
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


if __name__ == "__main__":
    main()
