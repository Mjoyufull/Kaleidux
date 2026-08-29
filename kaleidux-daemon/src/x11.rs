use raw_window_handle::{
    DisplayHandle, HandleError, HasDisplayHandle, HasWindowHandle, RawDisplayHandle,
    RawWindowHandle, WindowHandle, XcbDisplayHandle, XcbWindowHandle,
};
use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use tracing::{info, warn};
use x11rb::connection::Connection;
use x11rb::protocol::dri3::ConnectionExt as Dri3Ext;
use x11rb::protocol::present::ConnectionExt as PresentExt;
use x11rb::protocol::xproto::{
    Atom, ConnectionExt, CreateWindowAux, EventMask, PropMode, Window, WindowClass,
};
use x11rb::wrapper::ConnectionExt as _;
use x11rb::xcb_ffi::XCBConnection;

/// X11 Backend handling connection and window management
pub struct X11Backend {
    pub conn: Arc<XCBConnection>,
    pub screen_num: usize,
    pub root: Window,
    pub windows: HashMap<String, Window>,
    pub atoms: Atoms,
    #[allow(clippy::type_complexity)]
    pub cached_monitors: parking_lot::Mutex<Option<Vec<(String, i16, i16, u16, u16)>>>,
    pub monitors_dirty: std::sync::atomic::AtomicBool,
    pub capabilities: X11Capabilities,
    present_event_ids: HashMap<Window, u32>,
}

#[derive(Debug, Clone)]
pub struct X11Capabilities {
    pub server_vendor: String,
    pub server_release: u32,
    pub is_xwayland: bool,
    pub dri3_version: Option<(u32, u32)>,
    pub present_version: Option<(u32, u32)>,
    pub present_capabilities: u32,
    pub dri3_device_rdev: Option<u64>,
    pub root_depth: u8,
    pub root_bpp: u8,
}

impl X11Capabilities {
    pub fn present_syncobj(&self) -> bool {
        self.present_capabilities & u32::from(x11rb::protocol::present::Capability::SYNCOBJ) != 0
    }

    pub fn tier1_protocol_ready(&self) -> bool {
        self.dri3_version.is_some_and(|version| version >= (1, 4))
            && self
                .present_version
                .is_some_and(|version| version >= (1, 4))
            && self.present_syncobj()
            && self.dri3_device_rdev.is_some()
    }

    pub fn server_kind(&self) -> &'static str {
        if self.is_xwayland { "xwayland" } else { "xorg" }
    }
}

pub struct Atoms {
    pub _net_wm_window_type: Atom,
    pub _net_wm_window_type_desktop: Atom,
    pub _net_wm_state: Atom,
    pub _net_wm_state_fullscreen: Atom,
    pub _net_wm_state_below: Atom,
    pub _net_wm_state_sticky: Atom,
    pub _net_wm_state_skip_taskbar: Atom,
}

impl X11Backend {
    pub fn new() -> anyhow::Result<Self> {
        // Connect using XCB (requires libxcb)
        let (conn, screen_num) = XCBConnection::connect(None)?;
        let conn = Arc::new(conn);

        let screen = &conn.setup().roots[screen_num];
        let root = screen.root;
        let capabilities = probe_capabilities(&conn, screen_num)?;

        // Intern atoms
        let _net_wm_window_type = conn
            .intern_atom(false, b"_NET_WM_WINDOW_TYPE")?
            .reply()?
            .atom;
        let _net_wm_window_type_desktop = conn
            .intern_atom(false, b"_NET_WM_WINDOW_TYPE_DESKTOP")?
            .reply()?
            .atom;
        let _net_wm_state = conn.intern_atom(false, b"_NET_WM_STATE")?.reply()?.atom;
        let _net_wm_state_fullscreen = conn
            .intern_atom(false, b"_NET_WM_STATE_FULLSCREEN")?
            .reply()?
            .atom;
        let _net_wm_state_below = conn
            .intern_atom(false, b"_NET_WM_STATE_BELOW")?
            .reply()?
            .atom;
        let _net_wm_state_sticky = conn
            .intern_atom(false, b"_NET_WM_STATE_STICKY")?
            .reply()?
            .atom;
        let _net_wm_state_skip_taskbar = conn
            .intern_atom(false, b"_NET_WM_STATE_SKIP_TASKBAR")?
            .reply()?
            .atom;

        // Subscribe to RandR events
        use x11rb::protocol::randr::ConnectionExt as RandrExt;
        let _ = conn.randr_select_input(
            root,
            x11rb::protocol::randr::NotifyMask::OUTPUT_CHANGE
                | x11rb::protocol::randr::NotifyMask::CRTC_CHANGE,
        );

        Ok(Self {
            conn,
            screen_num,
            root,
            windows: HashMap::new(),
            atoms: Atoms {
                _net_wm_window_type,
                _net_wm_window_type_desktop,
                _net_wm_state,
                _net_wm_state_fullscreen,
                _net_wm_state_below,
                _net_wm_state_sticky,
                _net_wm_state_skip_taskbar,
            },
            cached_monitors: parking_lot::Mutex::new(None),
            monitors_dirty: std::sync::atomic::AtomicBool::new(true),
            capabilities,
            present_event_ids: HashMap::new(),
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn get_monitors(&self) -> anyhow::Result<Vec<(String, i16, i16, u16, u16)>> {
        self.query_monitors(true)
    }

    #[allow(clippy::type_complexity)]
    pub fn get_connected_monitors(&self) -> anyhow::Result<Vec<(String, i16, i16, u16, u16)>> {
        self.query_monitors(false)
    }

    #[allow(clippy::type_complexity)]
    fn query_monitors(
        &self,
        include_screen_fallback: bool,
    ) -> anyhow::Result<Vec<(String, i16, i16, u16, u16)>> {
        use x11rb::protocol::randr::ConnectionExt as RandrExt;

        // Fast path: return cache if not dirty
        if !self
            .monitors_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            if let Some(monitors) = self.cached_monitors.lock().as_ref() {
                let mut monitors = monitors.clone();
                if monitors.is_empty() && include_screen_fallback {
                    let screen = &self.conn.setup().roots[self.screen_num];
                    monitors.push((
                        "X11-0".to_string(),
                        0,
                        0,
                        screen.width_in_pixels,
                        screen.height_in_pixels,
                    ));
                }
                return Ok(monitors);
            }
        }

        let screen_res = self
            .conn
            .randr_get_screen_resources_current(self.root)?
            .reply()?;
        let mut monitors = Vec::new();

        for &crtc in &screen_res.crtcs {
            let crtc_info = self
                .conn
                .randr_get_crtc_info(crtc, screen_res.config_timestamp)?
                .reply()?;

            if crtc_info.mode == 0 {
                continue;
            } // Inactive CRTC

            // Find output name connected to this CRTC
            let mut name = format!("X11-{}", crtc); // Fallback
            if let Some(&output) = crtc_info.outputs.first() {
                let output_info = self
                    .conn
                    .randr_get_output_info(output, screen_res.config_timestamp)?
                    .reply()?;
                name = String::from_utf8_lossy(&output_info.name).to_string();
            }

            monitors.push((
                name,
                crtc_info.x,
                crtc_info.y,
                crtc_info.width,
                crtc_info.height,
            ));
        }

        // Cache only the raw RandR result. Whether to synthesize a fallback is
        // a property of the caller, not of the monitor query itself.
        {
            let mut cache = self.cached_monitors.lock();
            *cache = Some(monitors.clone());
            self.monitors_dirty
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }

        if monitors.is_empty() && include_screen_fallback {
            let screen = &self.conn.setup().roots[self.screen_num];
            monitors.push((
                "X11-0".to_string(),
                0,
                0,
                screen.width_in_pixels,
                screen.height_in_pixels,
            ));
        }

        Ok(monitors)
    }

    pub fn create_wallpaper_window(
        &mut self,
        name: &str,
        x: i16,
        y: i16,
        width: u16,
        height: u16,
    ) -> anyhow::Result<Window> {
        let win_id = self.conn.generate_id()?;
        let screen = &self.conn.setup().roots[self.screen_num];

        // Setup window attributes
        // REMOVED override_redirect(1) to let WM handle stacking (keeping it below apps)
        let win_aux = CreateWindowAux::new()
            .event_mask(EventMask::EXPOSURE | EventMask::STRUCTURE_NOTIFY)
            // Xwayland is rootless and Hyprland does not treat EWMH desktop
            // windows as compositor wallpaper layers. Keeping the window
            // unmanaged preserves the requested monitor rectangle instead of
            // letting the compositor tile it as an application window.
            .override_redirect(u32::from(self.capabilities.is_xwayland))
            // Keep startup black until the renderer presents real content.
            .background_pixel(screen.black_pixel);

        self.conn.create_window(
            x11rb::COPY_DEPTH_FROM_PARENT,
            win_id,
            self.root,
            x,
            y,
            width,
            height,
            0,
            WindowClass::INPUT_OUTPUT,
            0,
            &win_aux,
        )?;

        // Set _NET_WM_WINDOW_TYPE = _NET_WM_WINDOW_TYPE_DESKTOP
        self.conn.change_property(
            PropMode::REPLACE,
            win_id,
            self.atoms._net_wm_window_type,
            x11rb::protocol::xproto::AtomEnum::ATOM,
            32,
            1,
            &self.atoms._net_wm_window_type_desktop.to_ne_bytes(),
        )?;

        // Give compositors and diagnostics a stable identity even when the
        // rootless Xwayland window is override-redirect.
        self.conn.change_property8(
            PropMode::REPLACE,
            win_id,
            x11rb::protocol::xproto::AtomEnum::WM_CLASS,
            x11rb::protocol::xproto::AtomEnum::STRING,
            b"kaleidux-wallpaper\0KaleiduxWallpaper\0",
        )?;
        self.conn.change_property8(
            PropMode::REPLACE,
            win_id,
            x11rb::protocol::xproto::AtomEnum::WM_NAME,
            x11rb::protocol::xproto::AtomEnum::STRING,
            b"Kaleidux Wallpaper",
        )?;

        // Set _NET_WM_STATE = [_NET_WM_STATE_FULLSCREEN, _NET_WM_STATE_BELOW]
        let states = [
            self.atoms._net_wm_state_fullscreen,
            self.atoms._net_wm_state_below,
            self.atoms._net_wm_state_sticky,
            self.atoms._net_wm_state_skip_taskbar,
        ];

        let mut stated_bytes = Vec::new();
        for s in states {
            stated_bytes.extend_from_slice(&s.to_ne_bytes());
        }

        self.conn.change_property(
            PropMode::REPLACE,
            win_id,
            self.atoms._net_wm_state,
            x11rb::protocol::xproto::AtomEnum::ATOM,
            32, // atom is 32-bit
            states.len() as u32,
            &stated_bytes,
        )?;

        // Map window
        self.conn.map_window(win_id)?;

        self.probe_window_present(win_id)?;

        // Lower window to the bottom of the stack
        use x11rb::protocol::xproto::StackMode;
        self.conn.configure_window(
            win_id,
            &x11rb::protocol::xproto::ConfigureWindowAux::new().stack_mode(StackMode::BELOW),
        )?;

        self.conn.flush()?;
        // Wait for server to process all requests (Audit Point 11)
        self.conn.sync()?;

        self.windows.insert(name.to_string(), win_id);

        info!(
            "Created X11 wallpaper window for {}: id={}, rect={}x{}@{},{}",
            name, win_id, width, height, x, y
        );

        Ok(win_id)
    }

    pub fn configure_wallpaper_window(
        &self,
        window: Window,
        x: i16,
        y: i16,
        width: u16,
        height: u16,
    ) -> anyhow::Result<()> {
        self.conn.configure_window(
            window,
            &x11rb::protocol::xproto::ConfigureWindowAux::new()
                .x(i32::from(x))
                .y(i32::from(y))
                .width(u32::from(width))
                .height(u32::from(height)),
        )?;
        Ok(())
    }

    pub fn destroy_wallpaper_window(&mut self, name: &str) -> anyhow::Result<Option<Window>> {
        let Some(window) = self.windows.get(name).copied() else {
            return Ok(None);
        };
        if let Some(event_id) = self.present_event_ids.get(&window).copied() {
            let _ = self.conn.present_select_input(
                event_id,
                window,
                x11rb::protocol::present::EventMask::NO_EVENT,
            );
        }
        self.conn.destroy_window(window)?;
        // Keep both indexes intact if queuing XDestroyWindow fails so the
        // reconciliation orphan sweep can retry and event routing remains
        // coherent until then.
        self.windows.remove(name);
        self.present_event_ids.remove(&window);
        Ok(Some(window))
    }

    fn probe_window_present(&mut self, window: Window) -> anyhow::Result<()> {
        let Some(version) = self.capabilities.present_version else {
            return Ok(());
        };

        let capabilities = self.conn.present_query_capabilities(window)?.reply()?;
        let window_syncobj = capabilities.capabilities
            & u32::from(x11rb::protocol::present::Capability::SYNCOBJ)
            != 0;
        let window_tier1_ready = self
            .capabilities
            .dri3_version
            .is_some_and(|dri3| dri3 >= (1, 4))
            && version >= (1, 4)
            && window_syncobj
            && self.capabilities.dri3_device_rdev.is_some();
        let event_id = self.conn.generate_id()?;
        self.conn.present_select_input(
            event_id,
            window,
            x11rb::protocol::present::EventMask::CONFIGURE_NOTIFY
                | x11rb::protocol::present::EventMask::COMPLETE_NOTIFY
                | x11rb::protocol::present::EventMask::IDLE_NOTIFY,
        )?;
        self.present_event_ids.insert(window, event_id);

        let modifier_counts = if self
            .capabilities
            .dri3_version
            .is_some_and(|dri3| dri3 >= (1, 2))
        {
            match self
                .conn
                .dri3_get_supported_modifiers(
                    window,
                    self.capabilities.root_depth,
                    self.capabilities.root_bpp,
                )?
                .reply()
            {
                Ok(reply) => (reply.window_modifiers.len(), reply.screen_modifiers.len()),
                Err(error) => {
                    warn!("[X11-CAPS] DRI3 modifier query failed for window {window}: {error}");
                    (0, 0)
                }
            }
        } else {
            (0, 0)
        };

        info!(
            "[X11-CAPS] window={} present={}.{} capabilities=0x{:x} syncobj={} depth={} bpp={} window_modifiers={} screen_modifiers={} tier1_protocol_ready={}",
            window,
            version.0,
            version.1,
            capabilities.capabilities,
            window_syncobj,
            self.capabilities.root_depth,
            self.capabilities.root_bpp,
            modifier_counts.0,
            modifier_counts.1,
            window_tier1_ready,
        );
        Ok(())
    }
}

fn probe_capabilities(conn: &XCBConnection, screen_num: usize) -> anyhow::Result<X11Capabilities> {
    let setup = conn.setup();
    let screen = &setup.roots[screen_num];
    let server_vendor = String::from_utf8_lossy(&setup.vendor).into_owned();
    let is_xwayland = conn.query_extension(b"XWAYLAND")?.reply()?.present;
    let dri3_present = conn.query_extension(b"DRI3")?.reply()?.present;
    let present_present = conn.query_extension(b"Present")?.reply()?.present;
    let dri3_version = if dri3_present {
        let reply = conn.dri3_query_version(1, 4)?.reply()?;
        Some((reply.major_version, reply.minor_version))
    } else {
        None
    };
    let present_version = if present_present {
        let reply = conn.present_query_version(1, 4)?.reply()?;
        Some((reply.major_version, reply.minor_version))
    } else {
        None
    };
    let present_capabilities = if present_present {
        conn.present_query_capabilities(screen.root)?
            .reply()?
            .capabilities
    } else {
        0
    };
    let dri3_device_rdev = if dri3_present {
        match conn.dri3_open(screen.root, 0)?.reply() {
            Ok(reply) => {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                // SAFETY: `stat` points to writable storage and the owned DRI3
                // device fd remains live for the duration of `fstat`.
                let result = unsafe { libc::fstat(reply.device_fd.as_raw_fd(), stat.as_mut_ptr()) };
                if result == 0 {
                    // SAFETY: successful fstat initialized the complete struct.
                    Some(unsafe { stat.assume_init() }.st_rdev)
                } else {
                    warn!(
                        "[X11-CAPS] fstat on the DRI3 device failed: {}",
                        std::io::Error::last_os_error()
                    );
                    None
                }
            }
            Err(error) => {
                warn!("[X11-CAPS] DRI3 device open failed: {error}");
                None
            }
        }
    } else {
        None
    };
    let root_depth = screen.root_depth;
    let root_bpp = setup
        .pixmap_formats
        .iter()
        .find(|format| format.depth == root_depth)
        .map_or(0, |format| format.bits_per_pixel);

    let capabilities = X11Capabilities {
        server_vendor,
        server_release: setup.release_number,
        is_xwayland,
        dri3_version,
        present_version,
        present_capabilities,
        dri3_device_rdev,
        root_depth,
        root_bpp,
    };
    info!(
        "[X11-CAPS] server={} vendor={:?} release={} dri3={:?} present={:?} capabilities=0x{:x} syncobj={} dri3_device_rdev={:?} depth={} bpp={}",
        capabilities.server_kind(),
        capabilities.server_vendor,
        capabilities.server_release,
        capabilities.dri3_version,
        capabilities.present_version,
        capabilities.present_capabilities,
        capabilities.present_syncobj(),
        capabilities.dri3_device_rdev,
        capabilities.root_depth,
        capabilities.root_bpp,
    );
    Ok(capabilities)
}

/// Wrapper for RawWindowHandle for wgpu
pub struct RawX11Surface {
    pub window_id: u32,
    pub connection: Arc<XCBConnection>,
    pub screen: i32,
}

// SAFETY: `RawX11Surface` owns an `Arc<XCBConnection>` and stores immutable window/screen IDs.
// The raw handles returned from it are borrowed only for WGPU surface creation while `self` lives.
unsafe impl Send for RawX11Surface {}
// SAFETY: Shared access does not mutate the XCB connection wrapper or handle IDs.
unsafe impl Sync for RawX11Surface {}

impl HasWindowHandle for RawX11Surface {
    fn window_handle(&self) -> Result<WindowHandle<'_>, HandleError> {
        // We use XCB since x11rb is XCB-based
        let handle = XcbWindowHandle::new(
            std::num::NonZeroU32::new(self.window_id).expect("Window ID is 0"),
        );
        // SAFETY: The raw XCB window ID belongs to this surface and remains valid for `self`.
        Ok(unsafe { WindowHandle::borrow_raw(RawWindowHandle::Xcb(handle)) })
    }
}

impl HasDisplayHandle for RawX11Surface {
    fn display_handle(&self) -> Result<DisplayHandle<'_>, HandleError> {
        let ptr = self.connection.get_raw_xcb_connection();
        let handle = XcbDisplayHandle::new(NonNull::new(ptr as *mut _), self.screen);
        // SAFETY: The XCB display pointer is owned by `self.connection` and remains valid for `self`.
        Ok(unsafe { DisplayHandle::borrow_raw(RawDisplayHandle::Xcb(handle)) })
    }
}
