use x11rb::connection::Connection;
use x11rb::protocol::xproto::{
    ConnectionExt, CreateWindowAux, Window, WindowClass, EventMask,
    Atom, PropMode,
};
use x11rb::xcb_ffi::XCBConnection;
use x11rb::wrapper::ConnectionExt as _;
use raw_window_handle::{
    HasWindowHandle, HasDisplayHandle, WindowHandle, DisplayHandle,
    RawWindowHandle, RawDisplayHandle,
    XcbWindowHandle, XcbDisplayHandle, HandleError,
};
use std::sync::Arc;
use std::ptr::NonNull;
use tracing::info;
use std::collections::HashMap;

/// X11 Backend handling connection and window management
pub struct X11Backend {
    pub conn: Arc<XCBConnection>,
    pub screen_num: usize,
    pub root: Window,
    pub windows: HashMap<String, Window>,
    pub atoms: Atoms,
    pub cached_monitors: parking_lot::Mutex<Option<Vec<(String, i16, i16, u16, u16)>>>,
    pub monitors_dirty: std::sync::atomic::AtomicBool,
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
        
        // Intern atoms
        let _net_wm_window_type = conn.intern_atom(false, b"_NET_WM_WINDOW_TYPE")?.reply()?.atom;
        let _net_wm_window_type_desktop = conn.intern_atom(false, b"_NET_WM_WINDOW_TYPE_DESKTOP")?.reply()?.atom;
        let _net_wm_state = conn.intern_atom(false, b"_NET_WM_STATE")?.reply()?.atom;
        let _net_wm_state_fullscreen = conn.intern_atom(false, b"_NET_WM_STATE_FULLSCREEN")?.reply()?.atom;
        let _net_wm_state_below = conn.intern_atom(false, b"_NET_WM_STATE_BELOW")?.reply()?.atom;
        let _net_wm_state_sticky = conn.intern_atom(false, b"_NET_WM_STATE_STICKY")?.reply()?.atom;
        let _net_wm_state_skip_taskbar = conn.intern_atom(false, b"_NET_WM_STATE_SKIP_TASKBAR")?.reply()?.atom;
        
        // Subscribe to RandR events
        use x11rb::protocol::randr::{ConnectionExt as RandrExt};
        let _ = conn.randr_select_input(
            root,
            x11rb::protocol::randr::NotifyMask::OUTPUT_CHANGE
                | x11rb::protocol::randr::NotifyMask::CRTC_CHANGE
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
        })
    }
    
    pub fn get_monitors(&self) -> anyhow::Result<Vec<(String, i16, i16, u16, u16)>> {
        use x11rb::protocol::randr::{ConnectionExt as RandrExt};
        
        // Fast path: return cache if not dirty
        if !self.monitors_dirty.load(std::sync::atomic::Ordering::SeqCst) {
            if let Some(monitors) = self.cached_monitors.lock().as_ref() {
                return Ok(monitors.clone());
            }
        }

        let screen_res = self.conn.randr_get_screen_resources_current(self.root)?.reply()?;
        let mut monitors = Vec::new();
        
        for &crtc in &screen_res.crtcs {
            let crtc_info = self.conn.randr_get_crtc_info(crtc, screen_res.config_timestamp)?.reply()?;
            
            if crtc_info.mode == 0 { continue; } // Inactive CRTC
            
            // Find output name connected to this CRTC
            let mut name = format!("X11-{}", crtc); // Fallback
            if let Some(&output) = crtc_info.outputs.first() {
                 let output_info = self.conn.randr_get_output_info(output, screen_res.config_timestamp)?.reply()?;
                 name = String::from_utf8_lossy(&output_info.name).to_string();
            }
            
            monitors.push((
                name,
                crtc_info.x,
                crtc_info.y,
                crtc_info.width,
                crtc_info.height
            ));
        }
        
        // Fallback if no RandR monitors found (rare/failsafe)
        if monitors.is_empty() {
             let screen = &self.conn.setup().roots[self.screen_num];
             monitors.push((
                 "X11-0".to_string(),
                 0, 0,
                 screen.width_in_pixels,
                 screen.height_in_pixels
             ));
        }
        
        // Update cache
        {
            let mut cache = self.cached_monitors.lock();
            *cache = Some(monitors.clone());
            self.monitors_dirty.store(false, std::sync::atomic::Ordering::SeqCst);
        }

        Ok(monitors)
    }

    pub fn create_wallpaper_window(&mut self, name: &str, x: i16, y: i16, width: u16, height: u16) -> anyhow::Result<Window> {
        let win_id = self.conn.generate_id()?;
        let screen = &self.conn.setup().roots[self.screen_num];
        
        // Setup window attributes
        // REMOVED override_redirect(1) to let WM handle stacking (keeping it below apps)
        let win_aux = CreateWindowAux::new()
            .event_mask(EventMask::EXPOSURE | EventMask::STRUCTURE_NOTIFY)
            .background_pixel(screen.white_pixel);
            
        self.conn.create_window(
            x11rb::COPY_DEPTH_FROM_PARENT,
            win_id,
            self.root,
            x, y, width, height,
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
        
        // Lower window to the bottom of the stack
        use x11rb::protocol::xproto::StackMode;
        self.conn.configure_window(win_id, &x11rb::protocol::xproto::ConfigureWindowAux::new().stack_mode(StackMode::BELOW))?;
        
        self.conn.flush()?;
        // Wait for server to process all requests (Audit Point 11)
        let _ = self.conn.sync()?;
        
        self.windows.insert(name.to_string(), win_id);
        
        info!("Created X11 wallpaper window for {}: id={}, rect={}x{}@{},{}", name, win_id, width, height, x, y);
        
        Ok(win_id)
    }
}

/// Wrapper for RawWindowHandle for wgpu
pub struct RawX11Surface {
    pub window_id: u32,
    pub connection: Arc<XCBConnection>, 
    pub screen: i32,
}

unsafe impl Send for RawX11Surface {}
unsafe impl Sync for RawX11Surface {}

impl HasWindowHandle for RawX11Surface {
    fn window_handle(&self) -> Result<WindowHandle<'_>, HandleError> {
        // We use XCB since x11rb is XCB-based
        let handle = XcbWindowHandle::new(
            std::num::NonZeroU32::new(self.window_id).expect("Window ID is 0"),
        );
        Ok(unsafe { WindowHandle::borrow_raw(RawWindowHandle::Xcb(handle)) })
    }
}

impl HasDisplayHandle for RawX11Surface {
    fn display_handle(&self) -> Result<DisplayHandle<'_>, HandleError> {
        let ptr = self.connection.get_raw_xcb_connection();
        let handle = XcbDisplayHandle::new(
            NonNull::new(ptr as *mut _),
            self.screen,
        );
        Ok(unsafe { DisplayHandle::borrow_raw(RawDisplayHandle::Xcb(handle)) })
    }
}
