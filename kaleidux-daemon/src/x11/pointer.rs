//! Uses the X11 Shape input region to keep noninteractive wallpaper areas click-through.

use crate::{main_loop::MainLoopContext, renderer::Renderer, runtime::morph_input};
use std::collections::HashMap;
use x11rb::{
    errors::ConnectionError,
    protocol::{
        Event,
        shape::{ConnectionExt, SK, SO},
        xproto::{ClipOrdering, EventMask, KeyButMask, NotifyMode, Rectangle, Window},
    },
    xcb_ffi::XCBConnection,
};

pub(crate) struct DesktopPointer {
    supported: bool,
    regions: HashMap<Window, Vec<[i32; 4]>>,
    pressed: [Option<(Window, [f32; 2])>; 2],
}

impl DesktopPointer {
    pub(super) fn new(conn: &XCBConnection) -> Self {
        let supported = conn
            .shape_query_version()
            .ok()
            .and_then(|cookie| cookie.reply().ok())
            .is_some_and(|reply| (reply.major_version, reply.minor_version) >= (1, 1));
        Self {
            supported,
            regions: HashMap::new(),
            pressed: Default::default(),
        }
    }

    pub(super) fn event_mask(&self) -> EventMask {
        if self.supported {
            EventMask::BUTTON_PRESS
                | EventMask::BUTTON_RELEASE
                | EventMask::POINTER_MOTION
                | EventMask::ENTER_WINDOW
                | EventMask::LEAVE_WINDOW
        } else {
            EventMask::NO_EVENT
        }
    }

    pub(super) fn empty_input_region(
        &self,
        conn: &XCBConnection,
        window: Window,
    ) -> Result<(), ConnectionError> {
        if self.supported {
            set_region(conn, window, &[])?;
        }
        Ok(())
    }

    pub(super) fn retire(&mut self, window: Window) {
        self.regions.remove(&window);
        for pressed in &mut self.pressed {
            if pressed.is_some_and(|(origin, _)| origin == window) {
                *pressed = None;
            }
        }
    }

    pub(crate) fn sync_regions(
        &mut self,
        conn: &XCBConnection,
        windows: &HashMap<String, Window>,
        renderers: &HashMap<String, Renderer>,
    ) -> Result<bool, ConnectionError> {
        if !self.supported {
            return Ok(false);
        }
        let mut changed = false;
        for (name, renderer) in renderers {
            let regions = renderer.pointer_regions();
            if self.regions.is_empty() && regions.clone().next().is_none() {
                continue;
            }
            let Some(&window) = windows.get(name) else {
                continue;
            };
            let size = (renderer.config.width, renderer.config.height);
            let regions = regions.map(|rect| morph_input::pixel_region(rect, size));
            if regions
                .clone()
                .eq(self.regions.get(&window).into_iter().flatten().copied())
            {
                continue;
            }
            let regions: Vec<_> = regions.collect();
            set_region(conn, window, &regions)?;
            self.retire(window);
            if !regions.is_empty() {
                self.regions.insert(window, regions);
            }
            changed = true;
        }
        Ok(changed)
    }

    pub(crate) fn handle(
        &mut self,
        ctx: &mut MainLoopContext,
        names: &HashMap<Window, String>,
        event: Event,
    ) {
        let (window, x, y, state, button) = match event {
            Event::ButtonPress(event) => (
                event.event,
                event.event_x,
                event.event_y,
                event.state,
                Some((event.detail, true)),
            ),
            Event::ButtonRelease(event) => (
                event.event,
                event.event_x,
                event.event_y,
                event.state,
                Some((event.detail, false)),
            ),
            Event::MotionNotify(event) => {
                (event.event, event.event_x, event.event_y, event.state, None)
            }
            Event::EnterNotify(event) if event.mode == NotifyMode::NORMAL => {
                (event.event, event.event_x, event.event_y, event.state, None)
            }
            Event::LeaveNotify(event) if event.mode == NotifyMode::NORMAL => {
                self.pressed = Default::default();
                (
                    event.event,
                    event.event_x,
                    event.event_y,
                    KeyButMask::default(),
                    None,
                )
            }
            _ => return,
        };
        let Some(output) = names.get(&window) else {
            return;
        };
        let Some(renderer) = ctx.renderers.get(output) else {
            return;
        };
        if !self.regions.contains_key(&window) {
            return;
        }
        let [x, y] = [
            f32::from(x) / renderer.config.width.max(1) as f32,
            f32::from(y) / renderer.config.height.max(1) as f32,
        ];
        let left = button
            .filter(|(which, _)| *which == 1)
            .map_or(state.contains(KeyButMask::BUTTON1), |(_, pressed)| pressed);
        let right = button
            .filter(|(which, _)| *which == 3)
            .map_or(state.contains(KeyButMask::BUTTON3), |(_, pressed)| pressed);
        morph_input::position(
            ctx,
            output,
            [
                x.clamp(0.0, 1.0),
                y.clamp(0.0, 1.0),
                f32::from(u8::from(left)),
                f32::from(u8::from(right)),
            ],
            !matches!(event, Event::LeaveNotify(_)),
        );
        match button {
            Some((key @ (1 | 3), true)) => {
                self.pressed[usize::from(key == 3)] = Some((window, [x, y]))
            }
            Some((key @ (1 | 3), false)) => {
                if let Some((origin, pressed)) = self.pressed[usize::from(key == 3)].take()
                    && origin == window
                {
                    let gesture = if key == 1 {
                        kaleidux_common::MorphGesture::Primary
                    } else {
                        kaleidux_common::MorphGesture::Secondary
                    };
                    morph_input::activate(ctx, output.clone(), pressed, [x, y], gesture);
                }
            }
            Some((key @ (4 | 5), true)) => {
                let gesture = if key == 4 {
                    kaleidux_common::MorphGesture::ScrollUp
                } else {
                    kaleidux_common::MorphGesture::ScrollDown
                };
                morph_input::activate(ctx, output.clone(), [x, y], [x, y], gesture);
            }
            _ => {}
        }
    }
}

fn set_region(
    conn: &XCBConnection,
    window: Window,
    regions: &[[i32; 4]],
) -> Result<(), ConnectionError> {
    let rectangles: Vec<_> = regions
        .iter()
        .map(|&[left, top, width, height]| Rectangle {
            x: left.min(i32::from(i16::MAX)) as i16,
            y: top.min(i32::from(i16::MAX)) as i16,
            width: width.min(i32::from(u16::MAX)) as u16,
            height: height.min(i32::from(u16::MAX)) as u16,
        })
        .collect();
    conn.shape_rectangles(
        SO::SET,
        SK::INPUT,
        ClipOrdering::UNSORTED,
        window,
        0,
        0,
        &rectangles,
    )?;
    Ok(())
}
