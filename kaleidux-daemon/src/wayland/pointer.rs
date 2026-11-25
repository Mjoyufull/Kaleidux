//! Delivers compositor pointer events inside the configured input regions.

use super::WaylandBackend;
use crate::runtime::morph_input;
use crate::{main_loop::MainLoopContext, renderer::Renderer};
use kaleidux_common::MorphGesture;
use smithay_client_toolkit::seat::{
    Capability, SeatHandler, SeatState,
    pointer::{BTN_LEFT, BTN_RIGHT, PointerEvent, PointerEventKind, PointerHandler},
};
use smithay_client_toolkit::shell::WaylandSurface;
use std::collections::HashMap;
use wayland_client::{
    Connection, Proxy, QueueHandle,
    protocol::{wl_pointer::WlPointer, wl_seat::WlSeat},
};

type Region = [i32; 4];

#[derive(Default)]
pub(super) struct DesktopPointer {
    seats: HashMap<WlSeat, SeatPointer>,
    regions: HashMap<String, Vec<Region>>,
    positions: HashMap<String, [f32; 4]>,
    activations: Vec<Activation>,
    love_activations: Vec<String>,
}

struct SeatPointer {
    pointer: WlPointer,
    buttons: [bool; 2],
    pressed: [Option<(String, [f32; 2])>; 2],
    love_pressed: Option<String>,
}

struct Activation {
    output: String,
    pressed: [f32; 2],
    released: [f32; 2],
    gesture: MorphGesture,
}

impl DesktopPointer {
    pub(super) fn retire(&mut self, name: &str) {
        self.regions.remove(name);
        self.positions.remove(name);
        self.activations
            .retain(|activation| activation.output != name);
        self.love_activations.retain(|output| output != name);
        for seat in self.seats.values_mut() {
            for pressed in &mut seat.pressed {
                if pressed.as_ref().is_some_and(|(output, _)| output == name) {
                    *pressed = None;
                }
            }
            if seat.love_pressed.as_deref() == Some(name) {
                seat.love_pressed = None;
            }
        }
    }
}

impl WaylandBackend {
    pub(crate) fn sync_morph_input_regions(
        &mut self,
        renderers: &HashMap<String, Renderer>,
        qh: &QueueHandle<Self>,
    ) -> bool {
        let mut changed = false;
        for (name, renderer) in renderers {
            let regions = renderer.pointer_regions();
            if self.pointer.regions.is_empty() && regions.clone().next().is_none() {
                continue;
            }
            let Some(surface) = self.surfaces.get(name) else {
                continue;
            };
            let size =
                self.logical_surface_size(name, (renderer.config.width, renderer.config.height));
            let regions = regions.map(|rect| morph_input::pixel_region(rect, size));
            let previous = self.pointer.regions.get(name);
            if regions.clone().eq(previous.into_iter().flatten().copied()) {
                continue;
            }
            let regions: Vec<_> = regions.collect();
            let region = self.compositor.wl_compositor().create_region(qh, ());
            for &[x, y, width, height] in &regions {
                region.add(x, y, width, height);
            }
            surface.wl_surface().set_input_region(Some(&region));
            region.destroy();
            surface.commit();
            self.pointer.retire(name);
            if !regions.is_empty() {
                self.pointer.regions.insert(name.clone(), regions);
            }
            changed = true;
        }
        changed
    }

    pub(crate) fn dispatch_morph_pointer(&mut self, ctx: &mut MainLoopContext) {
        for output in self.pointer.love_activations.drain(..) {
            if let Err(error) = crate::runtime::love_button::toggle(ctx, &output) {
                tracing::warn!("Love button failed: {error:#}");
            }
        }
        for (name, position) in self.pointer.positions.drain() {
            morph_input::position(ctx, &name, position);
        }
        for activation in self.pointer.activations.drain(..) {
            morph_input::activate(
                ctx,
                activation.output,
                activation.pressed,
                activation.released,
                activation.gesture,
            );
        }
    }
}

impl SeatHandler for WaylandBackend {
    fn seat_state(&mut self) -> &mut SeatState {
        &mut self.seat_state
    }

    fn new_seat(&mut self, _: &Connection, _: &QueueHandle<Self>, _: WlSeat) {}

    fn new_capability(
        &mut self,
        _: &Connection,
        qh: &QueueHandle<Self>,
        seat: WlSeat,
        capability: Capability,
    ) {
        if capability == Capability::Pointer
            && !self.pointer.seats.contains_key(&seat)
            && let Ok(pointer) = self.seat_state.get_pointer(qh, &seat)
        {
            self.pointer.seats.insert(
                seat,
                SeatPointer {
                    pointer,
                    buttons: [false; 2],
                    pressed: Default::default(),
                    love_pressed: None,
                },
            );
        }
    }

    fn remove_capability(
        &mut self,
        conn: &Connection,
        qh: &QueueHandle<Self>,
        seat: WlSeat,
        capability: Capability,
    ) {
        if capability == Capability::Pointer {
            self.remove_seat(conn, qh, seat);
        }
    }

    fn remove_seat(&mut self, _: &Connection, _: &QueueHandle<Self>, seat: WlSeat) {
        if let Some(seat) = self.pointer.seats.remove(&seat)
            && seat.pointer.version() >= 3
        {
            seat.pointer.release();
        }
    }
}

impl PointerHandler for WaylandBackend {
    fn pointer_frame(
        &mut self,
        _: &Connection,
        _: &QueueHandle<Self>,
        pointer: &WlPointer,
        events: &[PointerEvent],
    ) {
        for event in events {
            if let Some((name, inside)) = self.find_love_surface(&event.surface, event.position) {
                let name = name.to_owned();
                if let Some(seat) = self
                    .pointer
                    .seats
                    .values_mut()
                    .find(|seat| seat.pointer == *pointer)
                {
                    match event.kind {
                        PointerEventKind::Press {
                            button: BTN_LEFT, ..
                        } if inside => seat.love_pressed = Some(name),
                        PointerEventKind::Release {
                            button: BTN_LEFT, ..
                        } => {
                            if seat.love_pressed.take().as_deref() == Some(&name)
                                && inside
                                && self.pointer.love_activations.len() < 64
                            {
                                self.pointer.love_activations.push(name);
                            }
                        }
                        PointerEventKind::Leave { .. } => seat.love_pressed = None,
                        _ => {}
                    }
                }
                continue;
            }
            let Some(output) = self.find_renderer_surface_name(&event.surface) else {
                continue;
            };
            let (width, height) = self.logical_surface_size(&output, (0, 0));
            if width == 0 || height == 0 {
                continue;
            }
            let position = [
                event.position.0 as f32 / width as f32,
                event.position.1 as f32 / height as f32,
            ];
            let Some(seat) = self
                .pointer
                .seats
                .values_mut()
                .find(|seat| seat.pointer == *pointer)
            else {
                continue;
            };
            match event.kind {
                PointerEventKind::Press { button, .. }
                    if matches!(button, BTN_LEFT | BTN_RIGHT) =>
                {
                    let index = usize::from(button == BTN_RIGHT);
                    seat.buttons[index] = true;
                    seat.pressed[index] = Some((output.clone(), position));
                }
                PointerEventKind::Release { button, .. }
                    if matches!(button, BTN_LEFT | BTN_RIGHT) =>
                {
                    let index = usize::from(button == BTN_RIGHT);
                    seat.buttons[index] = false;
                    if let Some((origin, pressed)) = seat.pressed[index].take()
                        && origin == output
                        && self.pointer.activations.len() < 64
                    {
                        self.pointer.activations.push(Activation {
                            output: origin,
                            pressed,
                            released: position,
                            gesture: if index == 0 {
                                MorphGesture::Primary
                            } else {
                                MorphGesture::Secondary
                            },
                        });
                    }
                }
                PointerEventKind::Axis { vertical, .. } => {
                    let movement = if vertical.value120 != 0 {
                        f64::from(vertical.value120)
                    } else if vertical.discrete != 0 {
                        f64::from(vertical.discrete)
                    } else {
                        vertical.absolute
                    };
                    if movement != 0.0 && self.pointer.activations.len() < 64 {
                        self.pointer.activations.push(Activation {
                            output: output.clone(),
                            pressed: position,
                            released: position,
                            gesture: if movement < 0.0 {
                                MorphGesture::ScrollUp
                            } else {
                                MorphGesture::ScrollDown
                            },
                        });
                    }
                }
                PointerEventKind::Leave { .. } => {
                    seat.buttons = [false; 2];
                    seat.pressed = Default::default();
                    seat.love_pressed = None;
                }
                PointerEventKind::Enter { .. } | PointerEventKind::Motion { .. } => {}
                _ => continue,
            }
            self.pointer.positions.insert(
                output,
                [
                    position[0].clamp(0.0, 1.0),
                    position[1].clamp(0.0, 1.0),
                    f32::from(u8::from(seat.buttons[0])),
                    f32::from(u8::from(seat.buttons[1])),
                ],
            );
        }
    }
}

smithay_client_toolkit::delegate_seat!(WaylandBackend);
smithay_client_toolkit::delegate_pointer!(WaylandBackend);
