//! Draws a small independent heart without holding wallpaper or video buffers.

use super::WaylandBackend;
use crate::main_loop::MainLoopContext;
use crate::renderer::{LOVE_FRAME_INTERVAL, LoveMark};
use anyhow::Result;
use smithay_client_toolkit::{
    shell::{
        WaylandSurface,
        wlr_layer::{Anchor, Layer, LayerSurface},
    },
    shm::slot::{Buffer, SlotPool},
};
use std::time::Instant;
use wayland_client::{
    Proxy, QueueHandle,
    protocol::{wl_output::WlOutput, wl_shm, wl_surface::WlSurface},
};

pub(super) struct LoveSurface {
    surface: LayerSurface,
    buffers: HeartBuffers,
    layout: Layout,
    ready: bool,
    drawn: Option<(bool, Option<Instant>)>,
    due: Option<Instant>,
}

struct HeartBuffers {
    pool: SlotPool,
    slots: [Buffer; 2],
}

impl HeartBuffers {
    fn new(shm: &smithay_client_toolkit::shm::Shm, side: i32) -> Result<Self> {
        let mut pool = SlotPool::new((side * side * 8) as usize, shm)?;
        let (front, _) = pool.create_buffer(side, side, side * 4, wl_shm::Format::Argb8888)?;
        let (back, _) = pool.create_buffer(side, side, side * 4, wl_shm::Format::Argb8888)?;
        Ok(Self {
            pool,
            slots: [front, back],
        })
    }
}

#[derive(Clone, Copy, PartialEq)]
struct Layout {
    side: i32,
    origin: [i32; 2],
    center: [f32; 2],
    pixels: f32,
}

impl Layout {
    fn new((width, height): (u32, u32), position: [f32; 2]) -> Self {
        let width = width.max(1) as i32;
        let height = height.max(1) as i32;
        let short = width.min(height);
        let pixels = short as f32 * 0.065;
        let side = (pixels * 3.25).ceil().clamp(1.0, 256.0) as i32;
        let side = side.min(short);
        let center = [position[0] * width as f32, position[1] * height as f32];
        let origin = [
            (center[0] as i32 - side / 2).clamp(0, width - side),
            (center[1] as i32 - side / 2).clamp(0, height - side),
        ];
        Self {
            side,
            origin,
            center: [center[0] - origin[0] as f32, center[1] - origin[1] as f32],
            pixels: pixels.min(side as f32 / 3.25).max(1.0),
        }
    }

    fn button(self) -> [i32; 4] {
        let reach = (self.pixels * 0.65).max(6.0);
        let [x, y] = self.center;
        let left = (x - reach).floor().max(0.0) as i32;
        let top = (y - reach).floor().max(0.0) as i32;
        let right = ((x + reach).ceil() as i32).min(self.side);
        let bottom = ((y + reach).ceil() as i32).min(self.side);
        [left, top, right - left, bottom - top]
    }
}

impl WaylandBackend {
    pub(super) fn find_love_surface(
        &self,
        surface: &WlSurface,
        point: (f64, f64),
    ) -> Option<(&str, bool)> {
        self.love_surfaces.iter().find_map(|(name, love)| {
            if love.surface.wl_surface() != surface {
                return None;
            }
            let [x, y, width, height] = love.layout.button().map(f64::from);
            let inside =
                point.0 >= x && point.0 < x + width && point.1 >= y && point.1 < y + height;
            Some((name.as_str(), inside))
        })
    }

    pub(super) fn configure_love_surface(&mut self, surface: &LayerSurface) -> bool {
        if let Some(heart) = self
            .love_surfaces
            .values_mut()
            .find(|heart| heart.surface.wl_surface() == surface.wl_surface())
        {
            heart.ready = true;
            true
        } else {
            false
        }
    }

    pub(super) fn retire_closed_love_surface(&mut self, surface: &LayerSurface) -> bool {
        let count = self.love_surfaces.len();
        self.love_surfaces
            .retain(|_, value| value.surface.wl_surface() != surface.wl_surface());
        self.love_surfaces.len() != count
    }

    pub(crate) fn love_deadline(&self) -> Option<Instant> {
        self.love_surfaces
            .values()
            .filter_map(|heart| heart.due)
            .min()
    }

    pub(crate) fn sync_love_surfaces(
        &mut self,
        ctx: &MainLoopContext,
        qh: &QueueHandle<Self>,
        now: Instant,
    ) -> bool {
        let previous_count = self.love_surfaces.len();
        self.love_surfaces.retain(|name, _| {
            ctx.renderers
                .get(name)
                .and_then(crate::renderer::Renderer::love_mark)
                .is_some()
                && !ctx.powered_off_outputs.contains(name)
                && !ctx.display_power_suspended
        });
        let mut changed = previous_count != self.love_surfaces.len();
        if ctx.display_power_suspended {
            return changed;
        }
        for (name, renderer) in &ctx.renderers {
            let Some(mark) = renderer
                .love_mark()
                .filter(|_| !ctx.powered_off_outputs.contains(name))
            else {
                continue;
            };
            let extent =
                self.logical_surface_size(name, (renderer.config.width, renderer.config.height));
            let layout = Layout::new(extent, mark.position);
            if !self.love_surfaces.contains_key(name) {
                let output = self.output_state.outputs().find(|output| {
                    self.output_state
                        .info(output)
                        .and_then(|info| info.name)
                        .as_deref()
                        == Some(name.as_str())
                });
                let Some(output) = output else { continue };
                let layer =
                    ctx.monitor_manager
                        .get_output_config(name)
                        .map_or(Layer::Bottom, |config| match config.layer {
                            crate::orchestration::Layer::Top => Layer::Top,
                            crate::orchestration::Layer::Overlay => Layer::Overlay,
                            _ => Layer::Bottom,
                        });
                match self.create_love_surface(&output, qh, layer, layout) {
                    Ok(surface) => {
                        self.love_surfaces.insert(name.clone(), surface);
                        changed = true;
                    }
                    Err(error) => tracing::warn!("Love surface could not be created: {error:#}"),
                }
                continue;
            }
            let heart = self
                .love_surfaces
                .get_mut(name)
                .expect("heart retained above");
            if heart.layout != layout {
                if heart.layout.side != layout.side {
                    match HeartBuffers::new(&self.shm, layout.side) {
                        Ok(buffers) => heart.buffers = buffers,
                        Err(error) => {
                            tracing::warn!("Love surface resize failed: {error:#}");
                            continue;
                        }
                    }
                    heart.ready = false;
                }
                heart.layout = layout;
                heart.drawn = None;
                heart.due = None;
                set_layout(&self.compositor, qh, &heart.surface, layout);
                changed = true;
            }
            match heart.draw(mark, now) {
                Ok(drawn) => changed |= drawn,
                Err(error) => {
                    tracing::warn!("Love surface update failed: {error:#}");
                    heart.due = Some(now + LOVE_FRAME_INTERVAL);
                }
            }
        }
        changed
    }

    fn create_love_surface(
        &self,
        output: &WlOutput,
        qh: &QueueHandle<Self>,
        layer: Layer,
        layout: Layout,
    ) -> Result<LoveSurface> {
        let buffers = HeartBuffers::new(&self.shm, layout.side)?;
        let surface = self.layer_shell.create_layer_surface(
            qh,
            self.compositor.create_surface(qh),
            layer,
            Some("kaleidux-love"),
            Some(output),
        );
        surface.set_anchor(Anchor::TOP | Anchor::LEFT);
        surface.set_exclusive_zone(-1);
        set_layout(&self.compositor, qh, &surface, layout);
        Ok(LoveSurface {
            surface,
            buffers,
            layout,
            ready: false,
            drawn: None,
            due: None,
        })
    }
}

fn set_layout(
    compositor: &smithay_client_toolkit::compositor::CompositorState,
    qh: &QueueHandle<WaylandBackend>,
    surface: &LayerSurface,
    layout: Layout,
) {
    surface.set_size(layout.side as u32, layout.side as u32);
    surface.set_margin(layout.origin[1], 0, 0, layout.origin[0]);
    let region = compositor.wl_compositor().create_region(qh, ());
    let [x, y, width, height] = layout.button();
    region.add(x, y, width, height);
    surface.wl_surface().set_input_region(Some(&region));
    region.destroy();
    surface.commit();
}

impl LoveSurface {
    fn draw(&mut self, mark: &LoveMark, now: Instant) -> Result<bool> {
        let state = (mark.loved, mark.animation);
        if !self.ready || (self.drawn == Some(state) && self.due.is_none_or(|due| due > now)) {
            return Ok(false);
        }
        let side = self.layout.side;
        let progress = mark.progress(now);
        let HeartBuffers { pool, slots } = &mut self.buffers;
        let Some(buffer) = slots.iter().find(|slot| !slot.slot().has_active_buffers()) else {
            // A compositor may hold both buffers while the output is hidden.
            // Retry on its next event without allocating or polling.
            self.drawn = None;
            self.due = None;
            return Ok(false);
        };
        let pixels = pool.canvas(buffer).expect("inactive heart slot is mapped");
        paint(pixels, self.layout, mark.loved, progress);
        let surface = self.surface.wl_surface();
        buffer.attach_to(surface)?;
        if surface.version() >= 4 {
            surface.damage_buffer(0, 0, side, side);
        } else {
            surface.damage(0, 0, side, side);
        }
        self.surface.commit();
        self.drawn = Some(state);
        self.due = (progress < 1.0).then_some(now + LOVE_FRAME_INTERVAL);
        Ok(true)
    }
}

fn paint(output: &mut [u8], layout: Layout, loved: bool, progress: f32) {
    let growth = 0.56 + 0.44 * (std::f32::consts::PI * progress).sin().max(0.0).powf(0.6);
    let damped = 1.0 + 0.21 * (-7.0 * progress).exp() * (22.0 * progress).sin();
    let factor = 1.0 / (layout.pixels * growth * damped);
    let [cx, cy] = layout.center;
    for (index, pixel) in output.as_chunks_mut::<4>().0.iter_mut().enumerate() {
        let px = ((index % layout.side as usize) as f32 + 0.5 - cx) * factor;
        let py = -((index / layout.side as usize) as f32 + 0.5 - cy) * factor + 0.10;
        let base = px * px + py * py - 0.69;
        let shape = base * base * base - px * px * py * py * py;
        if shape > 0.018 {
            pixel.fill(0);
            continue;
        }
        let edge = ((0.018 - shape) / 0.033).clamp(0.0, 1.0);
        let inside = edge * edge * (3.0 - 2.0 * edge);
        let ridge = (-shape.abs() * 20.0).exp();
        let light = (-((px + 0.32).powi(2) + (py - 0.53).powi(2)) * 12.0).exp();
        let shine = (0.85 * light + 0.55 * ridge).clamp(0.0, 1.0);
        let (base, gleam, alpha) = if loved {
            (
                [0.84, 0.17, 0.43],
                [1.0, 0.84, 0.91],
                inside * (0.88 + 0.1 * shine),
            )
        } else {
            (
                [0.56, 0.66, 0.74],
                [0.99, 1.0, 1.0],
                inside * (0.25 + 0.6 * shine),
            )
        };
        let rgb = std::array::from_fn::<_, 3, _>(|i| {
            ((base[i] + (gleam[i] - base[i]) * shine) * alpha * 255.0).round() as u8
        });
        let packed = u32::from_be_bytes([(alpha * 255.0).round() as u8, rgb[0], rgb[1], rgb[2]]);
        pixel.copy_from_slice(&packed.to_ne_bytes());
    }
}
