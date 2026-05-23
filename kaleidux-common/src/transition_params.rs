use super::Transition;

impl Transition {
    pub fn to_params(&self) -> [f32; 28] {
        let mut p = [0.0; 28];
        match self {
            Transition::BookFlip
            | Transition::Burn
            | Transition::CannabisLeaf
            | Transition::Circle
            | Transition::ColorPhase
            | Transition::CoordFromIn
            | Transition::CrossHatch
            | Transition::CrossWarp
            | Transition::Displacement
            | Transition::Dreamy
            | Transition::Fade
            | Transition::GlitchDisplace
            | Transition::GlitchMemories
            | Transition::Heart
            | Transition::HorizontalClose
            | Transition::HorizontalOpen
            | Transition::InvertedPageCurl
            | Transition::LeftRight
            | Transition::Luma
            | Transition::MultiplyBlend
            | Transition::Overexposure
            | Transition::RandomNoiseX
            | Transition::Rotate
            | Transition::ScaleIn
            | Transition::Swirl
            | Transition::TangentMotionBlur
            | Transition::TopBottom
            | Transition::VerticalClose
            | Transition::VerticalOpen
            | Transition::WipeDown
            | Transition::WipeLeft
            | Transition::WipeRight
            | Transition::WipeUp
            | Transition::WindowBlinds
            | Transition::XAxisTranslation
            | Transition::ZoomInCircles
            | Transition::Random => {}
            Transition::Angular { starting_angle } => {
                p[0] = *starting_angle;
            }
            Transition::BowTieWithParameter { adjust, reverse } => {
                p[0] = *adjust;
                p[1] = if *reverse { 1.0 } else { 0.0 };
            }
            Transition::BowTieHorizontal | Transition::BowTieVertical => {}
            Transition::Bounce {
                shadow_colour,
                shadow_height,
                bounces,
            } => {
                p[0..4].copy_from_slice(shadow_colour);
                p[4] = *shadow_height;
                p[5] = *bounces;
            }
            Transition::ButterflyWaveScrawler {
                amplitude,
                waves,
                color_separation,
            } => {
                p[0] = *amplitude;
                p[1] = *waves;
                p[2] = *color_separation;
            }
            Transition::CircleCrop { bgcolor } => {
                p[0..4].copy_from_slice(bgcolor);
            }
            Transition::CircleOpen {
                smoothness,
                opening,
            } => {
                p[0] = *smoothness;
                p[1] = if *opening { 1.0 } else { 0.0 };
            }
            Transition::CrazyParametricFun {
                a,
                b,
                amplitude,
                smoothness,
            } => {
                p[0] = *a;
                p[1] = *b;
                p[2] = *amplitude;
                p[3] = *smoothness;
            }
            Transition::ColourDistance { power } => {
                p[0] = *power;
            }
            Transition::CrossZoom { strength } => {
                p[0] = *strength;
            }
            Transition::Cube {
                persp,
                unzoom,
                reflection,
                floating,
            } => {
                p[0] = *persp;
                p[1] = *unzoom;
                p[2] = *reflection;
                p[3] = *floating;
            }
            Transition::Directional { direction } => {
                p[0..2].copy_from_slice(direction);
            }
            Transition::DirectionalEasing { direction } => {
                p[0..2].copy_from_slice(direction);
            }
            Transition::DirectionalScaled { direction, scale } => {
                p[0..2].copy_from_slice(direction);
                p[2] = *scale;
            }
            Transition::DirectionalWarp { direction } => {
                p[0..2].copy_from_slice(direction);
            }
            Transition::DirectionalWipe {
                direction,
                smoothness,
            } => {
                p[0..2].copy_from_slice(direction);
                p[2] = *smoothness;
            }
            Transition::Dissolve {
                line_width,
                spread_clr,
                hot_clr,
                pow,
                intensity,
            } => {
                p[0] = *line_width;
                p[1..4].copy_from_slice(spread_clr);
                p[4..7].copy_from_slice(hot_clr);
                p[7] = *pow;
                p[8] = *intensity;
            }
            Transition::Doom {
                bars,
                amplitude,
                noise,
                frequency,
                drip_scale,
            } => {
                p[0] = *bars as f32;
                p[1] = *amplitude;
                p[2] = *noise;
                p[3] = *frequency;
                p[4] = *drip_scale;
            }
            Transition::Doorway {
                reflection,
                perspective,
                depth,
            } => {
                p[0] = *reflection;
                p[1] = *perspective;
                p[2] = *depth;
            }
            Transition::DreamyZoom { rotation, scale } => {
                p[0] = *rotation;
                p[1] = *scale;
            }
            Transition::Edge {
                thickness,
                brightness,
            } => {
                p[0] = *thickness;
                p[1] = *brightness;
            }
            Transition::FadeColor { color, color_phase } => {
                p[0..3].copy_from_slice(color);
                p[3] = *color_phase;
            }
            Transition::FadeGrayscale { intensity } => {
                p[0] = *intensity;
            }
            Transition::FlyEye {
                size,
                zoom,
                color_separation,
            } => {
                p[0] = *size;
                p[1] = *zoom;
                p[2] = *color_separation;
            }
            Transition::GridFlip {
                size,
                pause,
                divider_width,
                bgcolor,
                randomness,
            } => {
                p[0] = size[0] as f32;
                p[1] = size[1] as f32;
                p[2] = *pause;
                p[3] = *divider_width;
                p[4..8].copy_from_slice(bgcolor);
                p[8] = *randomness;
            }
            Transition::Hexagonalize {
                steps,
                horizontal_hexagons,
            } => {
                p[0] = *steps as f32;
                p[1] = *horizontal_hexagons;
            }
            Transition::Kaleidoscope {
                speed,
                angle,
                power,
            } => {
                p[0] = *speed;
                p[1] = *angle;
                p[2] = *power;
            }
            Transition::LinearBlur { intensity } => {
                p[0] = *intensity;
            }
            Transition::LuminanceMelt {
                direction,
                luma_threshold,
            } => {
                p[0] = if *direction { 1.0 } else { 0.0 };
                p[1] = *luma_threshold;
            }
            Transition::Morph { strength } => {
                p[0] = *strength;
            }
            Transition::Mosaic { endx, endy } => {
                p[0] = *endx as f32;
                p[1] = *endy as f32;
            }
            Transition::MosaicTransition { mosaic_num } => {
                p[0] = *mosaic_num;
            }
            Transition::Perlin {
                scale,
                smoothness,
                seed,
            } => {
                p[0] = *scale;
                p[1] = *smoothness;
                p[2] = *seed;
            }
            Transition::Pinwheel { speed } => {
                p[0] = *speed;
            }
            Transition::Pixelize { squares_min, steps } => {
                p[0] = squares_min[0] as f32;
                p[1] = squares_min[1] as f32;
                p[2] = *steps as f32;
            }
            Transition::PolarFunction { segments } => {
                p[0] = *segments as f32;
            }
            Transition::PolkaDotsCurtain { dots, center } => {
                p[0] = *dots;
                p[1..3].copy_from_slice(center);
            }
            Transition::PowerKaleido {
                scale,
                radius,
                angle,
            } => {
                p[0] = *scale;
                p[1] = *radius;
                p[2] = *angle;
            }
            Transition::Radial { smoothness } => {
                p[0] = *smoothness;
            }
            Transition::RandomSquares { size, smoothness } => {
                p[0] = size[0] as f32;
                p[1] = size[1] as f32;
                p[2] = *smoothness;
            }
            Transition::Rectangle { bgcolor } => {
                p[0..4].copy_from_slice(bgcolor);
            }
            Transition::RectangleCrop { bgcolor } => {
                p[0..4].copy_from_slice(bgcolor);
            }
            Transition::Ripple { amplitude, speed } => {
                p[0] = *amplitude;
                p[1] = *speed;
            }
            Transition::Rolls {
                rolls_type,
                rot_down,
            } => {
                p[0] = *rolls_type as f32;
                p[1] = if *rot_down { 1.0 } else { 0.0 };
            }
            Transition::RotateScaleFade {
                center,
                rotations,
                scale,
                back_color,
            } => {
                p[0..2].copy_from_slice(center);
                p[2] = *rotations;
                p[3] = *scale;
                p[4..8].copy_from_slice(back_color);
            }
            Transition::RotateScaleVanish {
                fade_in_second,
                reverse_effect,
                reverse_rotation,
            } => {
                p[0] = if *fade_in_second { 1.0 } else { 0.0 };
                p[1] = if *reverse_effect { 1.0 } else { 0.0 };
                p[2] = if *reverse_rotation { 1.0 } else { 0.0 };
            }
            Transition::SimpleZoom { zoom_quickness } => {
                p[0] = *zoom_quickness;
            }
            Transition::SimpleZoomOut {
                zoom_quickness,
                fade_edge,
            } => {
                p[0] = *zoom_quickness;
                p[1] = if *fade_edge { 1.0 } else { 0.0 };
            }
            Transition::Slides {
                slides_type,
                slides_in,
            } => {
                p[0] = *slides_type as f32;
                p[1] = if *slides_in { 1.0 } else { 0.0 };
            }
            Transition::SquaresWire {
                squares,
                direction,
                smoothness,
            } => {
                p[0] = squares[0] as f32;
                p[1] = squares[1] as f32;
                p[2] = direction[0];
                p[3] = direction[1];
                p[4] = *smoothness;
            }
            Transition::Squeeze { color_separation } => {
                p[0] = *color_separation;
            }
            Transition::StaticFade {
                n_noise_pixels,
                static_luminosity,
            } => {
                p[0] = *n_noise_pixels;
                p[1] = *static_luminosity;
            }
            Transition::StaticWipe {
                up_to_down,
                max_static_span,
            } => {
                p[0] = if *up_to_down { 1.0 } else { 0.0 };
                p[1] = *max_static_span;
            }
            Transition::StereoViewer {
                zoom,
                corner_radius,
            } => {
                p[0] = *zoom;
                p[1] = *corner_radius;
            }
            Transition::Swap {
                reflection,
                perspective,
                depth,
            } => {
                p[0] = *reflection;
                p[1] = *perspective;
                p[2] = *depth;
            }
            Transition::TvStatic { offset } => {
                p[0] = *offset;
            }
            Transition::UndulatingBurnOut {
                smoothness,
                center,
                color,
            } => {
                p[0] = *smoothness;
                p[1..3].copy_from_slice(center);
                p[3..6].copy_from_slice(color);
            }
            Transition::WaterDrop { amplitude, speed } => {
                p[0] = *amplitude;
                p[1] = *speed;
            }
            Transition::Wind { size } => {
                p[0] = *size;
            }
            Transition::Custom { .. } => {
                // Named parameters handled via #define in daemon for Custom
            }
            Transition::WindowSlice { count, smoothness } => {
                p[0] = *count;
                p[1] = *smoothness;
            }
            Transition::ZoomLeftWipe { zoom_quickness }
            | Transition::ZoomRightWipe { zoom_quickness } => {
                p[0] = *zoom_quickness;
            }
            _ => {}
        }
        p
    }
}
