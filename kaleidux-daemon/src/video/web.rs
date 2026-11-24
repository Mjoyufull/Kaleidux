//! Uses a pull-based WebKit helper so paused and hidden outputs stop requesting frames.

use super::canvas::FrameStream;
use anyhow::Result;
use std::path::Path;

pub(crate) fn is_web_path(path: &Path) -> bool {
    path.extension().is_some_and(|suffix| {
        suffix.eq_ignore_ascii_case("html") || suffix.eq_ignore_ascii_case("htm")
    }) && !crate::media_source::MediaSource::is_virtual(path)
}

#[cfg(not(feature = "wallpaper-web"))]
pub(super) fn run(
    _: &Path,
    _: u64,
    _: Option<(u32, u32)>,
    _: Option<u32>,
    _: &mut FrameStream,
) -> Result<()> {
    anyhow::bail!("HTML wallpapers require the wallpaper-web build feature")
}

#[cfg(feature = "wallpaper-web")]
pub(super) use helper::run;

#[cfg(feature = "wallpaper-web")]
mod helper {
    use super::*;
    use crate::video::{VideoFrame, VideoFrameFormat, VideoFrameStorage, VideoGeometry};
    use anyhow::Context;
    use std::io::{ErrorKind, Read, Write};
    use std::os::fd::AsRawFd;
    use std::process::{Child, ChildStdout, Command, Stdio};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    struct Browser(Child);

    impl Drop for Browser {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    pub(crate) fn run(
        path: &Path,
        session_id: u64,
        size: Option<(u32, u32)>,
        fps: Option<u32>,
        stream: &mut FrameStream,
    ) -> Result<()> {
        let path = path.canonicalize()?;
        anyhow::ensure!(path.is_file(), "HTML wallpaper must be a regular file");
        let (width, height) = size.unwrap_or((1280, 720));
        let scale = (1920.0 / f64::from(width))
            .min(1080.0 / f64::from(height))
            .min(1.0);
        let width = (f64::from(width) * scale).round().max(1.0) as u32;
        let height = (f64::from(height) * scale).round().max(1.0) as u32;
        let interval = Duration::from_secs_f64(1.0 / f64::from(fps.unwrap_or(30).clamp(1, 120)));
        let mut browser = Browser(
            Command::new("python3")
                .args(["-u", "-c", include_str!("web_helper.py")])
                .arg(path)
                .arg(width.to_string())
                .arg(height.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .context("HTML playback needs Python 3, GTK 3 and WebKitGTK 4.1")?,
        );
        let mut input = browser
            .0
            .stdin
            .take()
            .context("Browser input unavailable")?;
        let mut output = browser
            .0
            .stdout
            .take()
            .context("Browser output unavailable")?;
        let mut position_ns = 0_u64;
        loop {
            let started = Instant::now();
            input.write_all(b"R")?;
            let mut pixels = vec![0; width as usize * height as usize * 4];
            if !read_frame(&mut output, &mut pixels, stream)? {
                return Ok(());
            }
            let frame = VideoFrame {
                storage: VideoFrameStorage::Cpu(Arc::from(pixels)),
                width,
                height,
                stride: width * 4,
                format: VideoFrameFormat::Rgba,
                session_id,
                pts_ns: Some(position_ns),
                duration_ns: Some(interval.as_nanos() as u64),
                color: Default::default(),
                geometry: VideoGeometry::for_dimensions(width, height),
            };
            if !stream.publish(frame) || !stream.wait(interval.saturating_sub(started.elapsed())) {
                return Ok(());
            }
            position_ns = position_ns.saturating_add(interval.as_nanos() as u64);
        }
    }

    fn read_frame(
        output: &mut ChildStdout,
        mut data: &mut [u8],
        stream: &FrameStream,
    ) -> Result<bool> {
        let deadline = Instant::now() + Duration::from_secs(6);
        let mut descriptor = libc::pollfd {
            fd: output.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        while !data.is_empty() && !stream.stopped() {
            anyhow::ensure!(
                Instant::now() < deadline,
                "HTML renderer did not produce a frame"
            );
            // SAFETY: This call only reads and updates the live poll descriptor.
            let ready = unsafe { libc::poll(&mut descriptor, 1, 50) };
            if ready == -1 {
                let error = std::io::Error::last_os_error();
                if error.kind() == ErrorKind::Interrupted {
                    continue;
                }
                return Err(error.into());
            }
            if ready > 0 {
                let read = output.read(data)?;
                anyhow::ensure!(
                    read > 0,
                    "HTML renderer exited. Check Python GI, GTK 3, WebKitGTK 4.1 and the display session"
                );
                data = &mut data[read..];
            }
        }
        Ok(data.is_empty())
    }
}
