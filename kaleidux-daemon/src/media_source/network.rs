//! Resolves hosted video URLs through an installed yt-dlp process off the display loop.
//! Output, runtime and retries are bounded; no media download or site API is implemented here.

use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const USER_AGENT: &str = "Mozilla/5.0";

struct ResolverProcess(std::process::Child);

impl Drop for ResolverProcess {
    fn drop(&mut self) {
        // SAFETY: process_group(0) created a group owned by this child. Killing
        // that group also closes pipes inherited by yt-dlp helper processes.
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.wait();
    }
}

pub fn resolve(uri: &str, should_abort: impl Fn() -> bool) -> anyhow::Result<String> {
    let host = uri
        .split_once("://")
        .map(|(_, rest)| rest.split('/').next().unwrap_or(""));
    if !matches!(
        host,
        Some("youtube.com" | "www.youtube.com" | "m.youtube.com" | "youtu.be" | "www.youtu.be")
    ) {
        return Ok(uri.to_owned());
    }
    let format = std::env::var("KLD_YTDLP_FORMAT")
        .unwrap_or_else(|_| "best[height<=2160]/bestvideo[height<=2160]".to_owned());
    let mut command = Command::new("yt-dlp");
    command.args([
        "--ignore-config",
        "--no-playlist",
        "--skip-download",
        "--get-url",
        "--socket-timeout",
        "10",
        "--retries",
        "1",
        "--extractor-retries",
        "1",
        "--user-agent",
        USER_AGENT,
        "--format",
        &format,
    ]);
    if let Ok(browser) = std::env::var("KLD_YTDLP_COOKIES_FROM_BROWSER") {
        command.args(["--cookies-from-browser", &browser]);
    }
    let mut child = ResolverProcess(
        command
            .arg("--")
            .arg(uri)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .map_err(|_| anyhow::anyhow!("YouTube playback requires yt-dlp on PATH"))?,
    );
    let mut stdout = child.0.stdout.take().expect("piped resolver stdout");
    // SAFETY: stdout owns this live pipe descriptor; nonblocking reads allow
    // cancellation even when a resolver helper keeps the write end open.
    unsafe {
        let flags = libc::fcntl(stdout.as_raw_fd(), libc::F_GETFL);
        anyhow::ensure!(
            flags >= 0
                && libc::fcntl(stdout.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) >= 0,
            "Could not configure stream resolver pipe"
        );
    }
    let mut bytes = Vec::new();
    let started = Instant::now();
    let status = loop {
        if should_abort() || started.elapsed() >= Duration::from_secs(30) {
            anyhow::bail!("Stream resolution cancelled or timed out");
        }
        read_available(&mut stdout, &mut bytes)?;
        if let Some(status) = child.0.try_wait()? {
            read_available(&mut stdout, &mut bytes)?;
            break status;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    anyhow::ensure!(
        status.success() && bytes.len() <= 64 * 1024,
        "yt-dlp could not resolve this stream; check format, authentication or yt-dlp version"
    );
    let text = String::from_utf8(bytes)?;
    let mut lines = text.lines().filter(|line| !line.is_empty());
    let resolved = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("yt-dlp returned no stream"))?;
    anyhow::ensure!(
        lines.next().is_none(),
        "Choose a single combined or video-only yt-dlp format"
    );
    anyhow::ensure!(
        matches!(
            super::MediaSource::parse(resolved)?,
            super::MediaSource::Network(_)
        ),
        "Resolver did not return a media URL"
    );
    Ok(resolved.to_owned())
}

fn read_available(stdout: &mut impl Read, bytes: &mut Vec<u8>) -> anyhow::Result<()> {
    let mut chunk = [0; 4096];
    loop {
        match stdout.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => {
                bytes.extend_from_slice(&chunk[..count]);
                anyhow::ensure!(
                    bytes.len() <= 64 * 1024,
                    "Stream resolver output exceeded its limit"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
