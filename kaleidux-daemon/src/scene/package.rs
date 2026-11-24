//! Indexes PKGV archives by byte range. Package entry layout follows the format
//! documented and inspected in Anima's MIT-licensed PKGV reader.

use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path};

pub(super) struct Package {
    file: BufReader<File>,
    index: HashMap<String, (u64, usize)>,
    data_offset: u64,
}

impl Package {
    pub(super) fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)?;
        let meta = file.metadata()?;
        anyhow::ensure!(meta.is_file(), "Package must be a regular file");
        let mut file = BufReader::new(file);
        let version = read_string(&mut file, 64)?;
        anyhow::ensure!(version.starts_with("PKGV"), "Unsupported package header");
        let entries = read_u32(&mut file)?;
        anyhow::ensure!(entries <= 65_536, "Package index contains too many entries");
        let mut index = HashMap::new();
        for _ in 0..entries {
            let name = safe_name(&read_string(&mut file, 4096)?)?;
            let offset = read_u32(&mut file)?;
            let length = read_u32(&mut file)?;
            anyhow::ensure!(
                file.stream_position()? <= 16 * 1024 * 1024,
                "Package index is too large"
            );
            anyhow::ensure!(
                index
                    .insert(name, (u64::from(offset), length as usize))
                    .is_none(),
                "Duplicate package entry"
            );
        }
        let data_offset = file.stream_position()?;
        for &(offset, length) in index.values() {
            anyhow::ensure!(
                data_offset + offset + length as u64 <= meta.len(),
                "Package entry extends past end of file"
            );
        }
        Ok(Self {
            file,
            index,
            data_offset,
        })
    }

    pub(super) fn has(&self, entry: &str) -> bool {
        safe_name(entry).is_ok_and(|name| self.index.contains_key(&name))
    }

    pub(super) fn read(&mut self, entry: &str, limit: usize) -> Result<Vec<u8>> {
        let name = safe_name(entry)?;
        let &(offset, length) = self
            .index
            .get(&name)
            .with_context(|| format!("Missing package entry {entry:?}"))?;
        anyhow::ensure!(length <= limit, "Package entry exceeds its size limit");
        self.file.seek(SeekFrom::Start(self.data_offset + offset))?;
        let mut bytes = vec![0; length];
        self.file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub(super) fn json(&mut self, entry: &str) -> Result<Value> {
        Ok(serde_json::from_slice(
            &self.read(entry, super::MAX_SCENE_BYTES as usize)?,
        )?)
    }
}

fn safe_name(input: &str) -> Result<String> {
    let name = input.replace('\\', "/").to_ascii_lowercase();
    anyhow::ensure!(
        !name.is_empty()
            && Path::new(&name)
                .components()
                .all(|part| matches!(part, Component::Normal(_) | Component::CurDir)),
        "Invalid package entry path"
    );
    Ok(name.trim_start_matches("./").to_owned())
}

pub(super) fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut field = [0; 4];
    reader.read_exact(&mut field)?;
    Ok(u32::from_le_bytes(field))
}

fn read_string(reader: &mut impl Read, max: u32) -> Result<String> {
    let size = read_u32(reader)?;
    anyhow::ensure!(size <= max, "Package string exceeds its size limit");
    let mut bytes = vec![0; size as usize];
    reader.read_exact(&mut bytes)?;
    Ok(String::from_utf8(bytes)?.trim_end_matches('\0').to_owned())
}
