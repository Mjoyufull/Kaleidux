//! Checks a user-selected transition source before admitting a wallpaper switch.

use super::ShaderManager;
use anyhow::{Context, Result, ensure};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;

const MAX_SHADER_BYTES: u64 = 64 * 1024;

impl ShaderManager {
    pub(crate) fn prepare_custom_transition(name: &str) -> Result<String> {
        ensure!(
            !name.is_empty()
                && name.len() <= 64
                && name
                    .bytes()
                    .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') }),
            "Use a custom shader basename with letters, digits, hyphens or underscores"
        );
        let path = dirs::config_dir()
            .context("Config directory is unavailable")?
            .join("kaleidux")
            .join("shaders")
            .join(format!("{name}.glsl"));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .context("Could not open custom transition shader")?;
        ensure!(
            file.metadata()?.is_file(),
            "Shader must be a regular GLSL file"
        );
        let mut glsl = String::new();
        file.take(MAX_SHADER_BYTES + 1).read_to_string(&mut glsl)?;
        ensure!(
            glsl.len() as u64 <= MAX_SHADER_BYTES,
            "Shader exceeds 64 KiB"
        );
        let wgsl = Self::compile_glsl(name, &glsl, "")?;
        validate_interface(&wgsl)?;
        Ok(wgsl)
    }
}

fn validate_interface(wgsl: &str) -> Result<()> {
    let module = naga::front::wgsl::parse_str(wgsl)?;
    ensure!(
        module
            .global_variables
            .iter()
            .filter_map(|(_, variable)| variable.binding.as_ref())
            .all(|binding| binding.group == 0 && binding.binding < 4),
        "Custom transitions use only the host's existing bindings"
    );
    naga::valid::Validator::new(
        naga::valid::ValidationFlags::all(),
        naga::valid::Capabilities::empty(),
    )
    .validate(&module)?;
    let [entry] = module.entry_points.as_slice() else {
        anyhow::bail!("Expected one custom transition entry point");
    };
    let [uv] = entry.function.arguments.as_slice() else {
        anyhow::bail!("Custom transitions receive only the host UV input");
    };
    let output = entry
        .function
        .result
        .as_ref()
        .context("Missing shader output")?;
    ensure!(
        is_location_zero_vector(&module, uv.ty, uv.binding.as_ref(), naga::VectorSize::Bi)
            && is_location_zero_vector(
                &module,
                output.ty,
                output.binding.as_ref(),
                naga::VectorSize::Quad
            ),
        "Custom transitions must receive UV and return RGBA"
    );
    Ok(())
}

fn is_location_zero_vector(
    module: &naga::Module,
    ty: naga::Handle<naga::Type>,
    binding: Option<&naga::Binding>,
    size: naga::VectorSize,
) -> bool {
    match (&module.types[ty].inner, binding) {
        (
            naga::TypeInner::Vector {
                size: actual,
                scalar,
            },
            Some(naga::Binding::Location {
                location: 0,
                second_blend_source: false,
                ..
            }),
        ) => *actual == size && *scalar == naga::Scalar::F32,
        (naga::TypeInner::Struct { members, .. }, None) => {
            let [member] = members.as_slice() else {
                return false;
            };
            is_location_zero_vector(module, member.ty, member.binding.as_ref(), size)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_source_matches_the_compositor_interface() {
        let style =
            "vec4 transition(vec2 uv) { return mix(getFromColor(uv), getToColor(uv), progress); }";
        let wgsl = ShaderManager::compile_glsl("test", style, "").unwrap();
        assert!(validate_interface(&wgsl).is_ok());
    }
}
