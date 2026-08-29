use ffmpeg_next::ffi;
use std::ffi::CStr;
use std::ptr;
use std::sync::{Mutex, OnceLock};
use std::{ffi::CString, path::PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativePathTier {
    SurfaceZeroCopy,
    SingleGpuCopy,
    HardwareTransfer,
    SoftwareDecode,
}

impl NativePathTier {
    pub fn label(self) -> &'static str {
        match self {
            Self::SurfaceZeroCopy => "surface-zero-copy",
            Self::SingleGpuCopy => "single-gpu-copy",
            Self::HardwareTransfer => "hardware-transfer",
            Self::SoftwareDecode => "software-decode",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeDecoderApi {
    VulkanVideo,
    Vaapi,
    Nvdec,
    Qsv,
    Amf,
    VideoToolbox,
    D3d12,
    D3d11,
    MediaCodec,
}

impl NativeDecoderApi {
    pub fn label(self) -> &'static str {
        match self {
            Self::VulkanVideo => "vulkan-video",
            Self::Vaapi => "vaapi",
            Self::Nvdec => "nvdec",
            Self::Qsv => "qsv",
            Self::Amf => "amf",
            Self::VideoToolbox => "videotoolbox",
            Self::D3d12 => "d3d12va",
            Self::D3d11 => "d3d11va",
            Self::MediaCodec => "mediacodec",
        }
    }

    pub fn preferred_tier(self) -> NativePathTier {
        match self {
            Self::VulkanVideo | Self::Vaapi | Self::VideoToolbox | Self::MediaCodec => {
                NativePathTier::SurfaceZeroCopy
            }
            Self::Nvdec | Self::Qsv | Self::Amf | Self::D3d12 | Self::D3d11 => {
                NativePathTier::SingleGpuCopy
            }
        }
    }

    pub fn device_type(self) -> ffi::AVHWDeviceType {
        match self {
            Self::VulkanVideo => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_VULKAN,
            Self::Vaapi => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI,
            Self::Nvdec => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_CUDA,
            Self::Qsv => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_QSV,
            Self::Amf => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_AMF,
            Self::VideoToolbox => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_VIDEOTOOLBOX,
            Self::D3d12 => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_D3D12VA,
            Self::D3d11 => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_D3D11VA,
            Self::MediaCodec => ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_MEDIACODEC,
        }
    }

    fn from_device_type(device_type: ffi::AVHWDeviceType) -> Option<Self> {
        match device_type {
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_VULKAN => Some(Self::VulkanVideo),
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI => Some(Self::Vaapi),
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_CUDA => Some(Self::Nvdec),
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_QSV => Some(Self::Qsv),
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_AMF => Some(Self::Amf),
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_VIDEOTOOLBOX => Some(Self::VideoToolbox),
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_D3D12VA => Some(Self::D3d12),
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_D3D11VA => Some(Self::D3d11),
            ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_MEDIACODEC => Some(Self::MediaCodec),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HardwareDecoderChoice {
    pub api: NativeDecoderApi,
    pub device_type: ffi::AVHWDeviceType,
    pub pixel_format: ffi::AVPixelFormat,
    pub device_index: Option<u32>,
}

pub fn codec_hardware_choices(codec: *const ffi::AVCodec) -> Vec<HardwareDecoderChoice> {
    if hardware_decode_disabled() {
        return Vec::new();
    }
    let mut choices = Vec::new();
    let mut index = 0;
    loop {
        // SAFETY: `codec` comes from FFmpeg's static decoder registry and the
        // returned config is immutable static storage.
        let config = unsafe { ffi::avcodec_get_hw_config(codec, index) };
        if config.is_null() {
            break;
        }
        // SAFETY: null was rejected above.
        let config = unsafe { &*config };
        let uses_device_context =
            config.methods & ffi::AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX as i32 != 0;
        if uses_device_context
            && let Some(api) = NativeDecoderApi::from_device_type(config.device_type)
        {
            let device_index = if api == NativeDecoderApi::VulkanVideo {
                // A codec can advertise AV_PIX_FMT_VULKAN even when the
                // physical device has generic Vulkan but no Vulkan Video
                // decode queue/codec extension. Keep Vulkan first-class, but
                // only on the exact hardware rung that can actually decode it.
                let codec_id = unsafe { (*codec).id };
                let Some(index) = vulkan_video_device_index(codec_id) else {
                    index += 1;
                    continue;
                };
                Some(index)
            } else {
                None
            };
            choices.push(HardwareDecoderChoice {
                api,
                device_type: config.device_type,
                pixel_format: config.pix_fmt,
                device_index,
            });
        }
        index += 1;
    }

    choices.sort_by_key(|choice| choice_priority(choice.api));
    if let Some(requested) = requested_decoder_api() {
        choices.retain(|choice| choice.api == requested);
    }
    choices
}

pub fn create_device(choice: HardwareDecoderChoice) -> Result<*mut ffi::AVBufferRef, String> {
    if choice.api == NativeDecoderApi::Vaapi {
        return create_vaapi_device(choice);
    }
    if choice.api == NativeDecoderApi::VulkanVideo {
        let device = choice.device_index.map(|index| index.to_string());
        return create_device_with_options(choice, device.as_deref(), None);
    }
    create_device_with_options(choice, None, None)
}

fn create_vaapi_device(choice: HardwareDecoderChoice) -> Result<*mut ffi::AVBufferRef, String> {
    let device = selected_render_node();
    let explicit_driver = std::env::var("KLD_NATIVE_VAAPI_DRIVER")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let mut drivers = explicit_driver
        .map(|driver| vec![driver])
        .unwrap_or_else(|| vaapi_driver_candidates(device.as_ref()));
    if drivers.is_empty() {
        drivers.push(String::new());
    }

    let mut failures = Vec::new();
    for driver in drivers {
        match create_device_with_options(
            choice,
            device.as_deref(),
            (!driver.is_empty()).then_some(driver.as_str()),
        ) {
            Ok(device) => return Ok(device),
            Err(error) => failures.push(error),
        }
    }
    Err(failures.join("; "))
}

fn create_device_with_options(
    choice: HardwareDecoderChoice,
    device_name: Option<&str>,
    driver: Option<&str>,
) -> Result<*mut ffi::AVBufferRef, String> {
    let mut device = ptr::null_mut();
    let device_name = device_name
        .map(CString::new)
        .transpose()
        .map_err(|_| "hardware device selector contains NUL".to_string())?;
    let driver = driver
        .map(CString::new)
        .transpose()
        .map_err(|_| "VAAPI driver name contains NUL".to_string())?;
    let mut options = ptr::null_mut();
    if let Some(driver) = driver.as_ref() {
        let key = c"driver";
        // SAFETY: FFmpeg copies both NUL-terminated strings into the dictionary.
        let result = unsafe { ffi::av_dict_set(&mut options, key.as_ptr(), driver.as_ptr(), 0) };
        if result < 0 {
            return Err(format!("failed to set VAAPI driver option: {result}"));
        }
    }
    // SAFETY: FFmpeg initializes `device` on success; no options or custom
    // device string are retained by the call. The dictionary is freed below.
    let result = unsafe {
        ffi::av_hwdevice_ctx_create(
            &mut device,
            choice.device_type,
            device_name
                .as_ref()
                .map(|value| value.as_ptr())
                .unwrap_or(ptr::null()),
            options,
            0,
        )
    };
    // SAFETY: FFmpeg accepts null and releases dictionary-owned strings.
    unsafe { ffi::av_dict_free(&mut options) };
    if result < 0 {
        return Err(format!(
            "{} device creation failed (device={} driver={}): {}",
            choice.api.label(),
            device_name
                .as_ref()
                .map(|value| value.to_string_lossy())
                .unwrap_or_else(|| "auto".into()),
            driver
                .as_ref()
                .map(|value| value.to_string_lossy())
                .unwrap_or_else(|| "auto".into()),
            ffmpeg_error(result)
        ));
    }
    if device.is_null() {
        return Err(format!(
            "{} device creation returned null",
            choice.api.label()
        ));
    }
    Ok(device)
}

fn selected_render_node() -> Option<String> {
    if let Ok(device) = std::env::var("KLD_NATIVE_DRM_DEVICE")
        && !device.trim().is_empty()
    {
        return Some(device);
    }
    let mut nodes: Vec<PathBuf> = std::fs::read_dir("/dev/dri")
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("renderD"))
                .unwrap_or(false)
        })
        .collect();
    nodes.sort();
    nodes
        .into_iter()
        .next()
        .map(|path| path.to_string_lossy().into_owned())
}

fn vaapi_driver_candidates(device: Option<&String>) -> Vec<String> {
    let vendor = device
        .and_then(|device| PathBuf::from(device).file_name().map(PathBuf::from))
        .and_then(|name| {
            PathBuf::from("/sys/class/drm")
                .join(name)
                .canonicalize()
                .ok()
        })
        .and_then(|path| std::fs::read_to_string(path.join("device/vendor")).ok())
        .map(|vendor| vendor.trim().to_ascii_lowercase());
    match vendor.as_deref() {
        Some("0x8086") => vec!["iHD".to_string(), "i965".to_string()],
        Some("0x1002") => vec!["radeonsi".to_string()],
        Some("0x10de") => vec!["nvidia".to_string()],
        _ => Vec::new(),
    }
}

fn vulkan_video_device_index(codec_id: ffi::AVCodecID) -> Option<u32> {
    static CACHE: OnceLock<Mutex<std::collections::HashMap<i32, Option<u32>>>> = OnceLock::new();
    let codec_key = codec_id as i32;
    let cache = CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    if let Some(index) = cache.lock().ok()?.get(&codec_key).copied() {
        return index;
    }
    let probed = probe_vulkan_video_device(codec_id);
    if let Ok(mut cache) = cache.lock() {
        cache.insert(codec_key, probed);
    }
    probed
}

fn probe_vulkan_video_device(codec_id: ffi::AVCodecID) -> Option<u32> {
    let codec_extension = match codec_id {
        ffi::AVCodecID::AV_CODEC_ID_H264 => "VK_KHR_video_decode_h264",
        ffi::AVCodecID::AV_CODEC_ID_HEVC => "VK_KHR_video_decode_h265",
        ffi::AVCodecID::AV_CODEC_ID_AV1 => "VK_KHR_video_decode_av1",
        ffi::AVCodecID::AV_CODEC_ID_VP9 => "VK_KHR_video_decode_vp9",
        _ => return None,
    };
    // SAFETY: ash loads the process Vulkan loader and owns the resulting
    // function table independently of FFmpeg's eventual device.
    let entry = unsafe { ash::Entry::load() }.ok()?;
    let app_name = c"Kaleidux native video probe";
    let app_info = ash::vk::ApplicationInfo::default()
        .application_name(app_name)
        .api_version(ash::vk::make_api_version(0, 1, 3, 0));
    let create_info = ash::vk::InstanceCreateInfo::default().application_info(&app_info);
    // SAFETY: create_info contains no borrowed extension arrays and entry is live.
    let instance = unsafe { entry.create_instance(&create_info, None) }.ok()?;
    // SAFETY: instance is live.
    let devices = unsafe { instance.enumerate_physical_devices() }.ok();
    let selected = devices.and_then(|devices| {
        devices
            .into_iter()
            .enumerate()
            .find_map(|(index, physical_device)| {
                // SAFETY: physical_device was returned by this instance.
                let extensions = unsafe {
                    instance
                        .enumerate_device_extension_properties(physical_device)
                        .ok()?
                };
                let has = |required: &str| {
                    extensions.iter().any(|property| {
                        // SAFETY: Vulkan extension_name is NUL terminated.
                        unsafe { CStr::from_ptr(property.extension_name.as_ptr()) }.to_bytes()
                            == required.as_bytes()
                    })
                };
                (has("VK_KHR_video_queue")
                    && has("VK_KHR_video_decode_queue")
                    && has(codec_extension))
                .then_some(index as u32)
            })
    });
    // SAFETY: no children escaped this probe instance.
    unsafe { instance.destroy_instance(None) };
    selected
}

pub fn unref_device(device: &mut *mut ffi::AVBufferRef) {
    // SAFETY: FFmpeg accepts null and clears the caller-owned reference.
    unsafe { ffi::av_buffer_unref(device) };
}

pub fn pixel_format_name(format: ffi::AVPixelFormat) -> &'static str {
    // SAFETY: FFmpeg returns a static string for a valid enum value.
    let name = unsafe { ffi::av_get_pix_fmt_name(format) };
    if name.is_null() {
        return "unknown";
    }
    // FFmpeg format names are ASCII and static for the process lifetime.
    unsafe { CStr::from_ptr(name) }
        .to_str()
        .unwrap_or("non-utf8")
}

fn requested_decoder_api() -> Option<NativeDecoderApi> {
    let requested = std::env::var("KLD_NATIVE_HWDECODER").ok()?;
    match requested.trim().to_ascii_lowercase().as_str() {
        "" | "auto" => None,
        "vulkan" | "vulkan-video" => Some(NativeDecoderApi::VulkanVideo),
        "vaapi" => Some(NativeDecoderApi::Vaapi),
        "cuda" | "nvdec" => Some(NativeDecoderApi::Nvdec),
        "qsv" => Some(NativeDecoderApi::Qsv),
        "amf" => Some(NativeDecoderApi::Amf),
        "videotoolbox" => Some(NativeDecoderApi::VideoToolbox),
        "d3d12" | "d3d12va" => Some(NativeDecoderApi::D3d12),
        "d3d11" | "d3d11va" => Some(NativeDecoderApi::D3d11),
        "mediacodec" => Some(NativeDecoderApi::MediaCodec),
        "none" | "software" => None,
        _ => None,
    }
}

fn hardware_decode_disabled() -> bool {
    std::env::var("KLD_NATIVE_HWDECODER")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "none" | "software"
            )
        })
        .unwrap_or(false)
}

fn choice_priority(api: NativeDecoderApi) -> u8 {
    match api {
        NativeDecoderApi::VulkanVideo => 0,
        NativeDecoderApi::Vaapi => 1,
        NativeDecoderApi::Nvdec => 2,
        NativeDecoderApi::Qsv => 3,
        NativeDecoderApi::Amf => 4,
        NativeDecoderApi::VideoToolbox => 5,
        NativeDecoderApi::D3d12 => 6,
        NativeDecoderApi::D3d11 => 7,
        NativeDecoderApi::MediaCodec => 8,
    }
}

fn ffmpeg_error(code: i32) -> String {
    let mut message = [0i8; 256];
    // SAFETY: the stack buffer is writable and its exact length is supplied.
    let result = unsafe { ffi::av_strerror(code, message.as_mut_ptr(), message.len()) };
    if result < 0 {
        return format!("FFmpeg error {code}");
    }
    // SAFETY: av_strerror writes a NUL-terminated string on success.
    unsafe { CStr::from_ptr(message.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::{NativeDecoderApi, NativePathTier};

    #[test]
    fn capability_ladder_keeps_vulkan_video_first_class() {
        assert_eq!(
            NativeDecoderApi::VulkanVideo.preferred_tier(),
            NativePathTier::SurfaceZeroCopy
        );
        assert_eq!(
            NativeDecoderApi::Nvdec.preferred_tier(),
            NativePathTier::SingleGpuCopy
        );
    }
}
