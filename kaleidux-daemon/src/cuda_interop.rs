use std::os::unix::io::RawFd;
use tracing::{error, info};

type CUresult = i32;
type CUdevice = i32;
type CUcontext = *mut std::ffi::c_void;
type CUdeviceptr = u64;
type CUmemGenericAllocationHandle = u64;

const CUDA_SUCCESS: CUresult = 0;
const CU_MEMORYTYPE_DEVICE: u32 = 2;
const CU_MEM_ALLOCATION_TYPE_PINNED: u32 = 1;
const CU_MEM_LOCATION_TYPE_DEVICE: u32 = 1;
const CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR: u32 = 1;
const CU_MEM_ACCESS_FLAGS_PROT_READWRITE: u32 = 3;
const CU_MEM_ALLOC_GRANULARITY_MINIMUM: u32 = 0;

// ── FFI function types ──────────────────────────────────────────────────

type FnCuInit = unsafe extern "C" fn(u32) -> CUresult;
type FnCuDeviceGet = unsafe extern "C" fn(*mut CUdevice, i32) -> CUresult;
type FnCuCtxCreate = unsafe extern "C" fn(*mut CUcontext, u32, CUdevice) -> CUresult;
type FnCuCtxSetCurrent = unsafe extern "C" fn(CUcontext) -> CUresult;
type FnCuCtxDestroy = unsafe extern "C" fn(CUcontext) -> CUresult;
type FnCuMemcpy2D = unsafe extern "C" fn(*const CudaMemcpy2D) -> CUresult;
type FnCuCtxSynchronize = unsafe extern "C" fn() -> CUresult;

// Virtual memory management (CUDA 10.2+)
type FnCuMemGetAllocationGranularity =
    unsafe extern "C" fn(*mut usize, *const CUmemAllocationProp, u32) -> CUresult;
type FnCuMemCreate =
    unsafe extern "C" fn(*mut CUmemGenericAllocationHandle, usize, *const CUmemAllocationProp, u64)
        -> CUresult;
type FnCuMemExportToShareableHandle =
    unsafe extern "C" fn(*mut std::ffi::c_void, CUmemGenericAllocationHandle, u32, u64) -> CUresult;
type FnCuMemAddressReserve =
    unsafe extern "C" fn(*mut CUdeviceptr, usize, usize, CUdeviceptr, u64) -> CUresult;
type FnCuMemMap =
    unsafe extern "C" fn(CUdeviceptr, usize, usize, CUmemGenericAllocationHandle, u64) -> CUresult;
type FnCuMemSetAccess =
    unsafe extern "C" fn(CUdeviceptr, usize, *const CUmemAccessDesc, usize) -> CUresult;
type FnCuMemUnmap = unsafe extern "C" fn(CUdeviceptr, usize) -> CUresult;
type FnCuMemAddressFree = unsafe extern "C" fn(CUdeviceptr, usize) -> CUresult;
type FnCuMemRelease = unsafe extern "C" fn(CUmemGenericAllocationHandle) -> CUresult;

// ── FFI structs ─────────────────────────────────────────────────────────

#[repr(C)]
struct CudaMemcpy2D {
    src_x_in_bytes: usize,
    src_y: usize,
    src_memory_type: u32,
    _pad0: u32,
    src_host: *const std::ffi::c_void,
    src_device: CUdeviceptr,
    src_array: *mut std::ffi::c_void,
    src_pitch: usize,
    dst_x_in_bytes: usize,
    dst_y: usize,
    dst_memory_type: u32,
    _pad1: u32,
    dst_host: *mut std::ffi::c_void,
    dst_device: CUdeviceptr,
    dst_array: *mut std::ffi::c_void,
    dst_pitch: usize,
    width_in_bytes: usize,
    height: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CUmemLocation {
    type_: u32,
    id: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CUmemAllocFlags {
    compression_type: u8,
    gpu_direct_rdma_capable: u8,
    usage: u16,
    reserved: [u8; 4],
}

#[repr(C)]
struct CUmemAllocationProp {
    type_: u32,
    requested_handle_types: u32,
    location: CUmemLocation,
    win32_handle_meta_data: *mut std::ffi::c_void,
    alloc_flags: CUmemAllocFlags,
}

#[repr(C)]
struct CUmemAccessDesc {
    location: CUmemLocation,
    flags: u32,
}

// ── Public types ────────────────────────────────────────────────────────

pub struct ExportableCudaAllocation {
    handle: CUmemGenericAllocationHandle,
    pub dev_ptr: CUdeviceptr,
    alloc_size: usize,
}

impl ExportableCudaAllocation {
    pub fn alloc_size(&self) -> usize {
        self.alloc_size
    }
}

pub struct CudaInterop {
    _lib: libloading::Library,
    ctx: CUcontext,
    device: CUdevice,
    cu_ctx_set_current: FnCuCtxSetCurrent,
    cu_ctx_destroy: FnCuCtxDestroy,
    cu_ctx_synchronize: FnCuCtxSynchronize,
    cu_memcpy_2d: FnCuMemcpy2D,
    cu_mem_get_allocation_granularity: FnCuMemGetAllocationGranularity,
    cu_mem_create: FnCuMemCreate,
    cu_mem_export_to_shareable_handle: FnCuMemExportToShareableHandle,
    cu_mem_address_reserve: FnCuMemAddressReserve,
    cu_mem_map: FnCuMemMap,
    cu_mem_set_access: FnCuMemSetAccess,
    cu_mem_unmap: FnCuMemUnmap,
    cu_mem_address_free: FnCuMemAddressFree,
    cu_mem_release: FnCuMemRelease,
}

unsafe impl Send for CudaInterop {}
unsafe impl Sync for CudaInterop {}

fn cuda_err(name: &str, res: CUresult) -> String {
    format!("[CUDA] {name} failed with error code {res}")
}

macro_rules! load_fn {
    ($lib:expr, $sym:literal) => {{
        *$lib
            .get($sym)
            .map_err(|e| format!("[CUDA] {}: {e}", std::str::from_utf8($sym).unwrap_or("?")))?
    }};
}

impl CudaInterop {
    pub fn new() -> Result<Self, String> {
        unsafe {
            let lib = libloading::Library::new("libcuda.so.1")
                .or_else(|_| libloading::Library::new("libcuda.so"))
                .map_err(|e| format!("[CUDA] Failed to load libcuda.so: {e}"))?;

            let cu_init: FnCuInit = load_fn!(lib, b"cuInit\0");
            let cu_device_get: FnCuDeviceGet = load_fn!(lib, b"cuDeviceGet\0");
            let cu_ctx_create: FnCuCtxCreate = load_fn!(lib, b"cuCtxCreate_v2\0");
            let cu_ctx_set_current: FnCuCtxSetCurrent = load_fn!(lib, b"cuCtxSetCurrent\0");
            let cu_ctx_destroy: FnCuCtxDestroy = load_fn!(lib, b"cuCtxDestroy_v2\0");
            let cu_ctx_synchronize: FnCuCtxSynchronize = load_fn!(lib, b"cuCtxSynchronize\0");
            let cu_memcpy_2d: FnCuMemcpy2D = load_fn!(lib, b"cuMemcpy2D_v2\0");

            let cu_mem_get_allocation_granularity: FnCuMemGetAllocationGranularity =
                load_fn!(lib, b"cuMemGetAllocationGranularity\0");
            let cu_mem_create: FnCuMemCreate = load_fn!(lib, b"cuMemCreate\0");
            let cu_mem_export_to_shareable_handle: FnCuMemExportToShareableHandle =
                load_fn!(lib, b"cuMemExportToShareableHandle\0");
            let cu_mem_address_reserve: FnCuMemAddressReserve =
                load_fn!(lib, b"cuMemAddressReserve\0");
            let cu_mem_map: FnCuMemMap = load_fn!(lib, b"cuMemMap\0");
            let cu_mem_set_access: FnCuMemSetAccess = load_fn!(lib, b"cuMemSetAccess\0");
            let cu_mem_unmap: FnCuMemUnmap = load_fn!(lib, b"cuMemUnmap\0");
            let cu_mem_address_free: FnCuMemAddressFree = load_fn!(lib, b"cuMemAddressFree\0");
            let cu_mem_release: FnCuMemRelease = load_fn!(lib, b"cuMemRelease\0");

            let res = cu_init(0);
            if res != CUDA_SUCCESS {
                return Err(cuda_err("cuInit", res));
            }

            let mut device: CUdevice = 0;
            let res = cu_device_get(&mut device, 0);
            if res != CUDA_SUCCESS {
                return Err(cuda_err("cuDeviceGet", res));
            }

            let mut ctx: CUcontext = std::ptr::null_mut();
            let res = cu_ctx_create(&mut ctx, 0, device);
            if res != CUDA_SUCCESS {
                return Err(cuda_err("cuCtxCreate", res));
            }

            info!("[CUDA] Interop context created on device {device}");

            Ok(Self {
                _lib: lib,
                ctx,
                device,
                cu_ctx_set_current,
                cu_ctx_destroy,
                cu_ctx_synchronize,
                cu_memcpy_2d,
                cu_mem_get_allocation_granularity,
                cu_mem_create,
                cu_mem_export_to_shareable_handle,
                cu_mem_address_reserve,
                cu_mem_map,
                cu_mem_set_access,
                cu_mem_unmap,
                cu_mem_address_free,
                cu_mem_release,
            })
        }
    }

    fn push_context(&self) -> Result<(), String> {
        let res = unsafe { (self.cu_ctx_set_current)(self.ctx) };
        if res != CUDA_SUCCESS {
            return Err(cuda_err("cuCtxSetCurrent", res));
        }
        Ok(())
    }

    /// Allocate CUDA memory exportable as a POSIX fd for Vulkan import.
    /// Returns (allocation, fd). The fd ownership transfers to the caller
    /// (Vulkan takes ownership on vkAllocateMemory with VkImportMemoryFdInfoKHR).
    pub fn allocate_exportable(&self, min_size: usize) -> Result<(ExportableCudaAllocation, RawFd), String> {
        self.push_context()?;

        unsafe {
            let prop = CUmemAllocationProp {
                type_: CU_MEM_ALLOCATION_TYPE_PINNED,
                requested_handle_types: CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                location: CUmemLocation {
                    type_: CU_MEM_LOCATION_TYPE_DEVICE,
                    id: self.device,
                },
                win32_handle_meta_data: std::ptr::null_mut(),
                alloc_flags: CUmemAllocFlags {
                    compression_type: 0,
                    gpu_direct_rdma_capable: 0,
                    usage: 0,
                    reserved: [0; 4],
                },
            };

            // Query allocation granularity
            let mut granularity: usize = 0;
            let res = (self.cu_mem_get_allocation_granularity)(
                &mut granularity,
                &prop,
                CU_MEM_ALLOC_GRANULARITY_MINIMUM,
            );
            if res != CUDA_SUCCESS {
                return Err(cuda_err("cuMemGetAllocationGranularity", res));
            }
            if granularity == 0 {
                return Err("[CUDA] Allocation granularity is 0".into());
            }

            // Round up to granularity
            let alloc_size = ((min_size + granularity - 1) / granularity) * granularity;

            // Create exportable allocation
            let mut handle: CUmemGenericAllocationHandle = 0;
            let res = (self.cu_mem_create)(&mut handle, alloc_size, &prop, 0);
            if res != CUDA_SUCCESS {
                return Err(cuda_err("cuMemCreate", res));
            }

            // Export as POSIX fd
            let mut fd: i32 = -1;
            let res = (self.cu_mem_export_to_shareable_handle)(
                &mut fd as *mut i32 as *mut std::ffi::c_void,
                handle,
                CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                0,
            );
            if res != CUDA_SUCCESS {
                (self.cu_mem_release)(handle);
                return Err(cuda_err("cuMemExportToShareableHandle", res));
            }

            // Reserve virtual address space
            let mut dev_ptr: CUdeviceptr = 0;
            let res =
                (self.cu_mem_address_reserve)(&mut dev_ptr, alloc_size, granularity, 0, 0);
            if res != CUDA_SUCCESS {
                (self.cu_mem_release)(handle);
                return Err(cuda_err("cuMemAddressReserve", res));
            }

            // Map the allocation to the reserved address
            let res = (self.cu_mem_map)(dev_ptr, alloc_size, 0, handle, 0);
            if res != CUDA_SUCCESS {
                (self.cu_mem_address_free)(dev_ptr, alloc_size);
                (self.cu_mem_release)(handle);
                return Err(cuda_err("cuMemMap", res));
            }

            // Set read/write access
            let access_desc = CUmemAccessDesc {
                location: CUmemLocation {
                    type_: CU_MEM_LOCATION_TYPE_DEVICE,
                    id: self.device,
                },
                flags: CU_MEM_ACCESS_FLAGS_PROT_READWRITE,
            };
            let res = (self.cu_mem_set_access)(dev_ptr, alloc_size, &access_desc, 1);
            if res != CUDA_SUCCESS {
                (self.cu_mem_unmap)(dev_ptr, alloc_size);
                (self.cu_mem_address_free)(dev_ptr, alloc_size);
                (self.cu_mem_release)(handle);
                return Err(cuda_err("cuMemSetAccess", res));
            }

            info!(
                "[CUDA] Exportable allocation: size={alloc_size} (requested {min_size}), \
                 granularity={granularity}, dev_ptr={dev_ptr:#x}, fd={fd}"
            );

            Ok((
                ExportableCudaAllocation {
                    handle,
                    dev_ptr,
                    alloc_size,
                },
                fd,
            ))
        }
    }

    pub fn synchronize(&self) -> Result<(), String> {
        self.push_context()?;
        let res = unsafe { (self.cu_ctx_synchronize)() };
        if res != CUDA_SUCCESS {
            return Err(cuda_err("cuCtxSynchronize", res));
        }
        Ok(())
    }

    pub fn free_exportable(&self, alloc: ExportableCudaAllocation) {
        let _ = self.push_context();
        unsafe {
            (self.cu_mem_unmap)(alloc.dev_ptr, alloc.alloc_size);
            (self.cu_mem_address_free)(alloc.dev_ptr, alloc.alloc_size);
            (self.cu_mem_release)(alloc.handle);
        }
    }

    pub fn copy_2d(
        &self,
        src_device_ptr: u64,
        src_pitch: usize,
        dst_device_ptr: u64,
        dst_pitch: usize,
        width_bytes: usize,
        height: usize,
    ) -> Result<(), String> {
        self.push_context()?;

        let params = CudaMemcpy2D {
            src_x_in_bytes: 0,
            src_y: 0,
            src_memory_type: CU_MEMORYTYPE_DEVICE,
            _pad0: 0,
            src_host: std::ptr::null(),
            src_device: src_device_ptr,
            src_array: std::ptr::null_mut(),
            src_pitch,
            dst_x_in_bytes: 0,
            dst_y: 0,
            dst_memory_type: CU_MEMORYTYPE_DEVICE,
            _pad1: 0,
            dst_host: std::ptr::null_mut(),
            dst_device: dst_device_ptr,
            dst_array: std::ptr::null_mut(),
            dst_pitch,
            width_in_bytes: width_bytes,
            height,
        };

        let res = unsafe { (self.cu_memcpy_2d)(&params) };
        if res != CUDA_SUCCESS {
            return Err(cuda_err("cuMemcpy2D", res));
        }
        Ok(())
    }
}

impl Drop for CudaInterop {
    fn drop(&mut self) {
        unsafe {
            (self.cu_ctx_destroy)(self.ctx);
        }
        info!("[CUDA] Interop context destroyed");
    }
}

// ── GStreamer CUDA buffer mapping ───────────────────────────────────────

pub struct CudaMapGuard {
    raw_buf: *mut gstreamer::ffi::GstBuffer,
    map_info: gstreamer::ffi::GstMapInfo,
}

unsafe impl Send for CudaMapGuard {}

impl CudaMapGuard {
    pub fn device_ptr(&self) -> u64 {
        self.map_info.data as u64
    }
}

impl Drop for CudaMapGuard {
    fn drop(&mut self) {
        unsafe {
            gstreamer::ffi::gst_buffer_unmap(self.raw_buf, &mut self.map_info);
        }
    }
}

pub fn map_buffer_cuda(buffer: &gstreamer::Buffer) -> Option<CudaMapGuard> {
    const GST_MAP_CUDA: u32 = 1 << 17;
    unsafe {
        let raw_buf = buffer.as_ptr() as *mut gstreamer::ffi::GstBuffer;
        let mut map_info: gstreamer::ffi::GstMapInfo = std::mem::zeroed();
        let flags = gstreamer::ffi::GST_MAP_READ | GST_MAP_CUDA;

        let ok = gstreamer::ffi::gst_buffer_map(raw_buf, &mut map_info, flags);

        if ok == 0 {
            error!("[CUDA] gst_buffer_map with CUDA flag failed");
            return None;
        }

        if map_info.data.is_null() {
            error!("[CUDA] gst_buffer_map returned null data pointer");
            gstreamer::ffi::gst_buffer_unmap(raw_buf, &mut map_info);
            return None;
        }

        Some(CudaMapGuard { raw_buf, map_info })
    }
}
