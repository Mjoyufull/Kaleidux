#[cfg(not(target_pointer_width = "64"))]
compile_error!("CUDA interop is only supported on 64-bit targets.");

use std::os::unix::io::RawFd;
use tracing::{error, info};

type CUresult = i32;
type CUdevice = i32;
type CUcontext = *mut std::ffi::c_void;
type CUstream = *mut std::ffi::c_void;
type CUexternalSemaphore = *mut std::ffi::c_void;
type CUdeviceptr = u64;
type CUmemGenericAllocationHandle = u64;

const CUDA_SUCCESS: CUresult = 0;
const CU_MEMORYTYPE_DEVICE: u32 = 2;
const CU_MEM_ALLOCATION_TYPE_PINNED: u32 = 1;
const CU_MEM_LOCATION_TYPE_DEVICE: u32 = 1;
const CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR: u32 = 1;
const CU_MEM_ACCESS_FLAGS_PROT_READWRITE: u32 = 3;
const CU_MEM_ALLOC_GRANULARITY_MINIMUM: u32 = 0;
const CU_STREAM_NON_BLOCKING: u32 = 1;
const CU_EXTERNAL_SEMAPHORE_HANDLE_TYPE_TIMELINE_SEMAPHORE_FD: u32 = 9;

// ── FFI function types ──────────────────────────────────────────────────

type FnCuInit = unsafe extern "C" fn(u32) -> CUresult;
type FnCuDeviceGet = unsafe extern "C" fn(*mut CUdevice, i32) -> CUresult;
type FnCuCtxCreate = unsafe extern "C" fn(*mut CUcontext, u32, CUdevice) -> CUresult;
type FnCuCtxSetCurrent = unsafe extern "C" fn(CUcontext) -> CUresult;
type FnCuCtxDestroy = unsafe extern "C" fn(CUcontext) -> CUresult;
type FnCuMemcpy2D = unsafe extern "C" fn(*const CudaMemcpy2D) -> CUresult;
type FnCuMemcpy2DAsync = unsafe extern "C" fn(*const CudaMemcpy2D, CUstream) -> CUresult;
type FnCuCtxSynchronize = unsafe extern "C" fn() -> CUresult;
type FnCuStreamCreate = unsafe extern "C" fn(*mut CUstream, u32) -> CUresult;
type FnCuStreamDestroy = unsafe extern "C" fn(CUstream) -> CUresult;
type FnCuImportExternalSemaphore = unsafe extern "C" fn(
    *mut CUexternalSemaphore,
    *const CudaExternalSemaphoreHandleDesc,
) -> CUresult;
type FnCuDestroyExternalSemaphore = unsafe extern "C" fn(CUexternalSemaphore) -> CUresult;
type FnCuSignalExternalSemaphoresAsync = unsafe extern "C" fn(
    *const CUexternalSemaphore,
    *const CudaExternalSemaphoreSignalParams,
    u32,
    CUstream,
) -> CUresult;
type FnCuWaitExternalSemaphoresAsync = unsafe extern "C" fn(
    *const CUexternalSemaphore,
    *const CudaExternalSemaphoreWaitParams,
    u32,
    CUstream,
) -> CUresult;

// Virtual memory management (CUDA 10.2+)
type FnCuMemGetAllocationGranularity =
    unsafe extern "C" fn(*mut usize, *const CUmemAllocationProp, u32) -> CUresult;
type FnCuMemCreate = unsafe extern "C" fn(
    *mut CUmemGenericAllocationHandle,
    usize,
    *const CUmemAllocationProp,
    u64,
) -> CUresult;
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

#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<CudaMemcpy2D>() == 128);

/// CUDA 13.x driver ABI. The handle union's largest Linux-visible member is
/// the two-pointer Win32 form, so it occupies 16 bytes even when `fd` is used.
#[repr(C)]
struct CudaExternalSemaphoreHandleDesc {
    type_: u32,
    _type_padding: u32,
    handle: CudaExternalSemaphoreHandle,
    flags: u32,
    reserved: [u32; 16],
    _tail_padding: u32,
}

#[repr(C)]
union CudaExternalSemaphoreHandle {
    fd: i32,
    _win32: [*mut std::ffi::c_void; 2],
}

/// Signal/wait parameter structs each contain a 72-byte `params` aggregate,
/// followed by flags and 16 reserved words. The timeline fence value is the
/// first field in that aggregate.
#[repr(C, align(8))]
struct CudaExternalSemaphoreSignalParams {
    value: u64,
    params_reserved: [u8; 64],
    flags: u32,
    reserved: [u32; 16],
    _tail_padding: u32,
}

#[repr(C, align(8))]
struct CudaExternalSemaphoreWaitParams {
    value: u64,
    params_reserved: [u8; 64],
    flags: u32,
    reserved: [u32; 16],
    _tail_padding: u32,
}

const _: () = assert!(std::mem::size_of::<CudaExternalSemaphoreHandleDesc>() == 96);
const _: () = assert!(std::mem::size_of::<CudaExternalSemaphoreSignalParams>() == 144);
const _: () = assert!(std::mem::size_of::<CudaExternalSemaphoreWaitParams>() == 144);

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

pub struct CudaTimelineSemaphore {
    // Store the opaque driver token as an integer so the owning renderer can
    // move between its initialization worker and main thread. CUDA access is
    // still serialized by CudaInterop::op_lock.
    handle: usize,
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
    cu_memcpy_2d_async: FnCuMemcpy2DAsync,
    cu_stream_destroy: FnCuStreamDestroy,
    stream: CUstream,
    external_semaphore_fns: Option<ExternalSemaphoreFns>,
    cu_mem_get_allocation_granularity: FnCuMemGetAllocationGranularity,
    cu_mem_create: FnCuMemCreate,
    cu_mem_export_to_shareable_handle: FnCuMemExportToShareableHandle,
    cu_mem_address_reserve: FnCuMemAddressReserve,
    cu_mem_map: FnCuMemMap,
    cu_mem_set_access: FnCuMemSetAccess,
    cu_mem_unmap: FnCuMemUnmap,
    cu_mem_address_free: FnCuMemAddressFree,
    cu_mem_release: FnCuMemRelease,
    op_lock: parking_lot::Mutex<()>,
}

#[derive(Clone, Copy)]
struct ExternalSemaphoreFns {
    import: FnCuImportExternalSemaphore,
    destroy: FnCuDestroyExternalSemaphore,
    signal_async: FnCuSignalExternalSemaphoresAsync,
    wait_async: FnCuWaitExternalSemaphoresAsync,
}

// SAFETY: all CUDA driver entry points on `CudaInterop` acquire `op_lock` before
// touching the context or allocation state, so moving the wrapper between threads is serialized.
unsafe impl Send for CudaInterop {}
// SAFETY: shared access is serialized by `op_lock`; function pointers remain valid because
// the owning CUDA driver `Library` is stored in the same struct.
unsafe impl Sync for CudaInterop {}

fn cuda_err(name: &str, res: CUresult) -> String {
    format!("[CUDA] {name} failed with error code {res}")
}

macro_rules! load_fn {
    ($lib:expr_2021, $sym:literal) => {{
        *$lib
            .get($sym)
            .map_err(|e| format!("[CUDA] {}: {e}", std::str::from_utf8($sym).unwrap_or("?")))?
    }};
}

impl CudaInterop {
    pub fn new(wgpu_device: &wgpu::Device) -> Result<Self, String> {
        // SAFETY: CUDA symbols are loaded from the process CUDA driver library and stored
        // with the owning `Library` in `CudaInterop`, so function pointers never outlive it.
        unsafe {
            let vulkan_uuid = wgpu_device
                .as_hal::<wgpu_hal::vulkan::Api, _, _>(|device| {
                    let device =
                        device.ok_or_else(|| "CUDA interop requires Vulkan".to_string())?;
                    let mut identity = ash::vk::PhysicalDeviceIDProperties::default();
                    let mut properties =
                        ash::vk::PhysicalDeviceProperties2::default().push_next(&mut identity);
                    device
                        .shared_instance()
                        .raw_instance()
                        .get_physical_device_properties2(
                            device.raw_physical_device(),
                            &mut properties,
                        );
                    Ok::<_, String>(identity.device_uuid)
                })
                .ok_or_else(|| "Vulkan device unavailable".to_string())??;
            let lib = libloading::Library::new("libcuda.so.1")
                .or_else(|_| libloading::Library::new("libcuda.so"))
                .map_err(|e| format!("[CUDA] Failed to load libcuda.so: {e}"))?;

            let cu_init: FnCuInit = load_fn!(lib, b"cuInit\0");
            let cu_device_get: FnCuDeviceGet = load_fn!(lib, b"cuDeviceGet\0");
            let cu_device_get_count: unsafe extern "C" fn(*mut i32) -> CUresult =
                load_fn!(lib, b"cuDeviceGetCount\0");
            let cu_device_get_uuid: unsafe extern "C" fn(*mut [u8; 16], CUdevice) -> CUresult =
                load_fn!(lib, b"cuDeviceGetUuid\0");
            let cu_ctx_create: FnCuCtxCreate = load_fn!(lib, b"cuCtxCreate_v2\0");
            let cu_ctx_set_current: FnCuCtxSetCurrent = load_fn!(lib, b"cuCtxSetCurrent\0");
            let cu_ctx_destroy: FnCuCtxDestroy = load_fn!(lib, b"cuCtxDestroy_v2\0");
            let cu_ctx_synchronize: FnCuCtxSynchronize = load_fn!(lib, b"cuCtxSynchronize\0");
            let cu_memcpy_2d: FnCuMemcpy2D = load_fn!(lib, b"cuMemcpy2D_v2\0");
            let cu_memcpy_2d_async: FnCuMemcpy2DAsync = load_fn!(lib, b"cuMemcpy2DAsync_v2\0");
            let cu_stream_create: FnCuStreamCreate = load_fn!(lib, b"cuStreamCreate\0");
            let cu_stream_destroy: FnCuStreamDestroy = load_fn!(lib, b"cuStreamDestroy_v2\0");

            let external_semaphore_fns = (|| {
                Some(ExternalSemaphoreFns {
                    import: *lib
                        .get::<FnCuImportExternalSemaphore>(b"cuImportExternalSemaphore\0")
                        .ok()?,
                    destroy: *lib
                        .get::<FnCuDestroyExternalSemaphore>(b"cuDestroyExternalSemaphore\0")
                        .ok()?,
                    signal_async: *lib
                        .get::<FnCuSignalExternalSemaphoresAsync>(
                            b"cuSignalExternalSemaphoresAsync\0",
                        )
                        .ok()?,
                    wait_async: *lib
                        .get::<FnCuWaitExternalSemaphoresAsync>(b"cuWaitExternalSemaphoresAsync\0")
                        .ok()?,
                })
            })();

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

            let mut count = 0;
            let res = cu_device_get_count(&mut count);
            if res != CUDA_SUCCESS {
                return Err(cuda_err("cuDeviceGetCount", res));
            }
            let mut matched = None;
            for ordinal in 0..count {
                let mut candidate = 0;
                let mut uuid = [0u8; 16];
                if cu_device_get(&mut candidate, ordinal) == CUDA_SUCCESS
                    && cu_device_get_uuid(&mut uuid, candidate) == CUDA_SUCCESS
                    && uuid == vulkan_uuid
                {
                    matched = Some(candidate);
                    break;
                }
            }
            let device = matched
                .ok_or_else(|| "No CUDA device matches the Vulkan adapter UUID".to_string())?;

            let mut ctx: CUcontext = std::ptr::null_mut();
            let res = cu_ctx_create(&mut ctx, 0, device);
            if res != CUDA_SUCCESS {
                return Err(cuda_err("cuCtxCreate", res));
            }

            let mut stream = std::ptr::null_mut();
            let res = cu_stream_create(&mut stream, CU_STREAM_NON_BLOCKING);
            if res != CUDA_SUCCESS {
                cu_ctx_destroy(ctx);
                return Err(cuda_err("cuStreamCreate", res));
            }

            info!(
                "[CUDA] Shared interop context created on device {device} (external_timeline={})",
                external_semaphore_fns.is_some()
            );

            Ok(Self {
                _lib: lib,
                ctx,
                device,
                cu_ctx_set_current,
                cu_ctx_destroy,
                cu_ctx_synchronize,
                cu_memcpy_2d,
                cu_memcpy_2d_async,
                cu_stream_destroy,
                stream,
                external_semaphore_fns,
                cu_mem_get_allocation_granularity,
                cu_mem_create,
                cu_mem_export_to_shareable_handle,
                cu_mem_address_reserve,
                cu_mem_map,
                cu_mem_set_access,
                cu_mem_unmap,
                cu_mem_address_free,
                cu_mem_release,
                op_lock: parking_lot::Mutex::new(()),
            })
        }
    }

    fn push_context(&self) -> Result<(), String> {
        // SAFETY: `self.ctx` was created by `cuCtxCreate` during construction and is
        // destroyed only in `Drop`; callers serialize CUDA use with `op_lock`.
        let res = unsafe { (self.cu_ctx_set_current)(self.ctx) };
        if res != CUDA_SUCCESS {
            return Err(cuda_err("cuCtxSetCurrent", res));
        }
        Ok(())
    }

    /// Allocate CUDA memory exportable as a POSIX fd for Vulkan import.
    /// Returns (allocation, fd). The fd ownership transfers to the caller
    /// (Vulkan takes ownership on vkAllocateMemory with VkImportMemoryFdInfoKHR).
    pub fn allocate_exportable(
        &self,
        min_size: usize,
    ) -> Result<(ExportableCudaAllocation, RawFd), String> {
        let _guard = self.op_lock.lock();
        self.push_context()?;

        // SAFETY: CUDA virtual-memory calls use initialized structs, checked result codes,
        // and unwind failures by closing fds and releasing CUDA handles before returning.
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
            let alloc_size = min_size.div_ceil(granularity) * granularity;

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
            let res = (self.cu_mem_address_reserve)(&mut dev_ptr, alloc_size, granularity, 0, 0);
            if res != CUDA_SUCCESS {
                libc::close(fd); // Audit Point 2: Close leaked fd
                (self.cu_mem_release)(handle);
                return Err(cuda_err("cuMemAddressReserve", res));
            }

            // Map the allocation to the reserved address
            let res = (self.cu_mem_map)(dev_ptr, alloc_size, 0, handle, 0);
            if res != CUDA_SUCCESS {
                libc::close(fd); // Audit Point 2: Close leaked fd
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
                libc::close(fd); // Audit Point 2: Close leaked fd
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
        let _guard = self.op_lock.lock();
        self.push_context()?;
        // SAFETY: the current context was pushed under `op_lock` immediately above.
        let res = unsafe { (self.cu_ctx_synchronize)() };
        if res != CUDA_SUCCESS {
            return Err(cuda_err("cuCtxSynchronize", res));
        }
        Ok(())
    }

    pub fn supports_external_timeline(&self) -> bool {
        self.external_semaphore_fns.is_some()
    }

    /// Import a Vulkan timeline semaphore. CUDA takes ownership of `fd` only
    /// after a successful import; the failure path closes it here.
    pub fn import_timeline_semaphore(&self, fd: RawFd) -> Result<CudaTimelineSemaphore, String> {
        let Some(fns) = self.external_semaphore_fns else {
            // SAFETY: fd is still caller-owned because no CUDA import occurred.
            unsafe { libc::close(fd) };
            return Err("[CUDA] external timeline semaphore APIs are unavailable".into());
        };
        let _guard = self.op_lock.lock();
        if let Err(error) = self.push_context() {
            // SAFETY: no CUDA import occurred, so fd remains caller-owned.
            unsafe { libc::close(fd) };
            return Err(error);
        }
        let desc = CudaExternalSemaphoreHandleDesc {
            type_: CU_EXTERNAL_SEMAPHORE_HANDLE_TYPE_TIMELINE_SEMAPHORE_FD,
            _type_padding: 0,
            handle: CudaExternalSemaphoreHandle { fd },
            flags: 0,
            reserved: [0; 16],
            _tail_padding: 0,
        };
        let mut handle = std::ptr::null_mut();
        // SAFETY: desc exactly matches CUDA's documented driver ABI and owns
        // a live OPAQUE_FD exported from a Vulkan timeline semaphore.
        let result = unsafe { (fns.import)(&mut handle, &desc) };
        if result != CUDA_SUCCESS {
            // SAFETY: CUDA only consumes the fd on successful import.
            unsafe { libc::close(fd) };
            return Err(cuda_err("cuImportExternalSemaphore(timeline)", result));
        }
        Ok(CudaTimelineSemaphore {
            handle: handle as usize,
        })
    }

    pub fn destroy_timeline_semaphore(&self, semaphore: CudaTimelineSemaphore) {
        let Some(fns) = self.external_semaphore_fns else {
            return;
        };
        let _guard = self.op_lock.lock();
        let _ = self.push_context();
        // SAFETY: the handle came from this context's successful import and is
        // consumed exactly once by this method.
        let result = unsafe { (fns.destroy)(semaphore.handle as CUexternalSemaphore) };
        if result != CUDA_SUCCESS {
            error!("{}", cuda_err("cuDestroyExternalSemaphore", result));
        }
    }

    pub fn wait_timeline_async(
        &self,
        semaphore: &CudaTimelineSemaphore,
        value: u64,
    ) -> Result<(), String> {
        let Some(fns) = self.external_semaphore_fns else {
            return Err("[CUDA] external timeline semaphore APIs are unavailable".into());
        };
        let _guard = self.op_lock.lock();
        self.push_context()?;
        let params = CudaExternalSemaphoreWaitParams {
            value,
            params_reserved: [0; 64],
            flags: 0,
            reserved: [0; 16],
            _tail_padding: 0,
        };
        // SAFETY: the imported handle is live, the parameter ABI is asserted
        // above, and CUDA copies the one-element arrays during this call.
        let handle = semaphore.handle as CUexternalSemaphore;
        let result = unsafe { (fns.wait_async)(&handle, &params, 1, self.stream) };
        if result != CUDA_SUCCESS {
            return Err(cuda_err("cuWaitExternalSemaphoresAsync", result));
        }
        Ok(())
    }

    pub fn signal_timeline_async(
        &self,
        semaphore: &CudaTimelineSemaphore,
        value: u64,
    ) -> Result<(), String> {
        let Some(fns) = self.external_semaphore_fns else {
            return Err("[CUDA] external timeline semaphore APIs are unavailable".into());
        };
        let _guard = self.op_lock.lock();
        self.push_context()?;
        let params = CudaExternalSemaphoreSignalParams {
            value,
            params_reserved: [0; 64],
            flags: 0,
            reserved: [0; 16],
            _tail_padding: 0,
        };
        // SAFETY: same ownership and ABI conditions as wait_timeline_async.
        let handle = semaphore.handle as CUexternalSemaphore;
        let result = unsafe { (fns.signal_async)(&handle, &params, 1, self.stream) };
        if result != CUDA_SUCCESS {
            return Err(cuda_err("cuSignalExternalSemaphoresAsync", result));
        }
        Ok(())
    }

    pub fn free_exportable(&self, alloc: ExportableCudaAllocation) {
        let _guard = self.op_lock.lock();
        let _ = self.push_context();
        // SAFETY: `alloc` was returned by `allocate_exportable`; unmap/free/release are
        // called once by ownership convention and serialized under `op_lock`.
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
        let _guard = self.op_lock.lock();
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

        // SAFETY: `params` contains device pointers/pitches supplied by the caller for live
        // CUDA allocations; the call is serialized under `op_lock` with this context current.
        let res = unsafe { (self.cu_memcpy_2d)(&params) };
        if res != CUDA_SUCCESS {
            return Err(cuda_err("cuMemcpy2D", res));
        }
        Ok(())
    }

    pub fn copy_2d_async(
        &self,
        src_device_ptr: u64,
        src_pitch: usize,
        dst_device_ptr: u64,
        dst_pitch: usize,
        width_bytes: usize,
        height: usize,
    ) -> Result<(), String> {
        let _guard = self.op_lock.lock();
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
        // SAFETY: the pointers describe live device allocations and the
        // operation is enqueued on this context's persistent stream.
        let result = unsafe { (self.cu_memcpy_2d_async)(&params, self.stream) };
        if result != CUDA_SUCCESS {
            return Err(cuda_err("cuMemcpy2DAsync", result));
        }
        Ok(())
    }
}

impl Drop for CudaInterop {
    fn drop(&mut self) {
        // SAFETY: `ctx` was created by this object and is destroyed exactly once in `Drop`.
        unsafe {
            let _ = (self.cu_stream_destroy)(self.stream);
            (self.cu_ctx_destroy)(self.ctx);
        }
        info!("[CUDA] Interop context destroyed");
    }
}

// ── GStreamer CUDA buffer mapping ───────────────────────────────────────

pub struct CudaMapGuard {
    buffer: gstreamer::Buffer, // Own the buffer to prevent UAF (Audit Point 3)
    map_info: gstreamer::ffi::GstMapInfo,
}

// SAFETY: the guard owns a cloned `Buffer` and unmaps it on drop; callers only expose
// the device pointer value and do not provide shared mutable Rust references to mapped data.
unsafe impl Send for CudaMapGuard {}

impl CudaMapGuard {
    pub fn device_ptr(&self) -> u64 {
        self.map_info.data as u64
    }
}

impl Drop for CudaMapGuard {
    fn drop(&mut self) {
        // SAFETY: `map_info` was initialized by a successful `gst_buffer_map` for `buffer`
        // and is unmapped exactly once when this guard is dropped.
        unsafe {
            gstreamer::ffi::gst_buffer_unmap(self.buffer.as_ptr() as *mut _, &mut self.map_info);
        }
    }
}

pub fn map_buffer_cuda(buffer: &gstreamer::Buffer) -> Option<CudaMapGuard> {
    const GST_MAP_CUDA: u32 = 1 << 17;
    // SAFETY: `map_info` is zero-initialized for GStreamer to fill, `buffer` stays alive
    // through the returned guard, and every successful map is paired with guard unmap.
    unsafe {
        let mut map_info: gstreamer::ffi::GstMapInfo = std::mem::zeroed();
        let flags = gstreamer::ffi::GST_MAP_READ | GST_MAP_CUDA;

        let ok = gstreamer::ffi::gst_buffer_map(
            buffer.as_ptr() as *mut gstreamer::ffi::GstBuffer,
            &mut map_info,
            flags,
        );

        if ok == 0 {
            error!("[CUDA] gst_buffer_map with CUDA flag failed");
            return None;
        }

        if map_info.data.is_null() {
            error!("[CUDA] gst_buffer_map returned null data pointer");
            gstreamer::ffi::gst_buffer_unmap(buffer.as_ptr() as *mut _, &mut map_info);
            return None;
        }

        Some(CudaMapGuard {
            buffer: buffer.clone(),
            map_info,
        })
    }
}
