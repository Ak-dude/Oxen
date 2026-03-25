/// C-compatible FFI for OxenDB.
///
/// All functions follow these conventions:
/// - Return 0 on success, negative error code on failure.
/// - Pointer arguments must be non-null unless documented otherwise.
/// - Byte slices are passed as (ptr, len) pairs.
/// - Output values are written through caller-provided pointers.
/// - Memory allocated by Rust (e.g. oxen_get) must be freed by the caller
///   using `oxen_free`.
use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::config::EngineConfig;
use crate::error::OxenError;
use crate::storage::StorageEngine;

/// Opaque handle returned to callers.  Internally an `Arc<StorageEngine>`.
pub struct OxenHandle {
    engine: Arc<StorageEngine>,
}

/// Error codes returned through the C API
pub const OXEN_OK: c_int = 0;
pub const OXEN_ERR_IO: c_int = -1;
pub const OXEN_ERR_NOT_FOUND: c_int = -2;
pub const OXEN_ERR_CORRUPTION: c_int = -3;
pub const OXEN_ERR_INVALID_ARG: c_int = -4;
pub const OXEN_ERR_ENGINE_CLOSED: c_int = -5;
pub const OXEN_ERR_NULL_PTR: c_int = -6;
pub const OXEN_ERR_UNKNOWN: c_int = -99;

fn map_error(e: &OxenError) -> c_int {
    match e {
        OxenError::Io(_) => OXEN_ERR_IO,
        OxenError::KeyNotFound => OXEN_ERR_NOT_FOUND,
        OxenError::Corruption(_) | OxenError::CrcMismatch { .. } => OXEN_ERR_CORRUPTION,
        OxenError::InvalidArgument(_) => OXEN_ERR_INVALID_ARG,
        OxenError::EngineClosed => OXEN_ERR_ENGINE_CLOSED,
        OxenError::NullPointer(_) => OXEN_ERR_NULL_PTR,
        _ => OXEN_ERR_UNKNOWN,
    }
}

/// Open a database.
///
/// # Safety
/// `data_dir` must be a valid, null-terminated UTF-8 C string.
/// `out_handle` must be a valid pointer to a `*mut OxenHandle` that will receive
/// the allocated handle on success.
#[no_mangle]
pub unsafe extern "C" fn oxen_open(
    data_dir: *const c_char,
    out_handle: *mut *mut OxenHandle,
) -> c_int {
    if data_dir.is_null() || out_handle.is_null() {
        return OXEN_ERR_NULL_PTR;
    }

    let dir = match CStr::from_ptr(data_dir).to_str() {
        Ok(s) => s,
        Err(_) => return OXEN_ERR_INVALID_ARG,
    };

    let mut config = EngineConfig::default();
    config.data_dir = std::path::PathBuf::from(dir);

    match StorageEngine::open(config) {
        Ok(engine) => {
            let handle = Box::new(OxenHandle {
                engine: Arc::new(engine),
            });
            *out_handle = Box::into_raw(handle);
            OXEN_OK
        }
        Err(e) => map_error(&e),
    }
}

/// Get the value for a key.
///
/// On success, `*out_value` points to a heap-allocated byte array of length
/// `*out_len`.  The caller must free this with `oxen_free`.
///
/// # Safety
/// All pointers must be valid and non-null.
#[no_mangle]
pub unsafe extern "C" fn oxen_get(
    handle: *mut OxenHandle,
    key_ptr: *const u8,
    key_len: usize,
    out_value: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    if handle.is_null() || key_ptr.is_null() || out_value.is_null() || out_len.is_null() {
        return OXEN_ERR_NULL_PTR;
    }

    let engine = &(*handle).engine;
    let key = std::slice::from_raw_parts(key_ptr, key_len);

    match engine.get(key) {
        Ok(Some(value)) => {
            let mut boxed = value.into_boxed_slice();
            let ptr = boxed.as_mut_ptr();
            let len = boxed.len();
            std::mem::forget(boxed);
            *out_value = ptr;
            *out_len = len;
            OXEN_OK
        }
        Ok(None) => OXEN_ERR_NOT_FOUND,
        Err(e) => map_error(&e),
    }
}

/// Store a key-value pair.
///
/// # Safety
/// All pointers must be valid and non-null.
#[no_mangle]
pub unsafe extern "C" fn oxen_put(
    handle: *mut OxenHandle,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) -> c_int {
    if handle.is_null() || key_ptr.is_null() || value_ptr.is_null() {
        return OXEN_ERR_NULL_PTR;
    }

    let engine = &(*handle).engine;
    let key = std::slice::from_raw_parts(key_ptr, key_len);
    let value = std::slice::from_raw_parts(value_ptr, value_len);

    match engine.put(key, value) {
        Ok(()) => OXEN_OK,
        Err(e) => map_error(&e),
    }
}

/// Delete a key (writes a tombstone).
///
/// # Safety
/// All pointers must be valid and non-null.
#[no_mangle]
pub unsafe extern "C" fn oxen_delete(
    handle: *mut OxenHandle,
    key_ptr: *const u8,
    key_len: usize,
) -> c_int {
    if handle.is_null() || key_ptr.is_null() {
        return OXEN_ERR_NULL_PTR;
    }

    let engine = &(*handle).engine;
    let key = std::slice::from_raw_parts(key_ptr, key_len);

    match engine.delete(key) {
        Ok(()) => OXEN_OK,
        Err(e) => map_error(&e),
    }
}

/// Scan a half-open key range `[start_key, end_key)`.
///
/// On success:
/// - `*out_keys`   — pointer to array of byte-pointer + length pairs
/// - `*out_values` — same, parallel array
/// - `*out_count`  — number of pairs
///
/// The caller must free each individual key/value buffer with `oxen_free`,
/// then free the two outer arrays with `oxen_free_scan`.
///
/// `start_key`/`end_key` may be null to indicate an open-ended bound.
///
/// # Safety
/// All non-null pointers must be valid.
#[repr(C)]
pub struct OxenByteSlice {
    pub ptr: *mut u8,
    pub len: usize,
}

#[no_mangle]
pub unsafe extern "C" fn oxen_scan(
    handle: *mut OxenHandle,
    start_ptr: *const u8,
    start_len: usize,
    end_ptr: *const u8,
    end_len: usize,
    out_keys: *mut *mut OxenByteSlice,
    out_values: *mut *mut OxenByteSlice,
    out_count: *mut usize,
) -> c_int {
    if handle.is_null() || out_keys.is_null() || out_values.is_null() || out_count.is_null() {
        return OXEN_ERR_NULL_PTR;
    }

    let engine = &(*handle).engine;

    let start: Option<&[u8]> = if start_ptr.is_null() {
        None
    } else {
        Some(std::slice::from_raw_parts(start_ptr, start_len))
    };
    let end: Option<&[u8]> = if end_ptr.is_null() {
        None
    } else {
        Some(std::slice::from_raw_parts(end_ptr, end_len))
    };

    match engine.scan(start, end) {
        Ok(pairs) => {
            let count = pairs.len();
            let mut keys_vec: Vec<OxenByteSlice> = Vec::with_capacity(count);
            let mut vals_vec: Vec<OxenByteSlice> = Vec::with_capacity(count);

            for (k, v) in pairs {
                let mut kb = k.into_boxed_slice();
                let kp = kb.as_mut_ptr();
                let kl = kb.len();
                std::mem::forget(kb);
                keys_vec.push(OxenByteSlice { ptr: kp, len: kl });

                let mut vb = v.into_boxed_slice();
                let vp = vb.as_mut_ptr();
                let vl = vb.len();
                std::mem::forget(vb);
                vals_vec.push(OxenByteSlice { ptr: vp, len: vl });
            }

            let mut keys_boxed = keys_vec.into_boxed_slice();
            let mut vals_boxed = vals_vec.into_boxed_slice();

            *out_keys = keys_boxed.as_mut_ptr();
            *out_values = vals_boxed.as_mut_ptr();
            *out_count = count;

            std::mem::forget(keys_boxed);
            std::mem::forget(vals_boxed);

            OXEN_OK
        }
        Err(e) => map_error(&e),
    }
}

/// Free a byte buffer previously returned by `oxen_get`.
///
/// # Safety
/// `ptr` must have been allocated by `oxen_get` or `oxen_scan`.
#[no_mangle]
pub unsafe extern "C" fn oxen_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    let _ = Vec::from_raw_parts(ptr, len, len);
}

/// Free arrays returned by `oxen_scan`.
///
/// The individual key/value buffers must be freed with `oxen_free` first.
///
/// # Safety
/// Pointers must have been returned by `oxen_scan`.
#[no_mangle]
pub unsafe extern "C" fn oxen_free_scan(
    keys: *mut OxenByteSlice,
    values: *mut OxenByteSlice,
    count: usize,
) {
    if !keys.is_null() {
        let _ = Vec::from_raw_parts(keys, count, count);
    }
    if !values.is_null() {
        let _ = Vec::from_raw_parts(values, count, count);
    }
}

/// Close the database and release all resources.
///
/// After this call `handle` is invalid and must not be used.
///
/// # Safety
/// `handle` must be a valid pointer returned by `oxen_open`.
#[no_mangle]
pub unsafe extern "C" fn oxen_close(handle: *mut OxenHandle) -> c_int {
    if handle.is_null() {
        return OXEN_ERR_NULL_PTR;
    }
    let boxed = Box::from_raw(handle);
    match boxed.engine.close() {
        Ok(()) => OXEN_OK,
        Err(e) => map_error(&e),
    }
}

/// Trigger an immediate synchronous compaction.
///
/// # Safety
/// `handle` must be a valid pointer returned by `oxen_open`.
#[no_mangle]
pub unsafe extern "C" fn oxen_compact(handle: *mut OxenHandle) -> c_int {
    if handle.is_null() {
        return OXEN_ERR_NULL_PTR;
    }
    let engine = &(*handle).engine;
    match engine.compact() {
        Ok(()) => OXEN_OK,
        Err(e) => map_error(&e),
    }
}
