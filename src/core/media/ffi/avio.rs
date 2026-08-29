//! Feeding FFmpeg from a byte slice instead of a file.
//!
//! FFmpeg reads through an `AVIOContext`; this supplies one backed by
//! memory, so a bundle that was decoded in RAM does not have to be written
//! out to be transcoded. The read and seek callbacks are handed to C, so
//! the borrowed slice must outlive the context -- hence the lifetime on
//! `CustomAvio`.

use std::ffi::c_void;
use std::marker::PhantomData;
use std::ptr;

use rsmpeg::ffi;

use crate::core::errors::ExportPipelineError;

use super::error::{cstring, media_error};
use super::AVERROR_EOF;

pub(super) struct CustomAvio<'a> {
    pub(super) ctx: *mut ffi::AVIOContext,
    opaque: *mut MemoryInput,
    pub(super) _data: PhantomData<&'a [u8]>,
}

impl<'a> CustomAvio<'a> {
    pub(super) fn new(data: &'a [u8]) -> Result<Self, ExportPipelineError> {
        let buffer_size = 32 * 1024;
        let buffer = unsafe { ffi::av_malloc(buffer_size) as *mut u8 };
        if buffer.is_null() {
            return Err(media_error("av_malloc failed for AVIO buffer"));
        }
        let opaque = Box::into_raw(Box::new(MemoryInput {
            data: data.as_ptr(),
            len: data.len(),
            position: 0,
        }));
        let ctx = unsafe {
            ffi::avio_alloc_context(
                buffer,
                buffer_size as i32,
                0,
                opaque as *mut c_void,
                Some(read_memory_packet),
                None,
                Some(seek_memory),
            )
        };
        if ctx.is_null() {
            unsafe {
                ffi::av_free(buffer as *mut c_void);
                drop(Box::from_raw(opaque));
            }
            return Err(media_error("avio_alloc_context failed"));
        }
        Ok(Self {
            ctx,
            opaque,
            _data: PhantomData,
        })
    }
}

impl Drop for CustomAvio<'_> {
    fn drop(&mut self) {
        unsafe {
            if !self.ctx.is_null() {
                ffi::avio_context_free(&mut self.ctx);
            }
            if !self.opaque.is_null() {
                drop(Box::from_raw(self.opaque));
                self.opaque = ptr::null_mut();
            }
        }
    }
}

struct MemoryInput {
    data: *const u8,
    len: usize,
    position: usize,
}

unsafe extern "C" fn read_memory_packet(opaque: *mut c_void, buf: *mut u8, buf_size: i32) -> i32 {
    let input = unsafe { &mut *(opaque as *mut MemoryInput) };
    if input.position >= input.len {
        return AVERROR_EOF;
    }
    let remaining = input.len - input.position;
    let len = remaining.min(buf_size as usize);
    unsafe {
        ptr::copy_nonoverlapping(input.data.add(input.position), buf, len);
    }
    input.position += len;
    len as i32
}

unsafe extern "C" fn seek_memory(opaque: *mut c_void, offset: i64, whence: i32) -> i64 {
    let input = unsafe { &mut *(opaque as *mut MemoryInput) };
    if whence == ffi::AVSEEK_SIZE as i32 {
        return input.len as i64;
    }
    let base = match whence {
        libc::SEEK_SET => 0_i64,
        libc::SEEK_CUR => input.position as i64,
        libc::SEEK_END => input.len as i64,
        _ => return -1,
    };
    let Some(position) = base.checked_add(offset) else {
        return -1;
    };
    if position < 0 || position as usize > input.len {
        return -1;
    }
    input.position = position as usize;
    position
}

pub(super) fn input_format_ptr(
    format: Option<&str>,
) -> Result<*mut ffi::AVInputFormat, ExportPipelineError> {
    let Some(format) = format else {
        return Ok(ptr::null_mut());
    };
    let format = cstring(format)?;
    let ptr = unsafe { ffi::av_find_input_format(format.as_ptr()) };
    if ptr.is_null() {
        Err(media_error(&format!(
            "FFmpeg input format is unavailable: {}",
            format.to_string_lossy()
        )))
    } else {
        Ok(ptr as *mut ffi::AVInputFormat)
    }
}
