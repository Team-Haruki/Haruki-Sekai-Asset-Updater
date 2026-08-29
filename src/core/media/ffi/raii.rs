//! Owning wrappers around the FFmpeg allocations this module makes.
//!
//! Every one exists so that an early return frees what was allocated: the
//! transcode paths are long, fallible and full of `?`, and the alternative
//! is a leak on each error path. They hold raw pointers and free them in
//! `Drop` -- no transcoding logic lives here.

use std::ffi::CStr;
use std::ptr;

use rsmpeg::ffi;

use crate::core::errors::ExportPipelineError;

use super::avio::{input_format_ptr, CustomAvio};
use super::error::{check, cstring, media_error};

pub(super) struct InputContext<'a> {
    pub(super) ptr: *mut ffi::AVFormatContext,
    pub(super) avio: Option<CustomAvio<'a>>,
}

impl<'a> InputContext<'a> {
    pub(super) unsafe fn open_file(
        url: &CStr,
        input_format: Option<&str>,
    ) -> Result<Self, ExportPipelineError> {
        let mut ptr = ptr::null_mut();
        let format = input_format_ptr(input_format)?;
        check(
            unsafe { ffi::avformat_open_input(&mut ptr, url.as_ptr(), format, ptr::null_mut()) },
            "avformat_open_input",
        )?;
        Ok(Self { ptr, avio: None })
    }

    pub(super) unsafe fn open_memory(
        data: &'a [u8],
        input_format: Option<&str>,
    ) -> Result<Self, ExportPipelineError> {
        let mut ctx = unsafe { ffi::avformat_alloc_context() };
        if ctx.is_null() {
            return Err(media_error("avformat_alloc_context failed"));
        }
        let avio = CustomAvio::new(data)?;
        unsafe {
            (*ctx).pb = avio.ctx;
            (*ctx).flags |= ffi::AVFMT_FLAG_CUSTOM_IO as i32;
        }
        let format = input_format_ptr(input_format)?;
        let mut ctx_for_open = ctx;
        let url = cstring("memory:input")?;
        check(
            unsafe {
                ffi::avformat_open_input(&mut ctx_for_open, url.as_ptr(), format, ptr::null_mut())
            },
            "avformat_open_input memory",
        )?;
        ctx = ctx_for_open;
        Ok(Self {
            ptr: ctx,
            avio: Some(avio),
        })
    }
}

impl Drop for InputContext<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::avformat_close_input(&mut self.ptr);
        }
        let _ = self.avio.take();
    }
}

pub(super) struct OutputContext {
    pub(super) ptr: *mut ffi::AVFormatContext,
    io_opened: bool,
}

impl OutputContext {
    pub(super) unsafe fn create(url: &CStr) -> Result<Self, ExportPipelineError> {
        let mut ptr = ptr::null_mut();
        check(
            unsafe {
                ffi::avformat_alloc_output_context2(
                    &mut ptr,
                    ptr::null_mut(),
                    ptr::null(),
                    url.as_ptr(),
                )
            },
            "avformat_alloc_output_context2",
        )?;
        if ptr.is_null() {
            return Err(media_error("avformat_alloc_output_context2 returned null"));
        }
        Ok(Self {
            ptr,
            io_opened: false,
        })
    }

    pub(super) unsafe fn open_io(&mut self, url: &CStr) -> Result<(), ExportPipelineError> {
        unsafe {
            if ((*(*self.ptr).oformat).flags & ffi::AVFMT_NOFILE as i32) == 0 {
                check(
                    ffi::avio_open(
                        &mut (*self.ptr).pb,
                        url.as_ptr(),
                        ffi::AVIO_FLAG_WRITE as i32,
                    ),
                    "avio_open",
                )?;
                self.io_opened = true;
            }
        }
        Ok(())
    }
}

impl Drop for OutputContext {
    fn drop(&mut self) {
        unsafe {
            if self.io_opened && !self.ptr.is_null() && !(*self.ptr).pb.is_null() {
                ffi::avio_closep(&mut (*self.ptr).pb);
            }
            if !self.ptr.is_null() {
                ffi::avformat_free_context(self.ptr);
            }
        }
    }
}

pub(super) struct CodecContext {
    pub(super) ptr: *mut ffi::AVCodecContext,
}

impl CodecContext {
    pub(super) fn new(codec: *const ffi::AVCodec) -> Result<Self, ExportPipelineError> {
        let ptr = unsafe { ffi::avcodec_alloc_context3(codec) };
        if ptr.is_null() {
            Err(media_error("avcodec_alloc_context3 failed"))
        } else {
            Ok(Self { ptr })
        }
    }
}

impl Drop for CodecContext {
    fn drop(&mut self) {
        unsafe {
            ffi::avcodec_free_context(&mut self.ptr);
        }
    }
}

pub(super) struct SwrContext {
    pub(super) ptr: *mut ffi::SwrContext,
}

impl SwrContext {
    pub(super) fn new(
        out_ch_layout: *const ffi::AVChannelLayout,
        out_sample_fmt: ffi::AVSampleFormat,
        out_sample_rate: i32,
        in_ch_layout: *const ffi::AVChannelLayout,
        in_sample_fmt: ffi::AVSampleFormat,
        in_sample_rate: i32,
    ) -> Result<Self, ExportPipelineError> {
        let mut ptr = ptr::null_mut();
        check(
            unsafe {
                ffi::swr_alloc_set_opts2(
                    &mut ptr,
                    out_ch_layout,
                    out_sample_fmt,
                    out_sample_rate,
                    in_ch_layout,
                    in_sample_fmt,
                    in_sample_rate,
                    0,
                    ptr::null_mut(),
                )
            },
            "swr_alloc_set_opts2",
        )?;
        if ptr.is_null() {
            return Err(media_error("swr_alloc_set_opts2 returned null"));
        }
        check(unsafe { ffi::swr_init(ptr) }, "swr_init")?;
        Ok(Self { ptr })
    }
}

impl Drop for SwrContext {
    fn drop(&mut self) {
        unsafe {
            ffi::swr_free(&mut self.ptr);
        }
    }
}

pub(super) struct ChannelLayout {
    pub(super) inner: ffi::AVChannelLayout,
}

impl ChannelLayout {
    pub(super) fn default_for_channels(channels: i32) -> Result<Self, ExportPipelineError> {
        if channels <= 0 {
            return Err(media_error("invalid audio channel count"));
        }
        let mut inner = unsafe { std::mem::zeroed::<ffi::AVChannelLayout>() };
        unsafe {
            ffi::av_channel_layout_default(&mut inner, channels);
        }
        Ok(Self { inner })
    }

    pub(super) fn default_or_copy(
        source: *const ffi::AVChannelLayout,
    ) -> Result<Self, ExportPipelineError> {
        let mut inner = unsafe { std::mem::zeroed::<ffi::AVChannelLayout>() };
        unsafe {
            if (*source).order == ffi::AV_CHANNEL_ORDER_UNSPEC {
                ffi::av_channel_layout_default(&mut inner, (*source).nb_channels);
            } else {
                check(
                    ffi::av_channel_layout_copy(&mut inner, source),
                    "av_channel_layout_copy",
                )?;
            }
        }
        Ok(Self { inner })
    }
}

impl Drop for ChannelLayout {
    fn drop(&mut self) {
        unsafe {
            ffi::av_channel_layout_uninit(&mut self.inner);
        }
    }
}

pub(super) struct Packet {
    pub(super) ptr: *mut ffi::AVPacket,
}

impl Packet {
    pub(super) fn new() -> Result<Self, ExportPipelineError> {
        let ptr = unsafe { ffi::av_packet_alloc() };
        if ptr.is_null() {
            Err(media_error("av_packet_alloc failed"))
        } else {
            Ok(Self { ptr })
        }
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        unsafe {
            ffi::av_packet_free(&mut self.ptr);
        }
    }
}

pub(super) struct Frame {
    pub(super) ptr: *mut ffi::AVFrame,
}

impl Frame {
    pub(super) fn new() -> Result<Self, ExportPipelineError> {
        let ptr = unsafe { ffi::av_frame_alloc() };
        if ptr.is_null() {
            Err(media_error("av_frame_alloc failed"))
        } else {
            Ok(Self { ptr })
        }
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        unsafe {
            ffi::av_frame_free(&mut self.ptr);
        }
    }
}
