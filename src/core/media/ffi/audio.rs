//! The audio half of the FFmpeg bridge: PCM encoding, resampling and the
//! sample FIFO an encoder with a fixed frame size needs.

use std::ffi::c_void;
use std::io::Cursor;
use std::path::Path;
use std::ptr;

use cridecoder::HcaDecoder;
use rsmpeg::ffi;

use crate::core::errors::ExportPipelineError;

use super::error::{check, ffmpeg_error, media_error, path_cstring};
use super::raii::{ChannelLayout, CodecContext, Frame, OutputContext, SwrContext};
// The codec choice, the loader guard and the encoder drain live with the
// transcode drivers; audio encoding calls into them rather than the reverse.
use super::{drain_encoder, ensure_ffmpeg_loaded, OutputCodec};

pub(super) fn convert_hca_bytes_to_audio(
    hca_bytes: &[u8],
    output: &Path,
    output_codec: OutputCodec,
) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    let mut decoder = HcaDecoder::from_reader(Cursor::new(hca_bytes)).map_err(|err| {
        ExportPipelineError::Media {
            message: format!("HCA decode init failed: {err}"),
        }
    })?;
    let info = decoder.info().clone();
    let sample_rate = info.sampling_rate as i32;
    let channels = info.channel_count as i32;

    let mut encode_error = None;
    unsafe {
        encode_pcm16_to_file(output, output_codec, sample_rate, channels, |encoder| {
            decoder
                .decode_to_pcm16_chunks(|samples| {
                    if encode_error.is_none() {
                        if let Err(err) = encoder.encode_samples(samples) {
                            encode_error = Some(err);
                        }
                    }
                    Ok(())
                })
                .map_err(|err| ExportPipelineError::Media {
                    message: format!("HCA decode failed: {err}"),
                })?;
            if let Some(err) = encode_error.take() {
                return Err(err);
            }
            Ok(())
        })
    }
}

pub(super) unsafe fn encode_pcm16_to_file<F>(
    output: &Path,
    output_codec: OutputCodec,
    sample_rate: i32,
    channels: i32,
    mut produce: F,
) -> Result<(), ExportPipelineError>
where
    F: FnMut(&mut Pcm16Encoder<'_>) -> Result<(), ExportPipelineError>,
{
    let output_url = path_cstring(output)?;
    let mut output_ctx = OutputContext::create(&output_url)?;
    let encoder = unsafe { ffi::avcodec_find_encoder(output_codec.codec_id()) };
    if encoder.is_null() {
        return Err(media_error(&format!(
            "could not find FFmpeg encoder for codec id {}",
            output_codec.codec_id()
        )));
    }
    let encoder_ctx = CodecContext::new(encoder)?;
    configure_pcm16_encoder(
        encoder_ctx.ptr,
        encoder,
        output_codec,
        output_ctx.ptr,
        sample_rate,
        channels,
    )?;
    check(
        unsafe { ffi::avcodec_open2(encoder_ctx.ptr, encoder, ptr::null_mut()) },
        "avcodec_open2 pcm encoder",
    )?;

    let output_stream = unsafe { ffi::avformat_new_stream(output_ctx.ptr, ptr::null()) };
    if output_stream.is_null() {
        return Err(media_error("avformat_new_stream failed"));
    }
    unsafe {
        (*output_stream).time_base = (*encoder_ctx.ptr).time_base;
    }
    check(
        unsafe { ffi::avcodec_parameters_from_context((*output_stream).codecpar, encoder_ctx.ptr) },
        "avcodec_parameters_from_context pcm",
    )?;

    output_ctx.open_io(&output_url)?;
    check(
        unsafe { ffi::avformat_write_header(output_ctx.ptr, ptr::null_mut()) },
        "avformat_write_header pcm",
    )?;

    let input = Frame::new()?;
    let converted = Frame::new()?;
    let mut audio_fifo = AudioFifo::new(encoder_ctx.ptr)?;
    let input_layout = ChannelLayout::default_for_channels(channels)?;
    let mut frame_index = 0_i64;
    let mut encoder_state = Pcm16Encoder {
        encoder_ctx: encoder_ctx.ptr,
        output_ctx: output_ctx.ptr,
        output_stream,
        input: input.ptr,
        converted: converted.ptr,
        audio_fifo: &mut audio_fifo,
        frame_index: &mut frame_index,
        sample_rate,
        channels,
        input_layout: &input_layout,
    };
    produce(&mut encoder_state)?;

    if let Some(fifo) = audio_fifo.as_mut() {
        fifo.encode_available(
            encoder_ctx.ptr,
            output_ctx.ptr,
            output_stream,
            &mut frame_index,
            true,
        )?;
    }
    check(
        unsafe { ffi::avcodec_send_frame(encoder_ctx.ptr, ptr::null()) },
        "avcodec_send_frame pcm flush",
    )?;
    drain_encoder(encoder_ctx.ptr, output_ctx.ptr, output_stream)?;
    check(
        unsafe { ffi::av_write_trailer(output_ctx.ptr) },
        "av_write_trailer pcm",
    )?;
    Ok(())
}

pub(super) unsafe fn configure_pcm16_encoder(
    encoder_ctx: *mut ffi::AVCodecContext,
    encoder: *const ffi::AVCodec,
    output_codec: OutputCodec,
    output_ctx: *mut ffi::AVFormatContext,
    sample_rate: i32,
    channels: i32,
) -> Result<(), ExportPipelineError> {
    unsafe {
        (*encoder_ctx).sample_rate = sample_rate;
        (*encoder_ctx).sample_fmt = choose_sample_format(encoder, ffi::AV_SAMPLE_FMT_S16)?;
        ffi::av_channel_layout_default(&mut (*encoder_ctx).ch_layout, channels);
        (*encoder_ctx).time_base = ffi::AVRational {
            num: 1,
            den: sample_rate,
        };
        if matches!(output_codec, OutputCodec::Mp3) {
            (*encoder_ctx).bit_rate = 320_000;
        }
        if !(*output_ctx).oformat.is_null()
            && ((*(*output_ctx).oformat).flags & ffi::AVFMT_GLOBALHEADER as i32) != 0
        {
            (*encoder_ctx).flags |= ffi::AV_CODEC_FLAG_GLOBAL_HEADER as i32;
        }
    }
    Ok(())
}

pub(super) struct Pcm16Encoder<'a> {
    encoder_ctx: *mut ffi::AVCodecContext,
    output_ctx: *mut ffi::AVFormatContext,
    output_stream: *mut ffi::AVStream,
    input: *mut ffi::AVFrame,
    converted: *mut ffi::AVFrame,
    audio_fifo: &'a mut Option<AudioFifo>,
    frame_index: &'a mut i64,
    sample_rate: i32,
    channels: i32,
    input_layout: &'a ChannelLayout,
}

impl Pcm16Encoder<'_> {
    pub(super) unsafe fn encode_samples(
        &mut self,
        samples: &[i16],
    ) -> Result<(), ExportPipelineError> {
        if samples.is_empty() {
            return Ok(());
        }
        let channels = self.channels as usize;
        if channels == 0 || !samples.len().is_multiple_of(channels) {
            return Err(media_error(
                "PCM16 sample chunk is not aligned to channel count",
            ));
        }

        unsafe {
            ffi::av_frame_unref(self.input);
            (*self.input).format = ffi::AV_SAMPLE_FMT_S16;
            (*self.input).sample_rate = self.sample_rate;
            (*self.input).nb_samples = (samples.len() / channels) as i32;
            check(
                ffi::av_channel_layout_copy(&mut (*self.input).ch_layout, &self.input_layout.inner),
                "av_channel_layout_copy pcm input frame",
            )?;
            check(
                ffi::av_frame_get_buffer(self.input, 0),
                "av_frame_get_buffer pcm input frame",
            )?;
            let byte_len = std::mem::size_of_val(samples);
            ptr::copy_nonoverlapping(
                samples.as_ptr() as *const u8,
                (*self.input).data[0],
                byte_len,
            );

            let needs_resample = (*self.encoder_ctx).sample_fmt != ffi::AV_SAMPLE_FMT_S16
                || (*self.encoder_ctx).sample_rate != self.sample_rate
                || ffi::av_channel_layout_compare(
                    &(*self.encoder_ctx).ch_layout,
                    &self.input_layout.inner,
                ) != 0;
            let encoded_frame = if needs_resample {
                resample_audio_frame(self.encoder_ctx, self.input, self.converted)?
            } else {
                self.input
            };

            if let Some(fifo) = self.audio_fifo.as_mut() {
                fifo.push(encoded_frame)?;
                fifo.encode_available(
                    self.encoder_ctx,
                    self.output_ctx,
                    self.output_stream,
                    self.frame_index,
                    false,
                )?;
            } else {
                (*encoded_frame).pts = *self.frame_index;
                *self.frame_index += (*encoded_frame).nb_samples as i64;
                check(
                    ffi::avcodec_send_frame(self.encoder_ctx, encoded_frame),
                    "avcodec_send_frame pcm",
                )?;
                drain_encoder(self.encoder_ctx, self.output_ctx, self.output_stream)?;
            }
            ffi::av_frame_unref(self.input);
            ffi::av_frame_unref(self.converted);
        }
        Ok(())
    }
}

pub(super) unsafe fn resample_audio_frame(
    encoder_ctx: *mut ffi::AVCodecContext,
    decoded: *mut ffi::AVFrame,
    converted: *mut ffi::AVFrame,
) -> Result<*mut ffi::AVFrame, ExportPipelineError> {
    unsafe {
        (*converted).format = (*encoder_ctx).sample_fmt;
        (*converted).sample_rate = (*encoder_ctx).sample_rate;
        (*converted).nb_samples = (*decoded).nb_samples;
        check(
            ffi::av_channel_layout_copy(&mut (*converted).ch_layout, &(*encoder_ctx).ch_layout),
            "av_channel_layout_copy audio resample output",
        )?;
        check(
            ffi::av_frame_get_buffer(converted, 0),
            "av_frame_get_buffer audio resample",
        )?;

        if (*decoded).ch_layout.order == ffi::AV_CHANNEL_ORDER_UNSPEC {
            let channels = (*decoded).ch_layout.nb_channels;
            ffi::av_channel_layout_uninit(&mut (*decoded).ch_layout);
            ffi::av_channel_layout_default(&mut (*decoded).ch_layout, channels);
        }
        let input_layout = ChannelLayout::default_or_copy(&(*decoded).ch_layout)?;
        let swr = SwrContext::new(
            &(*converted).ch_layout,
            (*encoder_ctx).sample_fmt,
            (*encoder_ctx).sample_rate,
            &input_layout.inner,
            (*decoded).format,
            (*decoded).sample_rate,
        )?;
        check(
            ffi::swr_convert_frame(swr.ptr, converted, decoded),
            "swr_convert_frame",
        )?;
        Ok(converted)
    }
}

pub(super) unsafe fn choose_sample_format(
    codec: *const ffi::AVCodec,
    decoder_format: ffi::AVSampleFormat,
) -> Result<ffi::AVSampleFormat, ExportPipelineError> {
    unsafe {
        if (*codec).sample_fmts.is_null() {
            return Ok(decoder_format);
        }
        let mut cursor = (*codec).sample_fmts;
        while *cursor != ffi::AV_SAMPLE_FMT_NONE {
            if *cursor == decoder_format {
                return Ok(decoder_format);
            }
            cursor = cursor.add(1);
        }
        Ok(*(*codec).sample_fmts)
    }
}

// Not an owning wrapper: this drives the encoder through
// `drain_encoder` as it drains, so it stays with the transcoding logic
// rather than moving to `raii` with the types that only own memory.
pub(super) struct AudioFifo {
    ptr: *mut ffi::AVAudioFifo,
    frame_size: i32,
    pad_final_frame: bool,
    sample_fmt: ffi::AVSampleFormat,
    sample_rate: i32,
    ch_layout: ChannelLayout,
    frame: Frame,
}

impl AudioFifo {
    pub(super) fn new(
        encoder_ctx: *mut ffi::AVCodecContext,
    ) -> Result<Option<Self>, ExportPipelineError> {
        unsafe {
            if (*encoder_ctx).codec_type != ffi::AVMEDIA_TYPE_AUDIO
                || (*encoder_ctx).frame_size <= 0
            {
                return Ok(None);
            }
            let ch_layout = ChannelLayout::default_or_copy(&(*encoder_ctx).ch_layout)?;
            let ptr = ffi::av_audio_fifo_alloc(
                (*encoder_ctx).sample_fmt,
                (*encoder_ctx).ch_layout.nb_channels,
                (*encoder_ctx).frame_size,
            );
            if ptr.is_null() {
                return Err(media_error("av_audio_fifo_alloc failed"));
            }
            Ok(Some(Self {
                ptr,
                frame_size: (*encoder_ctx).frame_size,
                pad_final_frame: (*encoder_ctx).codec_id == ffi::AV_CODEC_ID_MP3,
                sample_fmt: (*encoder_ctx).sample_fmt,
                sample_rate: (*encoder_ctx).sample_rate,
                ch_layout,
                frame: Frame::new()?,
            }))
        }
    }

    pub(super) unsafe fn push(
        &mut self,
        frame: *mut ffi::AVFrame,
    ) -> Result<(), ExportPipelineError> {
        let samples = unsafe { (*frame).nb_samples };
        let written = unsafe {
            ffi::av_audio_fifo_write(
                self.ptr,
                (*frame).data.as_ptr() as *const *mut c_void,
                samples,
            )
        };
        if written == samples {
            Ok(())
        } else if written < 0 {
            Err(ExportPipelineError::Media {
                message: format!("av_audio_fifo_write failed: {}", ffmpeg_error(written)),
            })
        } else {
            Err(media_error(
                "av_audio_fifo_write wrote fewer samples than requested",
            ))
        }
    }

    pub(super) unsafe fn encode_available(
        &mut self,
        encoder_ctx: *mut ffi::AVCodecContext,
        output_ctx: *mut ffi::AVFormatContext,
        output_stream: *mut ffi::AVStream,
        frame_index: &mut i64,
        flush: bool,
    ) -> Result<(), ExportPipelineError> {
        loop {
            let available = unsafe { ffi::av_audio_fifo_size(self.ptr) };
            if available <= 0 || (!flush && available < self.frame_size) {
                break;
            }
            let samples = self.samples_to_encode(available, flush);
            unsafe {
                self.fill_frame_from_fifo(available, samples)?;
                (*self.frame.ptr).pts = *frame_index;
                *frame_index += samples as i64;
                check(
                    ffi::avcodec_send_frame(encoder_ctx, self.frame.ptr),
                    "avcodec_send_frame",
                )?;
                drain_encoder(encoder_ctx, output_ctx, output_stream)?;
                ffi::av_frame_unref(self.frame.ptr);
            }
        }
        Ok(())
    }

    pub(super) fn samples_to_encode(&self, available: i32, flush: bool) -> i32 {
        if flush && self.pad_final_frame && available < self.frame_size {
            self.frame_size
        } else if flush {
            available.min(self.frame_size)
        } else {
            self.frame_size
        }
    }

    pub(super) unsafe fn fill_frame_from_fifo(
        &mut self,
        available: i32,
        samples: i32,
    ) -> Result<(), ExportPipelineError> {
        unsafe {
            ffi::av_frame_unref(self.frame.ptr);
            (*self.frame.ptr).format = self.sample_fmt;
            (*self.frame.ptr).sample_rate = self.sample_rate;
            (*self.frame.ptr).nb_samples = samples;
            check(
                ffi::av_channel_layout_copy(
                    &mut (*self.frame.ptr).ch_layout,
                    &self.ch_layout.inner,
                ),
                "av_channel_layout_copy audio fifo frame",
            )?;
            check(
                ffi::av_frame_get_buffer(self.frame.ptr, 0),
                "av_frame_get_buffer audio fifo frame",
            )?;
            let expected_read = available.min(samples);
            let read = ffi::av_audio_fifo_read(
                self.ptr,
                (*self.frame.ptr).data.as_ptr() as *const *mut c_void,
                expected_read,
            );
            if read != expected_read {
                return Err(audio_fifo_read_error(read));
            }
            if read < samples {
                check(
                    ffi::av_samples_set_silence(
                        (*self.frame.ptr).extended_data,
                        read,
                        samples - read,
                        self.ch_layout.inner.nb_channels,
                        self.sample_fmt,
                    ),
                    "av_samples_set_silence audio fifo padding",
                )?;
            }
        }
        Ok(())
    }
}

impl Drop for AudioFifo {
    fn drop(&mut self) {
        unsafe {
            ffi::av_audio_fifo_free(self.ptr);
        }
    }
}

pub(super) fn audio_fifo_read_error(read: i32) -> ExportPipelineError {
    if read < 0 {
        ExportPipelineError::Media {
            message: format!("av_audio_fifo_read failed: {}", ffmpeg_error(read)),
        }
    } else {
        media_error("av_audio_fifo_read read fewer samples than requested")
    }
}
