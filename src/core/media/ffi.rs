use std::path::Path;
use std::ptr;

use rsmpeg::ffi;

use super::FrameRate;
use crate::core::errors::ExportPipelineError;

mod audio;
mod avio;
mod error;
mod raii;
mod video;

use self::audio::{
    choose_sample_format, convert_hca_bytes_to_audio, resample_audio_frame, AudioFifo,
};
use self::error::{check, media_error, path_cstring, valid_rational};
use self::raii::{CodecContext, Frame, InputContext, OutputContext, Packet};
use self::video::{choose_pixel_format, scale_video_frame};

const AVERROR_EOF: i32 = -541_478_725;

const AVERROR_EAGAIN: i32 = -(ffi::EAGAIN as i32);

pub fn convert_usm_to_mp4(usm_file: &Path, mp4_file: &Path) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    unsafe { transcode_usm_file_to_mp4(usm_file, mp4_file) }
}

pub fn convert_m2v_to_mp4(
    m2v_file: &Path,
    mp4_file: &Path,
    frame_rate: Option<FrameRate>,
) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    unsafe {
        transcode_file_to_file(
            m2v_file,
            Some("mpegvideo"),
            mp4_file,
            OutputCodec::H264,
            frame_rate,
        )
    }
}

pub fn convert_m2v_bytes_to_mp4(
    m2v_bytes: &[u8],
    mp4_file: &Path,
    frame_rate: Option<FrameRate>,
) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    unsafe {
        transcode_memory_to_file(
            m2v_bytes,
            Some("mpegvideo"),
            mp4_file,
            OutputCodec::H264,
            frame_rate,
        )
    }
}

pub fn convert_wav_to_mp3(wav_file: &Path, mp3_file: &Path) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    unsafe { transcode_file_to_file(wav_file, None, mp3_file, OutputCodec::Mp3, None) }
}

pub fn convert_wav_bytes_to_mp3(
    wav_bytes: &[u8],
    mp3_file: &Path,
) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    unsafe { transcode_memory_to_file(wav_bytes, Some("wav"), mp3_file, OutputCodec::Mp3, None) }
}

pub fn convert_hca_bytes_to_mp3(
    hca_bytes: &[u8],
    mp3_file: &Path,
) -> Result<(), ExportPipelineError> {
    convert_hca_bytes_to_audio(hca_bytes, mp3_file, OutputCodec::Mp3)
}

pub fn convert_wav_to_flac(wav_file: &Path, flac_file: &Path) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    unsafe { transcode_file_to_file(wav_file, None, flac_file, OutputCodec::Flac, None) }
}

pub fn convert_wav_bytes_to_flac(
    wav_bytes: &[u8],
    flac_file: &Path,
) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    unsafe { transcode_memory_to_file(wav_bytes, Some("wav"), flac_file, OutputCodec::Flac, None) }
}

pub fn convert_hca_bytes_to_flac(
    hca_bytes: &[u8],
    flac_file: &Path,
) -> Result<(), ExportPipelineError> {
    convert_hca_bytes_to_audio(hca_bytes, flac_file, OutputCodec::Flac)
}

fn ensure_ffmpeg_loaded() -> Result<(), ExportPipelineError> {
    let avformat_version = unsafe { ffi::avformat_version() };
    let avcodec_version = unsafe { ffi::avcodec_version() };
    if avformat_version == 0 || avcodec_version == 0 {
        return Err(ExportPipelineError::Media {
            message: "FFmpeg libraries are unavailable".to_string(),
        });
    }
    unsafe {
        ffi::av_log_set_level(ffi::AV_LOG_ERROR as i32);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum OutputCodec {
    H264,
    Aac,
    Mp3,
    Flac,
}

impl OutputCodec {
    fn codec_id(self) -> ffi::AVCodecID {
        match self {
            Self::H264 => ffi::AV_CODEC_ID_H264,
            Self::Aac => ffi::AV_CODEC_ID_AAC,
            Self::Mp3 => ffi::AV_CODEC_ID_MP3,
            Self::Flac => ffi::AV_CODEC_ID_FLAC,
        }
    }

    fn media_type(self) -> ffi::AVMediaType {
        match self {
            Self::H264 => ffi::AVMEDIA_TYPE_VIDEO,
            Self::Aac | Self::Mp3 | Self::Flac => ffi::AVMEDIA_TYPE_AUDIO,
        }
    }
}

unsafe fn transcode_usm_file_to_mp4(
    input: &Path,
    output: &Path,
) -> Result<(), ExportPipelineError> {
    let input_url = path_cstring(input)?;
    let input_ctx = InputContext::open_file(&input_url, None)?;
    check(
        unsafe { ffi::avformat_find_stream_info(input_ctx.ptr, ptr::null_mut()) },
        "avformat_find_stream_info usm",
    )?;

    let video_stream_index = unsafe { find_best_stream(input_ctx.ptr, ffi::AVMEDIA_TYPE_VIDEO) }?;
    let audio_stream_index =
        unsafe { find_optional_best_stream(input_ctx.ptr, ffi::AVMEDIA_TYPE_AUDIO) };

    let output_url = path_cstring(output)?;
    let mut output_ctx = OutputContext::create(&output_url)?;
    let mut video = unsafe {
        TranscodeStream::new(
            input_ctx.ptr,
            output_ctx.ptr,
            video_stream_index,
            OutputCodec::H264,
            None,
        )
    }?;
    let mut audio = if let Some(index) = audio_stream_index {
        Some(unsafe {
            TranscodeStream::new(input_ctx.ptr, output_ctx.ptr, index, OutputCodec::Aac, None)
        }?)
    } else {
        None
    };

    output_ctx.open_io(&output_url)?;
    check(
        unsafe { ffi::avformat_write_header(output_ctx.ptr, ptr::null_mut()) },
        "avformat_write_header usm",
    )?;

    let packet = Packet::new()?;
    loop {
        let read = unsafe { ffi::av_read_frame(input_ctx.ptr, packet.ptr) };
        if read == AVERROR_EOF {
            break;
        }
        check(read, "av_read_frame usm")?;
        let stream_index = unsafe { (*packet.ptr).stream_index };
        if stream_index == video.input_stream_index {
            unsafe { video.send_packet(output_ctx.ptr, packet.ptr) }?;
        } else if let Some(audio) = audio.as_mut() {
            if stream_index == audio.input_stream_index {
                unsafe { audio.send_packet(output_ctx.ptr, packet.ptr) }?;
            }
        }
        unsafe { ffi::av_packet_unref(packet.ptr) };
    }

    unsafe { video.flush(output_ctx.ptr) }?;
    if let Some(audio) = audio.as_mut() {
        unsafe { audio.flush(output_ctx.ptr) }?;
    }
    check(
        unsafe { ffi::av_write_trailer(output_ctx.ptr) },
        "av_write_trailer usm",
    )?;
    Ok(())
}

unsafe fn transcode_file_to_file(
    input: &Path,
    input_format: Option<&str>,
    output: &Path,
    output_codec: OutputCodec,
    frame_rate: Option<FrameRate>,
) -> Result<(), ExportPipelineError> {
    let input_url = path_cstring(input)?;
    let mut input_ctx = InputContext::open_file(&input_url, input_format)?;
    transcode_open_input_to_file(&mut input_ctx, output, output_codec, frame_rate)
}

unsafe fn transcode_memory_to_file(
    input: &[u8],
    input_format: Option<&str>,
    output: &Path,
    output_codec: OutputCodec,
    frame_rate: Option<FrameRate>,
) -> Result<(), ExportPipelineError> {
    let mut input_ctx = InputContext::open_memory(input, input_format)?;
    transcode_open_input_to_file(&mut input_ctx, output, output_codec, frame_rate)
}

unsafe fn transcode_open_input_to_file(
    input_ctx: &mut InputContext<'_>,
    output: &Path,
    output_codec: OutputCodec,
    frame_rate: Option<FrameRate>,
) -> Result<(), ExportPipelineError> {
    check(
        unsafe { ffi::avformat_find_stream_info(input_ctx.ptr, ptr::null_mut()) },
        "avformat_find_stream_info",
    )?;
    let input_stream_index = find_best_stream(input_ctx.ptr, output_codec.media_type())?;
    let input_stream = unsafe { *(*input_ctx.ptr).streams.add(input_stream_index as usize) };

    let decoder = unsafe { ffi::avcodec_find_decoder((*(*input_stream).codecpar).codec_id) };
    if decoder.is_null() {
        return Err(media_error("could not find FFmpeg decoder"));
    }
    let decoder_ctx = CodecContext::new(decoder)?;
    check(
        unsafe { ffi::avcodec_parameters_to_context(decoder_ctx.ptr, (*input_stream).codecpar) },
        "avcodec_parameters_to_context",
    )?;
    unsafe {
        (*decoder_ctx.ptr).pkt_timebase = (*input_stream).time_base;
    }
    check(
        unsafe { ffi::avcodec_open2(decoder_ctx.ptr, decoder, ptr::null_mut()) },
        "avcodec_open2 decoder",
    )?;

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
    configure_encoder(
        encoder_ctx.ptr,
        encoder,
        decoder_ctx.ptr,
        output_codec,
        output_ctx.ptr,
        frame_rate,
        input_stream,
    )?;
    check(
        unsafe { ffi::avcodec_open2(encoder_ctx.ptr, encoder, ptr::null_mut()) },
        "avcodec_open2 encoder",
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
        "avcodec_parameters_from_context",
    )?;

    output_ctx.open_io(&output_url)?;
    check(
        unsafe { ffi::avformat_write_header(output_ctx.ptr, ptr::null_mut()) },
        "avformat_write_header",
    )?;

    let packet = Packet::new()?;
    let decoded = Frame::new()?;
    let converted = Frame::new()?;
    let mut audio_fifo = AudioFifo::new(encoder_ctx.ptr)?;
    let mut frame_index = 0_i64;

    loop {
        let read = unsafe { ffi::av_read_frame(input_ctx.ptr, packet.ptr) };
        if read == AVERROR_EOF {
            break;
        }
        check(read, "av_read_frame")?;
        if unsafe { (*packet.ptr).stream_index } == input_stream_index {
            send_packet_and_encode(
                decoder_ctx.ptr,
                encoder_ctx.ptr,
                output_ctx.ptr,
                output_stream,
                packet.ptr,
                decoded.ptr,
                converted.ptr,
                &mut audio_fifo,
                &mut frame_index,
            )?;
        }
        unsafe { ffi::av_packet_unref(packet.ptr) };
    }

    check(
        unsafe { ffi::avcodec_send_packet(decoder_ctx.ptr, ptr::null()) },
        "avcodec_send_packet flush",
    )?;
    drain_decoder_to_encoder(
        decoder_ctx.ptr,
        encoder_ctx.ptr,
        output_ctx.ptr,
        output_stream,
        decoded.ptr,
        converted.ptr,
        &mut audio_fifo,
        &mut frame_index,
    )?;
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
        "avcodec_send_frame flush",
    )?;
    drain_encoder(encoder_ctx.ptr, output_ctx.ptr, output_stream)?;
    check(
        unsafe { ffi::av_write_trailer(output_ctx.ptr) },
        "av_write_trailer",
    )?;
    Ok(())
}

struct TranscodeStream {
    input_stream_index: i32,
    decoder_ctx: CodecContext,
    encoder_ctx: CodecContext,
    output_stream: *mut ffi::AVStream,
    decoded: Frame,
    converted: Frame,
    audio_fifo: Option<AudioFifo>,
    frame_index: i64,
}

impl TranscodeStream {
    unsafe fn new(
        input_ctx: *mut ffi::AVFormatContext,
        output_ctx: *mut ffi::AVFormatContext,
        input_stream_index: i32,
        output_codec: OutputCodec,
        frame_rate: Option<FrameRate>,
    ) -> Result<Self, ExportPipelineError> {
        let input_stream = unsafe { *(*input_ctx).streams.add(input_stream_index as usize) };

        let decoder = unsafe { ffi::avcodec_find_decoder((*(*input_stream).codecpar).codec_id) };
        if decoder.is_null() {
            return Err(media_error("could not find FFmpeg decoder"));
        }
        let decoder_ctx = CodecContext::new(decoder)?;
        check(
            unsafe {
                ffi::avcodec_parameters_to_context(decoder_ctx.ptr, (*input_stream).codecpar)
            },
            "avcodec_parameters_to_context usm",
        )?;
        unsafe {
            (*decoder_ctx.ptr).pkt_timebase = (*input_stream).time_base;
        }
        check(
            unsafe { ffi::avcodec_open2(decoder_ctx.ptr, decoder, ptr::null_mut()) },
            "avcodec_open2 decoder usm",
        )?;

        let encoder = unsafe { ffi::avcodec_find_encoder(output_codec.codec_id()) };
        if encoder.is_null() {
            return Err(media_error(&format!(
                "could not find FFmpeg encoder for codec id {}",
                output_codec.codec_id()
            )));
        }
        let encoder_ctx = CodecContext::new(encoder)?;
        configure_encoder(
            encoder_ctx.ptr,
            encoder,
            decoder_ctx.ptr,
            output_codec,
            output_ctx,
            frame_rate,
            input_stream,
        )?;
        check(
            unsafe { ffi::avcodec_open2(encoder_ctx.ptr, encoder, ptr::null_mut()) },
            "avcodec_open2 encoder usm",
        )?;

        let output_stream = unsafe { ffi::avformat_new_stream(output_ctx, ptr::null()) };
        if output_stream.is_null() {
            return Err(media_error("avformat_new_stream usm failed"));
        }
        unsafe {
            (*output_stream).time_base = (*encoder_ctx.ptr).time_base;
        }
        check(
            unsafe {
                ffi::avcodec_parameters_from_context((*output_stream).codecpar, encoder_ctx.ptr)
            },
            "avcodec_parameters_from_context usm",
        )?;

        let decoded = Frame::new()?;
        let converted = Frame::new()?;
        let audio_fifo = AudioFifo::new(encoder_ctx.ptr)?;

        Ok(Self {
            input_stream_index,
            decoder_ctx,
            encoder_ctx,
            output_stream,
            decoded,
            converted,
            audio_fifo,
            frame_index: 0,
        })
    }

    unsafe fn send_packet(
        &mut self,
        output_ctx: *mut ffi::AVFormatContext,
        packet: *mut ffi::AVPacket,
    ) -> Result<(), ExportPipelineError> {
        send_packet_and_encode(
            self.decoder_ctx.ptr,
            self.encoder_ctx.ptr,
            output_ctx,
            self.output_stream,
            packet,
            self.decoded.ptr,
            self.converted.ptr,
            &mut self.audio_fifo,
            &mut self.frame_index,
        )
    }

    unsafe fn flush(
        &mut self,
        output_ctx: *mut ffi::AVFormatContext,
    ) -> Result<(), ExportPipelineError> {
        check(
            unsafe { ffi::avcodec_send_packet(self.decoder_ctx.ptr, ptr::null()) },
            "avcodec_send_packet flush usm",
        )?;
        drain_decoder_to_encoder(
            self.decoder_ctx.ptr,
            self.encoder_ctx.ptr,
            output_ctx,
            self.output_stream,
            self.decoded.ptr,
            self.converted.ptr,
            &mut self.audio_fifo,
            &mut self.frame_index,
        )?;
        if let Some(fifo) = self.audio_fifo.as_mut() {
            fifo.encode_available(
                self.encoder_ctx.ptr,
                output_ctx,
                self.output_stream,
                &mut self.frame_index,
                true,
            )?;
        }
        check(
            unsafe { ffi::avcodec_send_frame(self.encoder_ctx.ptr, ptr::null()) },
            "avcodec_send_frame flush usm",
        )?;
        drain_encoder(self.encoder_ctx.ptr, output_ctx, self.output_stream)?;
        Ok(())
    }
}

unsafe fn configure_encoder(
    encoder_ctx: *mut ffi::AVCodecContext,
    encoder: *const ffi::AVCodec,
    decoder_ctx: *const ffi::AVCodecContext,
    output_codec: OutputCodec,
    output_ctx: *mut ffi::AVFormatContext,
    frame_rate: Option<FrameRate>,
    input_stream: *mut ffi::AVStream,
) -> Result<(), ExportPipelineError> {
    unsafe {
        match output_codec.media_type() {
            ffi::AVMEDIA_TYPE_VIDEO => {
                (*encoder_ctx).height = (*decoder_ctx).height;
                (*encoder_ctx).width = (*decoder_ctx).width;
                (*encoder_ctx).sample_aspect_ratio = (*decoder_ctx).sample_aspect_ratio;
                (*encoder_ctx).pix_fmt = choose_pixel_format(encoder, (*decoder_ctx).pix_fmt)?;
                (*encoder_ctx).bit_rate = 4_000_000;
                (*encoder_ctx).gop_size = 12;
                (*encoder_ctx).max_b_frames = 2;
                let rate = frame_rate
                    .map(|rate| ffi::AVRational {
                        num: rate.numerator,
                        den: rate.denominator,
                    })
                    .or_else(|| valid_rational((*input_stream).avg_frame_rate))
                    .unwrap_or(ffi::AVRational { num: 30, den: 1 });
                (*encoder_ctx).framerate = rate;
                (*encoder_ctx).time_base = ffi::AVRational {
                    num: rate.den,
                    den: rate.num,
                };
            }
            ffi::AVMEDIA_TYPE_AUDIO => {
                (*encoder_ctx).sample_rate = (*decoder_ctx).sample_rate;
                (*encoder_ctx).sample_fmt =
                    choose_sample_format(encoder, (*decoder_ctx).sample_fmt)?;
                ffi::av_channel_layout_copy(
                    &mut (*encoder_ctx).ch_layout,
                    &(*decoder_ctx).ch_layout,
                );
                if (*encoder_ctx).ch_layout.order == ffi::AV_CHANNEL_ORDER_UNSPEC {
                    let channels = (*encoder_ctx).ch_layout.nb_channels;
                    ffi::av_channel_layout_uninit(&mut (*encoder_ctx).ch_layout);
                    ffi::av_channel_layout_default(&mut (*encoder_ctx).ch_layout, channels);
                }
                (*encoder_ctx).time_base = ffi::AVRational {
                    num: 1,
                    den: (*encoder_ctx).sample_rate,
                };
                if matches!(output_codec, OutputCodec::Aac) {
                    (*encoder_ctx).bit_rate = 192_000;
                } else if matches!(output_codec, OutputCodec::Mp3) {
                    (*encoder_ctx).bit_rate = 320_000;
                }
            }
            _ => return Err(media_error("unsupported media type for FFI encoder")),
        }

        if !(*output_ctx).oformat.is_null()
            && ((*(*output_ctx).oformat).flags & ffi::AVFMT_GLOBALHEADER as i32) != 0
        {
            (*encoder_ctx).flags |= ffi::AV_CODEC_FLAG_GLOBAL_HEADER as i32;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
unsafe fn send_packet_and_encode(
    decoder_ctx: *mut ffi::AVCodecContext,
    encoder_ctx: *mut ffi::AVCodecContext,
    output_ctx: *mut ffi::AVFormatContext,
    output_stream: *mut ffi::AVStream,
    packet: *mut ffi::AVPacket,
    decoded: *mut ffi::AVFrame,
    converted: *mut ffi::AVFrame,
    audio_fifo: &mut Option<AudioFifo>,
    frame_index: &mut i64,
) -> Result<(), ExportPipelineError> {
    check(
        unsafe { ffi::avcodec_send_packet(decoder_ctx, packet) },
        "avcodec_send_packet",
    )?;
    drain_decoder_to_encoder(
        decoder_ctx,
        encoder_ctx,
        output_ctx,
        output_stream,
        decoded,
        converted,
        audio_fifo,
        frame_index,
    )
}

#[allow(clippy::too_many_arguments)]
unsafe fn drain_decoder_to_encoder(
    decoder_ctx: *mut ffi::AVCodecContext,
    encoder_ctx: *mut ffi::AVCodecContext,
    output_ctx: *mut ffi::AVFormatContext,
    output_stream: *mut ffi::AVStream,
    decoded: *mut ffi::AVFrame,
    converted: *mut ffi::AVFrame,
    audio_fifo: &mut Option<AudioFifo>,
    frame_index: &mut i64,
) -> Result<(), ExportPipelineError> {
    loop {
        let ret = unsafe { ffi::avcodec_receive_frame(decoder_ctx, decoded) };
        if ret == AVERROR_EAGAIN || ret == AVERROR_EOF {
            break;
        }
        check(ret, "avcodec_receive_frame")?;
        let frame =
            prepare_frame_for_encoder(decoder_ctx, encoder_ctx, decoded, converted, frame_index)?;
        if let Some(fifo) = audio_fifo.as_mut() {
            fifo.push(frame)?;
            fifo.encode_available(encoder_ctx, output_ctx, output_stream, frame_index, false)?;
        } else {
            unsafe {
                if (*encoder_ctx).codec_type == ffi::AVMEDIA_TYPE_AUDIO {
                    (*frame).pts = *frame_index;
                    *frame_index += (*frame).nb_samples as i64;
                }
            }
            check(
                unsafe { ffi::avcodec_send_frame(encoder_ctx, frame) },
                "avcodec_send_frame",
            )?;
            drain_encoder(encoder_ctx, output_ctx, output_stream)?;
        }
        unsafe { ffi::av_frame_unref(decoded) };
        unsafe { ffi::av_frame_unref(converted) };
    }
    Ok(())
}

unsafe fn prepare_frame_for_encoder(
    decoder_ctx: *mut ffi::AVCodecContext,
    encoder_ctx: *mut ffi::AVCodecContext,
    decoded: *mut ffi::AVFrame,
    converted: *mut ffi::AVFrame,
    frame_index: &mut i64,
) -> Result<*mut ffi::AVFrame, ExportPipelineError> {
    unsafe {
        let codec_type = (*encoder_ctx).codec_type;
        if codec_type == ffi::AVMEDIA_TYPE_VIDEO {
            let needs_scale = (*decoded).format != (*encoder_ctx).pix_fmt
                || (*decoded).width != (*encoder_ctx).width
                || (*decoded).height != (*encoder_ctx).height;
            let frame = if needs_scale {
                scale_video_frame(decoder_ctx, encoder_ctx, decoded, converted)?
            } else {
                decoded
            };
            (*frame).pts = *frame_index;
            *frame_index += 1;
            Ok(frame)
        } else if codec_type == ffi::AVMEDIA_TYPE_AUDIO {
            let needs_resample = (*decoded).format != (*encoder_ctx).sample_fmt
                || (*decoded).sample_rate != (*encoder_ctx).sample_rate
                || ffi::av_channel_layout_compare(&(*decoded).ch_layout, &(*encoder_ctx).ch_layout)
                    != 0;
            let frame = if needs_resample {
                resample_audio_frame(encoder_ctx, decoded, converted)?
            } else {
                decoded
            };
            Ok(frame)
        } else {
            Err(media_error("unsupported decoded frame type"))
        }
    }
}

unsafe fn drain_encoder(
    encoder_ctx: *mut ffi::AVCodecContext,
    output_ctx: *mut ffi::AVFormatContext,
    output_stream: *mut ffi::AVStream,
) -> Result<(), ExportPipelineError> {
    let encoded = Packet::new()?;
    loop {
        let ret = unsafe { ffi::avcodec_receive_packet(encoder_ctx, encoded.ptr) };
        if ret == AVERROR_EAGAIN || ret == AVERROR_EOF {
            break;
        }
        check(ret, "avcodec_receive_packet")?;
        unsafe {
            ffi::av_packet_rescale_ts(
                encoded.ptr,
                (*encoder_ctx).time_base,
                (*output_stream).time_base,
            );
            (*encoded.ptr).stream_index = (*output_stream).index;
        }
        check(
            unsafe { ffi::av_interleaved_write_frame(output_ctx, encoded.ptr) },
            "av_interleaved_write_frame",
        )?;
        unsafe { ffi::av_packet_unref(encoded.ptr) };
    }
    Ok(())
}

unsafe fn find_best_stream(
    ctx: *mut ffi::AVFormatContext,
    media_type: ffi::AVMediaType,
) -> Result<i32, ExportPipelineError> {
    let index = unsafe { ffi::av_find_best_stream(ctx, media_type, -1, -1, ptr::null_mut(), 0) };
    if index < 0 {
        Err(media_error(&format!(
            "could not find FFmpeg stream type {media_type}"
        )))
    } else {
        Ok(index)
    }
}

unsafe fn find_optional_best_stream(
    ctx: *mut ffi::AVFormatContext,
    media_type: ffi::AVMediaType,
) -> Option<i32> {
    let index = unsafe { ffi::av_find_best_stream(ctx, media_type, -1, -1, ptr::null_mut(), 0) };
    (index >= 0).then_some(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::raii::ChannelLayout;

    /// The owning wrappers exist so an early return frees what was allocated.
    /// Allocating and dropping them in a loop is what turns a double free or a
    /// missing free into a visible failure rather than a slow leak in
    /// production.
    #[test]
    fn allocation_wrappers_allocate_and_free_repeatedly() {
        for _ in 0..64 {
            let packet = Packet::new().unwrap();
            assert!(!packet.ptr.is_null());
            let frame = Frame::new().unwrap();
            assert!(!frame.ptr.is_null());
        }
    }

    #[test]
    fn channel_layouts_are_built_for_valid_counts_only() {
        for channels in [1, 2, 6] {
            let layout = ChannelLayout::default_for_channels(channels).unwrap();
            assert_eq!(layout.inner.nb_channels, channels);
        }
        assert!(ChannelLayout::default_for_channels(0).is_err());
        assert!(ChannelLayout::default_for_channels(-2).is_err());
    }

    /// `default_or_copy` has two branches: an unspecified order is replaced by
    /// FFmpeg's default for that channel count, and a specified one is copied.
    /// Getting the first wrong yields a layout with no channels, which fails
    /// much later inside the encoder.
    #[test]
    fn unspecified_channel_order_falls_back_to_the_default_layout() {
        let mut unspecified = unsafe { std::mem::zeroed::<ffi::AVChannelLayout>() };
        unspecified.order = ffi::AV_CHANNEL_ORDER_UNSPEC;
        unspecified.nb_channels = 2;
        let copied = ChannelLayout::default_or_copy(&unspecified).unwrap();
        assert_eq!(copied.inner.nb_channels, 2);
        assert_ne!(copied.inner.order, ffi::AV_CHANNEL_ORDER_UNSPEC);

        let stereo = ChannelLayout::default_for_channels(2).unwrap();
        let copied = ChannelLayout::default_or_copy(&stereo.inner).unwrap();
        assert_eq!(copied.inner.nb_channels, 2);
        assert_eq!(copied.inner.order, stereo.inner.order);
    }

    /// A minimal 16-bit PCM WAV, synthesised so these need no fixture and run
    /// wherever FFmpeg 7 is -- CI included.
    fn sine_wav(seconds: f32, sample_rate: u32, channels: u16) -> Vec<u8> {
        let frames = (seconds * sample_rate as f32) as u32;
        let data_len = frames * u32::from(channels) * 2;
        let mut wav = Vec::with_capacity(44 + data_len as usize);
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + data_len).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&channels.to_le_bytes());
        wav.extend_from_slice(&sample_rate.to_le_bytes());
        wav.extend_from_slice(&(sample_rate * u32::from(channels) * 2).to_le_bytes());
        wav.extend_from_slice(&(channels * 2).to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_len.to_le_bytes());
        for frame in 0..frames {
            let t = frame as f32 / sample_rate as f32;
            let sample = ((t * 440.0 * std::f32::consts::TAU).sin() * 8000.0) as i16;
            for _ in 0..channels {
                wav.extend_from_slice(&sample.to_le_bytes());
            }
        }
        wav
    }

    /// The MP3 side of this is covered in `core::media`; FLAC takes a different
    /// encoder and a different sample format, so it reaches
    /// `choose_sample_format` and the resampler along a path MP3 never uses.
    #[test]
    fn wav_bytes_encode_to_flac() {
        let dir = tempfile::tempdir().unwrap();
        let flac = dir.path().join("out.flac");

        convert_wav_bytes_to_flac(&sine_wav(0.25, 44_100, 2), &flac).unwrap();

        let written = std::fs::read(&flac).unwrap();
        assert!(
            written.starts_with(b"fLaC"),
            "not FLAC: {:02x?}",
            &written[..4]
        );
    }

    /// Mono at a rate no MP3 encoder takes natively, so both the resampler and
    /// the channel-layout conversion have to do real work rather than passing
    /// the frames through.
    #[test]
    fn mono_at_an_unsupported_rate_is_resampled() {
        let dir = tempfile::tempdir().unwrap();
        let mp3 = dir.path().join("mono.mp3");

        convert_wav_bytes_to_mp3(&sine_wav(0.25, 11_025, 1), &mp3).unwrap();

        assert!(std::fs::metadata(&mp3).unwrap().len() > 256);
    }

    /// Input that is not audio must come back as an error, not a panic and not
    /// a truncated file. This path is all `unsafe`; a wrong turn is a crash in
    /// production rather than a failed job.
    #[test]
    fn garbage_input_is_rejected_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("nope.mp3");

        let error = convert_wav_bytes_to_mp3(b"this is not a wav file at all", &out).unwrap_err();

        assert!(!error.to_string().is_empty());
        assert!(!out.exists(), "a failed conversion must not leave a file");
    }
}
