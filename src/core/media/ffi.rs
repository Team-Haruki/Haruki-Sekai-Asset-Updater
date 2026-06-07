use std::ffi::{c_char, c_void, CStr, CString};
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::ptr;

use cridecoder::HcaDecoder;
use rsmpeg::ffi;

use super::FrameRate;
use crate::core::codec;
use crate::core::errors::ExportPipelineError;

const AVERROR_EOF: i32 = -541_478_725;
const AVERROR_EAGAIN: i32 = -(ffi::EAGAIN as i32);

pub fn convert_usm_to_mp4(usm_file: &Path, mp4_file: &Path) -> Result<(), ExportPipelineError> {
    ensure_ffmpeg_loaded()?;
    let usm_bytes = fs::read(usm_file).map_err(|source| ExportPipelineError::Io {
        path: usm_file.to_path_buf(),
        source,
    })?;
    let fallback_name = usm_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("input.usm");
    let streams = codec::export_usm_to_memory(&usm_bytes, fallback_name.as_bytes(), true)?;
    let has_audio = streams
        .iter()
        .any(|stream| !stream.extension.eq_ignore_ascii_case("m2v"));
    if has_audio {
        return Err(ExportPipelineError::Media {
            message: "USM contains audio streams; FFmpeg FFI USM muxing is not implemented yet"
                .to_string(),
        });
    }
    let video = streams
        .into_iter()
        .find(|stream| stream.extension.eq_ignore_ascii_case("m2v"))
        .ok_or_else(|| ExportPipelineError::Media {
            message: format!(
                "USM did not contain an M2V video stream: {}",
                usm_file.display()
            ),
        })?;
    unsafe {
        transcode_memory_to_file(
            &video.data,
            Some("mpegvideo"),
            mp4_file,
            OutputCodec::H264,
            None,
        )
    }
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
    Ok(())
}

fn convert_hca_bytes_to_audio(
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

#[derive(Debug, Clone, Copy)]
enum OutputCodec {
    H264,
    Mp3,
    Flac,
}

impl OutputCodec {
    fn codec_id(self) -> ffi::AVCodecID {
        match self {
            Self::H264 => ffi::AV_CODEC_ID_H264,
            Self::Mp3 => ffi::AV_CODEC_ID_MP3,
            Self::Flac => ffi::AV_CODEC_ID_FLAC,
        }
    }

    fn media_type(self) -> ffi::AVMediaType {
        match self {
            Self::H264 => ffi::AVMEDIA_TYPE_VIDEO,
            Self::Mp3 | Self::Flac => ffi::AVMEDIA_TYPE_AUDIO,
        }
    }
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
    let mut input_ctx = InputContext::open_memory(input.to_vec(), input_format)?;
    transcode_open_input_to_file(&mut input_ctx, output, output_codec, frame_rate)
}

unsafe fn transcode_open_input_to_file(
    input_ctx: &mut InputContext,
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

unsafe fn encode_pcm16_to_file<F>(
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

unsafe fn configure_pcm16_encoder(
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

struct Pcm16Encoder<'a> {
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
    unsafe fn encode_samples(&mut self, samples: &[i16]) -> Result<(), ExportPipelineError> {
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
                if matches!(output_codec, OutputCodec::Mp3) {
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

unsafe fn resample_audio_frame(
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

unsafe fn scale_video_frame(
    decoder_ctx: *mut ffi::AVCodecContext,
    encoder_ctx: *mut ffi::AVCodecContext,
    decoded: *mut ffi::AVFrame,
    converted: *mut ffi::AVFrame,
) -> Result<*mut ffi::AVFrame, ExportPipelineError> {
    unsafe {
        (*converted).format = (*encoder_ctx).pix_fmt;
        (*converted).width = (*encoder_ctx).width;
        (*converted).height = (*encoder_ctx).height;
        check(
            ffi::av_frame_get_buffer(converted, 0),
            "av_frame_get_buffer",
        )?;
        let sws = ffi::sws_getContext(
            (*decoder_ctx).width,
            (*decoder_ctx).height,
            (*decoded).format,
            (*encoder_ctx).width,
            (*encoder_ctx).height,
            (*encoder_ctx).pix_fmt,
            ffi::SWS_BILINEAR as i32,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null(),
        );
        if sws.is_null() {
            return Err(media_error("sws_getContext failed"));
        }
        ffi::sws_scale(
            sws,
            (*decoded).data.as_ptr() as *const *const u8,
            (*decoded).linesize.as_ptr(),
            0,
            (*decoder_ctx).height,
            (*converted).data.as_mut_ptr(),
            (*converted).linesize.as_mut_ptr(),
        );
        ffi::sws_freeContext(sws);
        Ok(converted)
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

unsafe fn choose_pixel_format(
    codec: *const ffi::AVCodec,
    decoder_format: ffi::AVPixelFormat,
) -> Result<ffi::AVPixelFormat, ExportPipelineError> {
    unsafe {
        if (*codec).pix_fmts.is_null() {
            return Ok(decoder_format);
        }
        let mut cursor = (*codec).pix_fmts;
        while *cursor != ffi::AV_PIX_FMT_NONE {
            if *cursor == decoder_format {
                return Ok(decoder_format);
            }
            cursor = cursor.add(1);
        }
        Ok(*(*codec).pix_fmts)
    }
}

unsafe fn choose_sample_format(
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

fn valid_rational(value: ffi::AVRational) -> Option<ffi::AVRational> {
    (value.num > 0 && value.den > 0).then_some(value)
}

fn path_cstring(path: &Path) -> Result<CString, ExportPipelineError> {
    CString::new(path.to_string_lossy().as_bytes()).map_err(|err| ExportPipelineError::Media {
        message: format!("path contains NUL byte: {err}"),
    })
}

fn cstring(value: &str) -> Result<CString, ExportPipelineError> {
    CString::new(value).map_err(|err| ExportPipelineError::Media {
        message: format!("FFmpeg string contains NUL byte: {err}"),
    })
}

fn check(ret: i32, operation: &str) -> Result<(), ExportPipelineError> {
    if ret >= 0 {
        Ok(())
    } else {
        Err(ExportPipelineError::Media {
            message: format!("{operation} failed: {}", ffmpeg_error(ret)),
        })
    }
}

fn ffmpeg_error(code: i32) -> String {
    let mut buf = [0 as c_char; 128];
    unsafe {
        if ffi::av_strerror(code, buf.as_mut_ptr(), buf.len()) == 0 {
            CStr::from_ptr(buf.as_ptr()).to_string_lossy().into_owned()
        } else {
            format!("FFmpeg error {code}")
        }
    }
}

fn media_error(message: &str) -> ExportPipelineError {
    ExportPipelineError::Media {
        message: message.to_string(),
    }
}

struct InputContext {
    ptr: *mut ffi::AVFormatContext,
    avio: Option<CustomAvio>,
}

impl InputContext {
    unsafe fn open_file(
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

    unsafe fn open_memory(
        data: Vec<u8>,
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

impl Drop for InputContext {
    fn drop(&mut self) {
        unsafe {
            ffi::avformat_close_input(&mut self.ptr);
        }
        let _ = self.avio.take();
    }
}

struct OutputContext {
    ptr: *mut ffi::AVFormatContext,
    io_opened: bool,
}

impl OutputContext {
    unsafe fn create(url: &CStr) -> Result<Self, ExportPipelineError> {
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

    unsafe fn open_io(&mut self, url: &CStr) -> Result<(), ExportPipelineError> {
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

struct CodecContext {
    ptr: *mut ffi::AVCodecContext,
}

impl CodecContext {
    fn new(codec: *const ffi::AVCodec) -> Result<Self, ExportPipelineError> {
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

struct SwrContext {
    ptr: *mut ffi::SwrContext,
}

impl SwrContext {
    fn new(
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

struct ChannelLayout {
    inner: ffi::AVChannelLayout,
}

impl ChannelLayout {
    fn default_for_channels(channels: i32) -> Result<Self, ExportPipelineError> {
        if channels <= 0 {
            return Err(media_error("invalid audio channel count"));
        }
        let mut inner = unsafe { std::mem::zeroed::<ffi::AVChannelLayout>() };
        unsafe {
            ffi::av_channel_layout_default(&mut inner, channels);
        }
        Ok(Self { inner })
    }

    fn default_or_copy(source: *const ffi::AVChannelLayout) -> Result<Self, ExportPipelineError> {
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

struct AudioFifo {
    ptr: *mut ffi::AVAudioFifo,
    frame_size: i32,
    pad_final_frame: bool,
    sample_fmt: ffi::AVSampleFormat,
    sample_rate: i32,
    ch_layout: ChannelLayout,
    frame: Frame,
}

impl AudioFifo {
    fn new(encoder_ctx: *mut ffi::AVCodecContext) -> Result<Option<Self>, ExportPipelineError> {
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

    unsafe fn push(&mut self, frame: *mut ffi::AVFrame) -> Result<(), ExportPipelineError> {
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

    unsafe fn encode_available(
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
            let samples = if flush && self.pad_final_frame && available < self.frame_size {
                self.frame_size
            } else if flush {
                available.min(self.frame_size)
            } else {
                self.frame_size
            };
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
                let read = ffi::av_audio_fifo_read(
                    self.ptr,
                    (*self.frame.ptr).data.as_ptr() as *const *mut c_void,
                    available.min(samples),
                );
                let expected_read = available.min(samples);
                if read != expected_read {
                    if read < 0 {
                        return Err(ExportPipelineError::Media {
                            message: format!("av_audio_fifo_read failed: {}", ffmpeg_error(read)),
                        });
                    }
                    return Err(media_error(
                        "av_audio_fifo_read read fewer samples than requested",
                    ));
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
}

impl Drop for AudioFifo {
    fn drop(&mut self) {
        unsafe {
            ffi::av_audio_fifo_free(self.ptr);
        }
    }
}

struct Packet {
    ptr: *mut ffi::AVPacket,
}

impl Packet {
    fn new() -> Result<Self, ExportPipelineError> {
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

struct Frame {
    ptr: *mut ffi::AVFrame,
}

impl Frame {
    fn new() -> Result<Self, ExportPipelineError> {
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

struct CustomAvio {
    ctx: *mut ffi::AVIOContext,
    opaque: *mut MemoryInput,
}

impl CustomAvio {
    fn new(data: Vec<u8>) -> Result<Self, ExportPipelineError> {
        let buffer_size = 32 * 1024;
        let buffer = unsafe { ffi::av_malloc(buffer_size) as *mut u8 };
        if buffer.is_null() {
            return Err(media_error("av_malloc failed for AVIO buffer"));
        }
        let opaque = Box::into_raw(Box::new(MemoryInput { data, position: 0 }));
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
        Ok(Self { ctx, opaque })
    }
}

impl Drop for CustomAvio {
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
    data: Vec<u8>,
    position: usize,
}

unsafe extern "C" fn read_memory_packet(opaque: *mut c_void, buf: *mut u8, buf_size: i32) -> i32 {
    let input = unsafe { &mut *(opaque as *mut MemoryInput) };
    if input.position >= input.data.len() {
        return AVERROR_EOF;
    }
    let remaining = input.data.len() - input.position;
    let len = remaining.min(buf_size as usize);
    unsafe {
        ptr::copy_nonoverlapping(input.data.as_ptr().add(input.position), buf, len);
    }
    input.position += len;
    len as i32
}

unsafe extern "C" fn seek_memory(opaque: *mut c_void, offset: i64, whence: i32) -> i64 {
    let input = unsafe { &mut *(opaque as *mut MemoryInput) };
    if whence == ffi::AVSEEK_SIZE as i32 {
        return input.data.len() as i64;
    }
    let base = match whence {
        libc::SEEK_SET => 0_i64,
        libc::SEEK_CUR => input.position as i64,
        libc::SEEK_END => input.data.len() as i64,
        _ => return -1,
    };
    let Some(position) = base.checked_add(offset) else {
        return -1;
    };
    if position < 0 || position as usize > input.data.len() {
        return -1;
    }
    input.position = position as usize;
    position
}

fn input_format_ptr(format: Option<&str>) -> Result<*mut ffi::AVInputFormat, ExportPipelineError> {
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
