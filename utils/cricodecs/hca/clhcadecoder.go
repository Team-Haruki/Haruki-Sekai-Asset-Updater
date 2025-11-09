package hca

import "errors"

// Decoding step 1: Unpack scaleFactors
func unpackScaleFactors(ch *stChannel, br *clData, hfrGroupCount uint, version uint) error {
	csCount := ch.codedCount
	var extraCount uint

	deltaBits := byte(bitreaderRead(br, 3))

	if ch.channelType == stereoSecondary || hfrGroupCount <= 0 || version <= hcaVersion200 {
		extraCount = 0
	} else {
		extraCount = hfrGroupCount
		csCount = csCount + extraCount

		if csCount > hcaSamplesPerSubframe {
			return errors.New("invalid coded count")
		}
	}

	if deltaBits >= 6 {
		// Fixed scaleFactors
		for i := uint(0); i < csCount; i++ {
			ch.scaleFactors[i] = byte(bitreaderRead(br, 6))
		}
	} else if deltaBits > 0 {
		// Delta scaleFactors
		expectedDelta := byte((1 << deltaBits) - 1)
		value := byte(bitreaderRead(br, 6))

		ch.scaleFactors[0] = value
		for i := uint(1); i < csCount; i++ {
			delta := byte(bitreaderRead(br, int(deltaBits)))

			if delta == expectedDelta {
				value = byte(bitreaderRead(br, 6))
			} else {
				scalefactorTest := int(value) + (int(delta) - int(expectedDelta>>1))
				if scalefactorTest < 0 || scalefactorTest >= 64 {
					return errors.New("invalid scalefactor")
				}

				value = value - (expectedDelta >> 1) + delta
				value = value & 0x3F
			}
			ch.scaleFactors[i] = value
		}
	} else {
		// No scaleFactors
		for i := 0; i < hcaSamplesPerSubframe; i++ {
			ch.scaleFactors[i] = 0
		}
	}

	// Set derived HFR scales for v3.0
	for i := uint(0); i < extraCount; i++ {
		ch.scaleFactors[hcaSamplesPerSubframe-1-i] = ch.scaleFactors[csCount-i]
	}

	return nil
}

// Unpack intensity
func unpackIntensity(ch *stChannel, br *clData, hfrGroupCount uint, version uint) error {
	if ch.channelType == stereoSecondary {
		if version <= hcaVersion200 {
			value := byte(bitreaderPeek(br, 4))

			ch.intensity[0] = value
			if value < 15 {
				bitreaderSkip(br, 4)
				for i := 1; i < hcaSubframes; i++ {
					ch.intensity[i] = byte(bitreaderRead(br, 4))
				}
			}
		} else {
			value := byte(bitreaderPeek(br, 4))

			if value < 15 {
				bitreaderSkip(br, 4)

				deltaBits := byte(bitreaderRead(br, 2))

				ch.intensity[0] = value
				if deltaBits == 3 {
					// Fixed intensities
					for i := 1; i < hcaSubframes; i++ {
						ch.intensity[i] = byte(bitreaderRead(br, 4))
					}
				} else {
					// Delta intensities
					bmax := byte((2 << deltaBits) - 1)
					bits := deltaBits + 1

					for i := 1; i < hcaSubframes; i++ {
						delta := byte(bitreaderRead(br, int(bits)))
						if delta == bmax {
							value = byte(bitreaderRead(br, 4))
						} else {
							value = value - (bmax >> 1) + delta
							if value > 15 {
								return errors.New("invalid intensity")
							}
						}

						ch.intensity[i] = value
					}
				}
			} else {
				bitreaderSkip(br, 4)
				for i := 0; i < hcaSubframes; i++ {
					ch.intensity[i] = 7
				}
			}
		}
	} else {
		if version <= hcaVersion200 {
			hfrScales := ch.scaleFactors[128-hfrGroupCount:]
			for i := uint(0); i < hfrGroupCount; i++ {
				hfrScales[i] = byte(bitreaderRead(br, 6))
			}
		}
	}

	return nil
}

// Calculate resolution
func calculateResolution(ch *stChannel, packedNoiseLevel uint, athCurve *[hcaSamplesPerSubframe]byte,
	minResolution, maxResolution uint) {
	crCount := ch.codedCount
	noiseCount := uint(0)
	validCount := uint(0)

	for i := uint(0); i < crCount; i++ {
		newResolution := byte(0)
		scalefactor := ch.scaleFactors[i]

		if scalefactor > 0 {
			noiseLevel := int(athCurve[i]) + ((int(packedNoiseLevel) + int(i)) >> 8)
			curvePosition := noiseLevel + 1 - ((5 * int(scalefactor)) >> 1)

			if curvePosition < 0 {
				newResolution = 15
			} else if curvePosition <= 65 {
				newResolution = hcadecoderInvertTable[curvePosition]
			} else {
				newResolution = 0
			}

			if newResolution > byte(maxResolution) {
				newResolution = byte(maxResolution)
			} else if newResolution < byte(minResolution) {
				newResolution = byte(minResolution)
			}

			if newResolution < 1 {
				ch.noises[noiseCount] = byte(i)
				noiseCount++
			} else {
				ch.noises[hcaSamplesPerSubframe-1-validCount] = byte(i)
				validCount++
			}
		}
		ch.resolution[i] = newResolution
	}

	ch.noiseCount = noiseCount
	ch.validCount = validCount

	for i := crCount; i < hcaSamplesPerSubframe; i++ {
		ch.resolution[i] = 0
	}
}

// Calculate gain
func calculateGain(ch *stChannel) {
	cgCount := ch.codedCount

	for i := uint(0); i < cgCount; i++ {
		scalefactorScale := hcadequantizerScalingTableFloat[ch.scaleFactors[i]]
		resolutionScale := hcadequantizerRangeTableFloat[ch.resolution[i]]
		ch.gain[i] = scalefactorScale * resolutionScale
	}
}

// Decoding step 2: Dequantize coefficients
func dequantizeCoefficients(ch *stChannel, br *clData, subframe int) {
	ccCount := ch.codedCount

	for i := uint(0); i < ccCount; i++ {
		var qc float32
		resolution := ch.resolution[i]
		bits := hcatbdecoderMaxBitTable[resolution]
		code := bitreaderRead(br, int(bits))

		if resolution > 7 {
			// Sign-magnitude form
			signedCode := int((1 - ((code & 1) << 1)) * (code >> 1))
			if signedCode == 0 {
				bitreaderSkip(br, -1)
			}
			qc = float32(signedCode)
		} else {
			// Prefix codebooks
			index := (uint(resolution) << 4) + code
			skip := int(hcatbdecoderReadBitTable[index]) - int(bits)
			bitreaderSkip(br, skip)
			qc = hcatbdecoderReadValTable[index]
		}

		ch.spectra[subframe][i] = ch.gain[i] * qc
	}

	// Clean rest of spectra
	for i := ccCount; i < hcaSamplesPerSubframe; i++ {
		ch.spectra[subframe][i] = 0
	}
}

// Decoding step 3: Reconstruct noise
func reconstructNoise(ch *stChannel, minResolution, msStereo uint, random *uint, subframe int) {
	if minResolution > 0 {
		return
	}
	if ch.validCount <= 0 || ch.noiseCount <= 0 {
		return
	}
	if !(msStereo == 0 || ch.channelType == stereoPrimary) {
		return
	}

	r := *random

	for i := uint(0); i < ch.noiseCount; i++ {
		r = 0x343FD*r + 0x269EC3

		randomIndex := hcaSamplesPerSubframe - ch.validCount + (((r & 0x7FFF) * ch.validCount) >> 15)

		noiseIndex := ch.noises[i]
		validIndex := ch.noises[randomIndex]

		sfNoise := ch.scaleFactors[noiseIndex]
		sfValid := ch.scaleFactors[validIndex]
		scIndex := int(sfNoise) - int(sfValid) + 62
		if scIndex < 0 {
			scIndex = 0
		}

		ch.spectra[subframe][noiseIndex] =
			hcadecoderScaleConversionTable[scIndex] * ch.spectra[subframe][validIndex]
	}

	*random = r
}

// Reconstruct high frequency
func reconstructHighFrequency(ch *stChannel, hfrGroupCount, bandsPerHfrGroup,
	stereoBandCount, baseBandCount, totalBandCount, version uint, subframe int) {
	if bandsPerHfrGroup == 0 {
		return
	}
	if ch.channelType == stereoSecondary {
		return
	}

	var groupLimit uint
	startBand := stereoBandCount + baseBandCount
	highband := startBand
	lowband := startBand - 1
	hfrScales := ch.scaleFactors[128-hfrGroupCount:]

	if version <= hcaVersion200 {
		groupLimit = hfrGroupCount
	} else {
		if int(hfrGroupCount) >= 0 {
			groupLimit = hfrGroupCount
		} else {
			groupLimit = hfrGroupCount + 1
		}
		groupLimit = groupLimit >> 1
	}

	for group := uint(0); group < hfrGroupCount; group++ {
		lowbandSub := uint(1)
		if group >= groupLimit {
			lowbandSub = 0
		}

		for i := uint(0); i < bandsPerHfrGroup; i++ {
			if highband >= totalBandCount || lowband < 0 {
				break
			}

			scIndex := int(hfrScales[group]) - int(ch.scaleFactors[lowband]) + 63
			if scIndex < 0 {
				scIndex = 0
			}

			ch.spectra[subframe][highband] = hcadecoderScaleConversionTable[scIndex] * ch.spectra[subframe][lowband]

			highband++
			lowband -= lowbandSub
		}
	}

	ch.spectra[subframe][highband-1] = 0.0
}

// Decoding step 4: Apply intensity stereo
func applyIntensityStereo(chPair *[hcaMaxChannels]stChannel, chIdx uint, subframe int,
	baseBandCount, totalBandCount uint) {
	if chPair[chIdx].channelType != stereoPrimary {
		return
	}

	ratioL := hcadecoderIntensityRatioTable[chPair[chIdx+1].intensity[subframe]]
	ratioR := 2.0 - ratioL
	spL := chPair[chIdx].spectra[subframe][:]
	spR := chPair[chIdx+1].spectra[subframe][:]

	for band := baseBandCount; band < totalBandCount; band++ {
		coefL := spL[band] * ratioL
		coefR := spL[band] * ratioR
		spL[band] = coefL
		spR[band] = coefR
	}
}

// Apply MS stereo
func applyMsStereo(chPair *[hcaMaxChannels]stChannel, chIdx uint, msStereo uint, subframe int,
	baseBandCount, totalBandCount uint) {
	if msStereo == 0 {
		return
	}
	if chPair[chIdx].channelType != stereoPrimary {
		return
	}

	const ratio = float32(0.70710676908493)
	spL := chPair[chIdx].spectra[subframe][:]
	spR := chPair[chIdx+1].spectra[subframe][:]

	for band := baseBandCount; band < totalBandCount; band++ {
		coefL := (spL[band] + spR[band]) * ratio
		coefR := (spL[band] - spR[band]) * ratio
		spL[band] = coefL
		spR[band] = coefR
	}
}

// DecodeBlock unpacks and transforms a frame
func (hca *ClHCA) DecodeBlock(data []byte) error {
	res := hca.decodeBlockUnpack(data)
	if res < 0 {
		return errors.New("unpack failed")
	}
	hca.decodeBlockTransform()
	return nil
}

// TestBlock tests if a block decodes correctly (for key testing)
func (hca *ClHCA) TestBlock(data []byte) int {
	if isEmptyBlock(data) {
		return 0
	}

	status := hca.decodeBlockUnpack(data)
	if status < 0 {
		return -1
	}

	if errCode := validateBitreader(data, status, int(hca.frameSize)); errCode != 0 {
		return errCode
	}

	hca.decodeBlockTransform()

	return evaluateDecodeQuality(hca)
}

func isEmptyBlock(data []byte) bool {
	for i := 0x02; i < len(data)-0x02; i++ {
		if data[i] != 0 {
			return false
		}
	}
	return true
}

func validateBitreader(data []byte, status, frameSize int) int {
	bitsMax := frameSize * 8
	if status+14 > bitsMax {
		return hcaErrorBitreader
	}

	byteStart := status / 8
	if status%8 != 0 {
		byteStart++
	}
	for i := byteStart; i < frameSize-0x02; i++ {
		if data[i] != 0 {
			return -1
		}
	}

	return 0
}

func evaluateDecodeQuality(hca *ClHCA) int {
	const framesamples = hcaSubframes * hcaSamplesPerSubframe
	const scale = 32768.0

	clips := 0
	blanks := 0
	channelBlanks := make([]int, hcaMaxChannels)

	for ch := uint(0); ch < hca.channels; ch++ {
		for sf := 0; sf < hcaSubframes; sf++ {
			for s := 0; s < hcaSamplesPerSubframe; s++ {
				fsample := hca.channel[ch].wave[sf][s]

				if fsample > 1.0 || fsample < -1.0 {
					clips++
				} else {
					psample := int32(fsample * scale)
					if psample == 0 || psample == -1 {
						blanks++
						channelBlanks[ch]++
					}
				}
			}
		}
	}

	return calculateScore(clips, blanks, channelBlanks, framesamples, hca.channels)
}

func calculateScore(clips, blanks int, channelBlanks []int, framesamples int, channels uint) int {
	if clips == 1 {
		clips++
	}
	if clips > 1 {
		return clips
	}

	if blanks == int(channels)*framesamples {
		return 0
	}

	if channels >= 2 {
		if channelBlanks[0] == framesamples && channelBlanks[1] != framesamples {
			return 3
		}
	}

	return 1
}

func (hca *ClHCA) decodeBlockUnpack(data []byte) int {
	if !hca.isValid {
		return hcaErrorParams
	}
	if len(data) < int(hca.frameSize) {
		return hcaErrorParams
	}

	br := &clData{}
	bitreaderInit(br, data, int(hca.frameSize))

	// Test sync
	sync := bitreaderRead(br, 16)
	if sync != 0xFFFF {
		return hcaErrorSync
	}

	if crc16Checksum(data, hca.frameSize) != 0 {
		return hcaErrorChecksum
	}

	cipherDecrypt(&hca.cipherTable, data, int(hca.frameSize))

	// Unpack frame values
	frameAcceptableNoiseLevel := bitreaderRead(br, 9)
	frameEvaluationBoundary := bitreaderRead(br, 7)

	packedNoiseLevel := (frameAcceptableNoiseLevel << 8) - frameEvaluationBoundary

	for ch := uint(0); ch < hca.channels; ch++ {
		err := unpackScaleFactors(&hca.channel[ch], br, hca.hfrGroupCount, hca.version)
		if err != nil {
			return hcaErrorUnpack
		}

		_ = unpackIntensity(&hca.channel[ch], br, hca.hfrGroupCount, hca.version)

		calculateResolution(&hca.channel[ch], packedNoiseLevel, &hca.athCurve,
			hca.minResolution, hca.maxResolution)

		calculateGain(&hca.channel[ch])
	}

	for subframe := 0; subframe < hcaSubframes; subframe++ {
		for ch := uint(0); ch < hca.channels; ch++ {
			dequantizeCoefficients(&hca.channel[ch], br, subframe)
		}
	}

	return br.bit
}

func (hca *ClHCA) decodeBlockTransform() {
	for subframe := 0; subframe < hcaSubframes; subframe++ {
		// Restore missing bands
		for ch := uint(0); ch < hca.channels; ch++ {
			reconstructNoise(&hca.channel[ch], hca.minResolution, hca.msStereo, &hca.random, subframe)

			reconstructHighFrequency(&hca.channel[ch], hca.hfrGroupCount, hca.bandsPerHfrGroup,
				hca.stereoBandCount, hca.baseBandCount, hca.totalBandCount, hca.version, subframe)
		}

		// Restore joint stereo bands
		if hca.stereoBandCount > 0 {
			for ch := uint(0); ch < hca.channels-1; ch++ {
				applyIntensityStereo(&hca.channel, ch, subframe, hca.baseBandCount, hca.totalBandCount)

				applyMsStereo(&hca.channel, ch, hca.msStereo, subframe, hca.baseBandCount, hca.totalBandCount)
			}
		}

		// Apply IMDCT
		for ch := uint(0); ch < hca.channels; ch++ {
			imdctTransform(&hca.channel[ch], subframe)
		}
	}
}
