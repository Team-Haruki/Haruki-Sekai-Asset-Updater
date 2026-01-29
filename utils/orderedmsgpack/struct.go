package orderedmsgpack

import "github.com/shamaton/msgpack/v2/ext"

const orderedMapExtCode = 100

type internalOrderedMap struct {
	Keys []string `msgpack:"k"`
	Vals []any    `msgpack:"v"`
}

type OrderedMapDecoder struct {
	ext.DecoderCommon
}

type OrderedMapStreamDecoder struct{}

type OrderedMapEncoder struct {
	ext.EncoderCommon
}

type OrderedMapStreamEncoder struct{}

var _ ext.Decoder = (*OrderedMapDecoder)(nil)
var _ ext.StreamDecoder = (*OrderedMapStreamDecoder)(nil)
var _ ext.Encoder = (*OrderedMapEncoder)(nil)
var _ ext.StreamEncoder = (*OrderedMapStreamEncoder)(nil)
