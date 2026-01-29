package orderedmsgpack

import (
	"bytes"
	"fmt"
	"io"
	"reflect"

	"github.com/iancoleman/orderedmap"
	"github.com/shamaton/msgpack/v2"
	"github.com/shamaton/msgpack/v2/def"
	"github.com/shamaton/msgpack/v2/ext"
)

func (iom internalOrderedMap) ToOrderedMap() orderedmap.OrderedMap {
	om := orderedmap.New()
	om.SetEscapeHTML(false)
	for i, key := range iom.Keys {
		om.Set(key, iom.Vals[i])
	}
	return *om
}

func newInternalMap(om orderedmap.OrderedMap) internalOrderedMap {
	keys := om.Keys()
	v := internalOrderedMap{
		Keys: keys,
		Vals: make([]any, 0, len(keys)),
	}
	for _, key := range keys {
		val, _ := om.Get(key)
		v.Vals = append(v.Vals, val)
	}
	return v
}

func (d *OrderedMapDecoder) Code() int8 {
	return orderedMapExtCode
}

func (d *OrderedMapDecoder) IsType(offset int, data *[]byte) bool {
	code, offset := d.ReadSize1(offset, data)
	if code == def.Ext8 {
		_, offset = d.ReadSize1(offset, data)
		t, _ := d.ReadSize1(offset, data)
		return int8(t) == d.Code()
	}
	return false
}

func (d *OrderedMapDecoder) AsValue(offset int, k reflect.Kind, data *[]byte) (any, int, error) {
	code, offset := d.ReadSize1(offset, data)

	switch code {
	case def.Ext8:
		size, offset := d.ReadSize1(offset, data)
		_, offset = d.ReadSize1(offset, data)
		extData, offset := d.ReadSizeN(offset, int(size), data)

		var iom internalOrderedMap
		err := msgpack.Unmarshal(extData, &iom)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal ordered map data: %w", err)
		}

		return iom.ToOrderedMap(), offset, nil
	}
	return nil, 0, fmt.Errorf("should not reach this line!! code %x decoding %v", d.Code(), k)
}

func (d *OrderedMapStreamDecoder) Code() int8 {
	return orderedMapExtCode
}

func (d *OrderedMapStreamDecoder) IsType(code byte, innerType int8, _ int) bool {
	return code == def.Ext8 && innerType == d.Code()
}

func (d *OrderedMapStreamDecoder) ToValue(code byte, data []byte, k reflect.Kind) (any, error) {
	if code == def.Ext8 {
		var iom internalOrderedMap
		err := msgpack.Unmarshal(data, &iom)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal ordered map data: %w", err)
		}

		return iom.ToOrderedMap(), nil
	}
	return nil, fmt.Errorf("should not reach this line!! code %x decoding %v", d.Code(), k)
}

func (e *OrderedMapEncoder) Code() int8 {
	return orderedMapExtCode
}

func (e *OrderedMapEncoder) Type() reflect.Type {
	return reflect.TypeOf(orderedmap.OrderedMap{})
}

func (e *OrderedMapEncoder) CalcByteSize(value reflect.Value) (int, error) {
	om := value.Interface().(orderedmap.OrderedMap)
	v := newInternalMap(om)

	data, err := msgpack.Marshal(v)
	if err != nil {
		return 0, err
	}

	return def.Byte1 + def.Byte1 + def.Byte1 + len(data), nil
}

func (e *OrderedMapEncoder) WriteToBytes(value reflect.Value, offset int, bytes *[]byte) int {
	om := value.Interface().(orderedmap.OrderedMap)
	v := newInternalMap(om)
	data, _ := msgpack.Marshal(v)

	offset = e.SetByte1Int(def.Ext8, offset, bytes)
	offset = e.SetByte1Int(len(data), offset, bytes)
	offset = e.SetByte1Int(int(e.Code()), offset, bytes)
	offset = e.SetBytes(data, offset, bytes)
	return offset
}

func (e *OrderedMapStreamEncoder) Code() int8 {
	return orderedMapExtCode
}

func (e *OrderedMapStreamEncoder) Type() reflect.Type {
	return reflect.TypeOf(orderedmap.OrderedMap{})
}

func (e *OrderedMapStreamEncoder) Write(w ext.StreamWriter, value reflect.Value) error {
	om := value.Interface().(orderedmap.OrderedMap)
	v := newInternalMap(om)

	data, err := msgpack.Marshal(v)
	if err != nil {
		return err
	}

	if err := w.WriteByte1Int(def.Ext8); err != nil {
		return err
	}
	if err := w.WriteByte1Int(len(data)); err != nil {
		return err
	}
	if err := w.WriteByte1Int(int(e.Code())); err != nil {
		return err
	}
	if err := w.WriteBytes(data); err != nil {
		return err
	}

	return nil
}

func RegisterOrderedMapExt() error {
	if err := msgpack.AddExtCoder(&OrderedMapEncoder{}, &OrderedMapDecoder{}); err != nil {
		return fmt.Errorf("failed to register OrderedMap ext coder: %w", err)
	}
	if err := msgpack.AddExtStreamCoder(&OrderedMapStreamEncoder{}, &OrderedMapStreamDecoder{}); err != nil {
		return fmt.Errorf("failed to register OrderedMap stream ext coder: %w", err)
	}

	return nil
}

func Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

func MarshalWrite(w io.Writer, v any) error {
	return msgpack.MarshalWrite(w, v)
}

func UnmarshalRead(r io.Reader, v any) error {
	return msgpack.UnmarshalRead(r, v)
}

func MsgpackToOrderedMap(b []byte) (*orderedmap.OrderedMap, error) {
	return MsgpackToOrderedMapFromReader(bytes.NewReader(b))
}

func MsgpackToOrderedMapFromReader(r io.Reader) (*orderedmap.OrderedMap, error) {
	var decoded orderedmap.OrderedMap
	if err := UnmarshalRead(r, &decoded); err != nil {
		return nil, err
	}
	decoded.SetEscapeHTML(false)
	return &decoded, nil
}
