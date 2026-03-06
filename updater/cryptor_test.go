package updater

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"testing"

	"haruki-sekai-asset/utils/orderedmsgpack"
)

const (
	testAESKeyHex = "6732666343305A637A4E394D544A3631"
	testAESIVHex  = "6D737833495630693958453575595A31"
)

func pkcs7PadForTest(data []byte, blockSize int) []byte {
	padding := blockSize - (len(data) % blockSize)
	if padding == 0 {
		padding = blockSize
	}
	out := make([]byte, len(data)+padding)
	copy(out, data)
	for i := len(data); i < len(out); i++ {
		out[i] = byte(padding)
	}
	return out
}

func encryptMsgpackForTest(t *testing.T, c *SekaiCryptor, payload any) []byte {
	t.Helper()
	raw, err := orderedmsgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}
	padded := pkcs7PadForTest(raw, aes.BlockSize)
	out := make([]byte, len(padded))
	encrypter := cipher.NewCBCEncrypter(c.block, c.iv)
	encrypter.CryptBlocks(out, padded)
	return out
}

type cryptorPayload struct {
	Name  string `msgpack:"name"`
	Count int    `msgpack:"count"`
}

func TestNewSekaiCryptorFromHex_Validation(t *testing.T) {
	if _, err := NewSekaiCryptorFromHex("zz", testAESIVHex); err == nil {
		t.Fatalf("expected error for invalid key hex")
	}
	if _, err := NewSekaiCryptorFromHex(testAESKeyHex, "00"); err == nil {
		t.Fatalf("expected error for invalid iv length")
	}

	c, err := NewSekaiCryptorFromHex(testAESKeyHex, testAESIVHex)
	if err != nil {
		t.Fatalf("unexpected cryptor init error: %v", err)
	}
	key, _ := hex.DecodeString(testAESKeyHex)
	if len(c.key) != len(key) {
		t.Fatalf("unexpected key length: got %d, want %d", len(c.key), len(key))
	}
}

func TestUnpackInto_ValidationErrors(t *testing.T) {
	c, err := NewSekaiCryptorFromHex(testAESKeyHex, testAESIVHex)
	if err != nil {
		t.Fatalf("cryptor init failed: %v", err)
	}

	if err := c.UnpackInto(nil, &cryptorPayload{}); err != ErrEmptyContent {
		t.Fatalf("expected ErrEmptyContent, got %v", err)
	}
	if err := c.UnpackInto([]byte{1, 2, 3}, &cryptorPayload{}); err != ErrInvalidBlockSize {
		t.Fatalf("expected ErrInvalidBlockSize, got %v", err)
	}
	if err := c.UnpackInto(make([]byte, aes.BlockSize), nil); err == nil {
		t.Fatalf("expected error when out is nil")
	}
}

func TestUnpackInto_Success(t *testing.T) {
	c, err := NewSekaiCryptorFromHex(testAESKeyHex, testAESIVHex)
	if err != nil {
		t.Fatalf("cryptor init failed: %v", err)
	}

	src := cryptorPayload{Name: "alpha", Count: 7}
	enc := encryptMsgpackForTest(t, c, src)

	var got cryptorPayload
	if err := c.UnpackInto(enc, &got); err != nil {
		t.Fatalf("unpack failed: %v", err)
	}
	if got != src {
		t.Fatalf("unpacked payload mismatch: got %+v, want %+v", got, src)
	}
}

func TestUnpackOrdered_Success(t *testing.T) {
	c, err := NewSekaiCryptorFromHex(testAESKeyHex, testAESIVHex)
	if err != nil {
		t.Fatalf("cryptor init failed: %v", err)
	}

	type orderedStruct struct {
		First  string `msgpack:"first"`
		Second int    `msgpack:"second"`
	}
	enc := encryptMsgpackForTest(t, c, orderedStruct{First: "x", Second: 2})

	om, err := c.UnpackOrdered(enc)
	if err != nil {
		t.Fatalf("UnpackOrdered failed: %v", err)
	}
	keys := om.Keys()
	if len(keys) != 2 || keys[0] != "first" || keys[1] != "second" {
		t.Fatalf("unexpected ordered map keys: %v", keys)
	}
}
