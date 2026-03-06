package orderedmsgpack

import (
	"testing"

	"github.com/iancoleman/orderedmap"
	"github.com/shamaton/msgpack/v2"
)

func TestOrderedMsgpackPreservesOrder(t *testing.T) {
	// Register the extension (normally done in main/init)
	err := RegisterOrderedMapExt()
	if err != nil {
		t.Fatalf("Failed to register extension: %v", err)
	}

	t.Run("Order A-B-C", func(t *testing.T) {
		o := orderedmap.New()
		o.Set("a", 1)
		o.Set("b", 2)
		o.Set("c", 3)

		// Marshal using extension
		data, err := Marshal(o)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		// Unmarshal using new decoder
		loaded, err := MsgpackToOrderedMap(data)
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		keys := loaded.Keys()
		expectedKeys := []string{"a", "b", "c"}
		if len(keys) != len(expectedKeys) {
			t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(keys))
		}
		for i, k := range keys {
			if k != expectedKeys[i] {
				t.Errorf("Key %d: expected %s, got %s", i, expectedKeys[i], k)
			}
		}
	})

	t.Run("Order C-B-A", func(t *testing.T) {
		o := orderedmap.New()
		o.Set("c", 3)
		o.Set("b", 2)
		o.Set("a", 1)

		data, err := Marshal(o)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		loaded, err := MsgpackToOrderedMap(data)
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		keys := loaded.Keys()
		expectedKeys := []string{"c", "b", "a"}
		if len(keys) != len(expectedKeys) {
			t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(keys))
		}
		for i, k := range keys {
			if k != expectedKeys[i] {
				t.Errorf("Key %d: expected %s, got %s", i, expectedKeys[i], k)
			}
		}
	})
}

func TestStandardMsgpackToOrderedMap(t *testing.T) {
	t.Run("Simple flat map", func(t *testing.T) {
		// Use a struct with ordered fields to guarantee the encoding order
		type orderedStruct struct {
			X int `msgpack:"x"`
			Y int `msgpack:"y"`
			Z int `msgpack:"z"`
		}
		data, err := msgpack.Marshal(orderedStruct{X: 10, Y: 20, Z: 30})
		if err != nil {
			t.Fatalf("Standard marshal failed: %v", err)
		}

		loaded, err := MsgpackToOrderedMap(data)
		if err != nil {
			t.Fatalf("MsgpackToOrderedMap failed: %v", err)
		}

		keys := loaded.Keys()
		expectedKeys := []string{"x", "y", "z"}
		if len(keys) != len(expectedKeys) {
			t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(keys), keys)
		}
		for i, k := range keys {
			if k != expectedKeys[i] {
				t.Errorf("Key %d: expected %s, got %s", i, expectedKeys[i], k)
			}
		}

		// Verify values
		xVal, _ := loaded.Get("x")
		yVal, _ := loaded.Get("y")
		zVal, _ := loaded.Get("z")
		if xVal.(int64) != 10 {
			t.Errorf("Expected x=10, got %v", xVal)
		}
		if yVal.(int64) != 20 {
			t.Errorf("Expected y=20, got %v", yVal)
		}
		if zVal.(int64) != 30 {
			t.Errorf("Expected z=30, got %v", zVal)
		}
	})

	t.Run("Nested map", func(t *testing.T) {
		type inner struct {
			A string `msgpack:"a"`
			B int    `msgpack:"b"`
		}
		type outer struct {
			Name  string `msgpack:"name"`
			Inner inner  `msgpack:"inner"`
			Score int    `msgpack:"score"`
		}
		data, err := msgpack.Marshal(outer{
			Name:  "test",
			Inner: inner{A: "hello", B: 42},
			Score: 100,
		})
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		loaded, err := MsgpackToOrderedMap(data)
		if err != nil {
			t.Fatalf("MsgpackToOrderedMap failed: %v", err)
		}

		// Check outer keys
		keys := loaded.Keys()
		expectedKeys := []string{"name", "inner", "score"}
		if len(keys) != len(expectedKeys) {
			t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(keys), keys)
		}
		for i, k := range keys {
			if k != expectedKeys[i] {
				t.Errorf("Key %d: expected %s, got %s", i, expectedKeys[i], k)
			}
		}

		// Check inner map is also an OrderedMap
		innerVal, _ := loaded.Get("inner")
		innerOM, ok := innerVal.(*orderedmap.OrderedMap)
		if !ok {
			t.Fatalf("Expected inner to be *orderedmap.OrderedMap, got %T", innerVal)
		}
		innerKeys := innerOM.Keys()
		expectedInnerKeys := []string{"a", "b"}
		if len(innerKeys) != len(expectedInnerKeys) {
			t.Fatalf("Expected %d inner keys, got %d: %v", len(expectedInnerKeys), len(innerKeys), innerKeys)
		}
		aVal, _ := innerOM.Get("a")
		if aVal.(string) != "hello" {
			t.Errorf("Expected a=hello, got %v", aVal)
		}
	})

	t.Run("Map with array", func(t *testing.T) {
		type withArray struct {
			Items []int  `msgpack:"items"`
			Label string `msgpack:"label"`
		}
		data, err := msgpack.Marshal(withArray{
			Items: []int{1, 2, 3},
			Label: "test",
		})
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		loaded, err := MsgpackToOrderedMap(data)
		if err != nil {
			t.Fatalf("MsgpackToOrderedMap failed: %v", err)
		}

		keys := loaded.Keys()
		if len(keys) != 2 {
			t.Fatalf("Expected 2 keys, got %d: %v", len(keys), keys)
		}
		if keys[0] != "items" || keys[1] != "label" {
			t.Errorf("Unexpected keys: %v", keys)
		}

		itemsVal, _ := loaded.Get("items")
		items, ok := itemsVal.([]any)
		if !ok {
			t.Fatalf("Expected items to be []any, got %T", itemsVal)
		}
		if len(items) != 3 {
			t.Fatalf("Expected 3 items, got %d", len(items))
		}
		for i, expected := range []int64{1, 2, 3} {
			if items[i].(int64) != expected {
				t.Errorf("Item %d: expected %d, got %v", i, expected, items[i])
			}
		}
	})

	t.Run("Map with various types", func(t *testing.T) {
		type mixed struct {
			Name   string  `msgpack:"name"`
			Age    int     `msgpack:"age"`
			Score  float64 `msgpack:"score"`
			Active bool    `msgpack:"active"`
		}
		data, err := msgpack.Marshal(mixed{
			Name:   "Alice",
			Age:    25,
			Score:  99.5,
			Active: true,
		})
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		loaded, err := MsgpackToOrderedMap(data)
		if err != nil {
			t.Fatalf("MsgpackToOrderedMap failed: %v", err)
		}

		nameVal, _ := loaded.Get("name")
		if nameVal.(string) != "Alice" {
			t.Errorf("Expected name=Alice, got %v", nameVal)
		}
		ageVal, _ := loaded.Get("age")
		if ageVal.(int64) != 25 {
			t.Errorf("Expected age=25, got %v", ageVal)
		}
		scoreVal, _ := loaded.Get("score")
		if scoreVal.(float64) != 99.5 {
			t.Errorf("Expected score=99.5, got %v", scoreVal)
		}
		activeVal, _ := loaded.Get("active")
		if activeVal.(bool) != true {
			t.Errorf("Expected active=true, got %v", activeVal)
		}
	})
}
