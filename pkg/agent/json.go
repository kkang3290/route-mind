package agent

import (
	"bytes"
	"encoding/json"
	"math/big"
	"reflect"
	"strconv"
	"strings"
)

// PostgreSQL JSONB changes object ordering and numeric spelling. Compare schema
// meaning, not its serialized bytes; retain exact numbers (never float64).
func sameJSON(a, b []byte) bool {
	decode := func(data []byte) (any, error) {
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		var v any
		e := d.Decode(&v)
		return normalizeNumbers(v), e
	}
	x, ex := decode(a)
	y, ey := decode(b)
	return ex == nil && ey == nil && reflect.DeepEqual(x, y)
}
func normalizeNumbers(v any) any {
	switch x := v.(type) {
	case json.Number:
		if at := strings.IndexAny(string(x), "eE"); at >= 0 {
			exp, e := strconv.Atoi(string(x)[at+1:])
			if e != nil || exp > 4096 || exp < -4096 {
				return x
			}
		}
		r, ok := new(big.Rat).SetString(string(x))
		if ok {
			return r
		}
		return x
	case map[string]any:
		for k, v := range x {
			x[k] = normalizeNumbers(v)
		}
	case []any:
		for i, v := range x {
			x[i] = normalizeNumbers(v)
		}
	}
	return v
}
