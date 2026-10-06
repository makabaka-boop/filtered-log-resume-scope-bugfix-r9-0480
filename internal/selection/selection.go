package selection

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// Predicate is a scalar-equality filter over a JSON Pointer (RFC 6901).
//
// Scalars are distinguished by type: the string "1" never matches the number
// 1, booleans are not strings, and a missing pointer is not null. Numbers are
// compared by exact numeric value, so 1, 1.0 and 1e0 have the same filter
// identity. Only scalar targets match; an object or array behind the pointer
// is not equal to any scalar.
type Predicate struct {
	// Pointer is the raw, valid RFC 6901 JSON Pointer the subscription binds to.
	Pointer string
	// tokens are the unescaped reference tokens of Pointer.
	tokens []string
	// want is the parsed target scalar, always one of string, bool, nil
	// (JSON null), json.Number, int64 or float64.
	want any

	scope string
}

// Parse builds the predicate for field=<JSON Pointer>&value=<JSON scalar>.
//
// Both parameters must be supplied together: a value without a location or a
// location without a value is a bad request. value must itself be a complete
// JSON scalar (string in JSON quotes, number, true/false, null) — never an
// object or array. A nil predicate and nil error mean "no filter" and keep the
// unfiltered stream behaviour.
func Parse(pointer, value string) (*Predicate, error) {
	if pointer == "" && value == "" {
		return nil, nil
	}
	if pointer == "" {
		return nil, fmt.Errorf("filter requires both field and value: field is empty")
	}
	if value == "" {
		return nil, fmt.Errorf("filter requires both field and value: value is empty")
	}
	tokens, err := ParsePointer(pointer)
	if err != nil {
		return nil, err
	}
	want, err := parseScalar(value)
	if err != nil {
		return nil, err
	}
	p := &Predicate{Pointer: pointer, tokens: tokens, want: want}
	p.scope = computeScope(pointer, want)
	return p, nil
}

// Scope is the opaque identity of the filter condition. It is embedded in
// cursors so a token can only be resumed under the exact same predicate;
// numerically equal values share a scope, different scalar types do not.
func (p *Predicate) Scope() string {
	if p == nil {
		return ""
	}
	return p.scope
}

// Match reports whether the complete, already-validated JSON line carries the
// wanted scalar exactly at Pointer. It never errors: a structurally unmatchable
// line is simply a miss.
func (p *Predicate) Match(raw []byte) bool {
	if p == nil {
		return true
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return false
	}
	got, ok := resolve(doc, p.tokens)
	if !ok {
		return false // missing is not null
	}
	return scalarEqual(got, p.want)
}

// ParsePointer parses an RFC 6901 JSON Pointer into its reference tokens,
// applying ~1 -> "/" and ~0 -> "~" (in that order). It rejects malformed
// escapes and pointers that do not start with "/".
func ParsePointer(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, fmt.Errorf("invalid JSON Pointer %q: empty pointer", pointer)
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, fmt.Errorf("invalid JSON Pointer %q: must start with \"/\"", pointer)
	}
	rest := pointer[1:]
	if rest == "" {
		return []string{}, nil
	}
	parts := strings.Split(rest, "/")
	tokens := make([]string, 0, len(parts))
	for _, part := range parts {
		tok, err := unescapeToken(part)
		if err != nil {
			return nil, fmt.Errorf("invalid JSON Pointer %q: %v", pointer, err)
		}
		tokens = append(tokens, tok)
	}
	return tokens, nil
}

func unescapeToken(tok string) (string, error) {
	if !strings.ContainsRune(tok, '~') {
		return tok, nil
	}
	var b strings.Builder
	b.Grow(len(tok))
	for i := 0; i < len(tok); i++ {
		if tok[i] != '~' {
			b.WriteByte(tok[i])
			continue
		}
		if i+1 >= len(tok) {
			return "", fmt.Errorf("bare \"~\" must be escaped as ~0")
		}
		switch tok[i+1] {
		case '0':
			b.WriteByte('~')
		case '1':
			b.WriteByte('/')
		default:
			return "", fmt.Errorf("bad escape ~%c (only ~0 and ~1 are valid)", tok[i+1])
		}
		i++
	}
	return b.String(), nil
}

// parseScalar decodes one JSON scalar using UseNumber so numeric identity is
// never lost to float64 rounding. Objects and arrays are rejected.
func parseScalar(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("invalid filter value %q: must be a JSON scalar: %w", s, err)
	}
	// Reject trailing tokens ("1 x", "1 2").
	if dec.More() {
		return nil, fmt.Errorf("invalid filter value %q: must be a single JSON scalar", s)
	}
	switch v.(type) {
	case string, json.Number, bool, nil:
		return v, nil
	default:
		return nil, fmt.Errorf("invalid filter value %q: only scalars (string/number/boolean/null) are supported", s)
	}
}

// resolve walks the already-decoded document. found=false means a required key
// or array index is absent — including an explicit JSON null along the way
// (null is a leaf, not a traversable container).
func resolve(doc any, tokens []string) (v any, found bool) {
	cur := doc
	for _, tok := range tokens {
		switch node := cur.(type) {
		case map[string]any:
			child, ok := node[tok]
			if !ok {
				return nil, false
			}
			cur = child
		case []any:
			idx, ok := arrayIndex(tok, len(node))
			if !ok {
				return nil, false
			}
			cur = node[idx]
		default:
			return nil, false
		}
	}
	return cur, true
}

// arrayIndex implements the RFC 6901 array-index rule: digits only, no leading
// zero except "0" itself, and within bounds.
func arrayIndex(tok string, n int) (int, bool) {
	if tok == "" {
		return 0, false
	}
	if len(tok) > 1 && tok[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] < '0' || tok[i] > '9' {
			return 0, false
		}
	}
	idx, err := strconv.Atoi(tok)
	if err != nil || idx < 0 || idx >= n {
		return 0, false
	}
	return idx, true
}

// scalarEqual compares two decoded JSON scalars with strict type identity,
// except numbers, which compare by exact numeric value.
func scalarEqual(a, b any) bool {
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case nil:
		return b == nil
	case json.Number:
		return numbersEqual(av, b)
	case int64:
		return numbersEqual(json.Number(strconv.FormatInt(av, 10)), b)
	case float64:
		return numbersEqual(json.Number(strconv.FormatFloat(av, 'g', -1, 64)), b)
	default:
		// Objects and arrays never equal a scalar target.
		return false
	}
}

func numbersEqual(n json.Number, other any) bool {
	o, ok := other.(json.Number)
	if !ok {
		return false
	}
	ra, ok1 := new(big.Rat).SetString(string(n))
	rb, ok2 := new(big.Rat).SetString(string(o))
	if !ok1 || !ok2 {
		return false
	}
	return ra.Cmp(rb) == 0
}

// computeScope binds a cursor to its predicate: the raw pointer plus a
// canonical, type-tagged rendering of the target value. Numbers collapse to
// their exact rational form so equal numeric values (1 / 1.0 / 1e0) share a
// scope and different types never collide.
func computeScope(pointer string, want any) string {
	h := sha256.New()
	h.Write([]byte(pointer))
	h.Write([]byte{0})
	h.Write([]byte(canonicalScalar(want)))
	return hex.EncodeToString(h.Sum(nil))
}

func canonicalScalar(v any) string {
	switch t := v.(type) {
	case nil:
		return "n:"
	case bool:
		if t {
			return "b:true"
		}
		return "b:false"
	case string:
		return "s:" + t
	case json.Number:
		r, ok := new(big.Rat).SetString(string(t))
		if !ok {
			return "x:" + string(t)
		}
		return "num:" + r.RatString()
	default:
		return "?"
	}
}
