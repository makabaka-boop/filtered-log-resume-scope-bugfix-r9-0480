// Package selection implements the field filter applied to NDJSON records:
// a JSON Pointer selecting a scalar and a JSON scalar to compare against.
//
// Scalars keep their JSON identity: the string "5" never matches the number
// 5, booleans and null are their own kinds, and a field present with value
// null is distinct from a missing field. Numbers are compared by exact
// numeric value (big.Rat), so 1, 1.0 and 1e0 share one filter identity while
// values that merely round to the same float64 do not match.
package selection

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// maxNumberExp bounds exponents accepted while turning JSON numbers into
// exact rationals, so a hostile literal (1e999999...) cannot allocate freely.
const maxNumberExp = 100_000

// Predicate is one validated field/value subscription filter. The nil
// *Predicate means "no filter" and matches every record.
type Predicate struct {
	// Pointer is the validated raw JSON Pointer (always starts with "/").
	Pointer string
	// Value is the decoded scalar: nil (JSON null), string, bool or *big.Rat.
	// A nil Value therefore means the JSON scalar null, not "no value".
	Value any

	tokens []string // unescaped pointer tokens, in order
	canon  string   // canonical scalar encoding used for Scope
}

// Parse builds a Predicate from the field/value query parameters.
//
// Both empty means no filter (nil). They must be supplied together. The
// pointer must be a non-empty JSON Pointer (RFC 6901 syntax, including ~0/~1
// escapes) and the value must be a single JSON scalar: string, number,
// boolean or null — objects and arrays are not filter scalars.
func Parse(pointer, value string) (*Predicate, error) {
	if pointer == "" && value == "" {
		return nil, nil
	}
	if pointer == "" || value == "" {
		return nil, errors.New("selection: field and value must be provided together")
	}
	tokens, err := parsePointer(pointer)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(strings.NewReader(value))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("selection: value is not valid JSON: %w", err)
	}
	scalar, canon, err := normalizeScalar(v)
	if err != nil {
		return nil, err
	}
	return &Predicate{Pointer: pointer, Value: scalar, tokens: tokens, canon: canon}, nil
}

// Scope is the stable, collision-resistant identity of the filter bound into
// resumption cursors. Equal numeric spellings (1, 1.0, 1e0) share a scope;
// different scalar kinds never do.
func (p *Predicate) Scope() string {
	if p == nil {
		return ""
	}
	h := sha256.New()
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(p.Pointer)))
	h.Write(lenBuf[:n])
	h.Write([]byte(p.Pointer))
	h.Write([]byte(p.canon))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Match reports whether the compacted JSON record contains the selected
// scalar equal to the filter value. Malformed JSON, non-matching types,
// missing fields and a pointer leading through a non-container all miss.
func (p *Predicate) Match(raw []byte) bool {
	if p == nil {
		return true
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return false
	}
	got, present := lookup(root, p.tokens)
	if !present {
		return false // missing field is not the same as null
	}
	return scalarEqual(p.Value, got)
}

// parsePointer validates RFC 6901 syntax and returns the unescaped tokens.
// The leading "/" is required: an empty pointer cannot be expressed through
// the query API ("" means the parameter is absent).
func parsePointer(p string) ([]string, error) {
	if p == "" || p[0] != '/' {
		return nil, errors.New(`selection: field must be a JSON Pointer starting with "/"`)
	}
	parts := strings.Split(p[1:], "/")
	tokens := make([]string, 0, len(parts))
	for _, part := range parts {
		tok, err := unescapeToken(part)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
	}
	return tokens, nil
}

// unescapeToken applies ~0 -> "~" and ~1 -> "/". Any other use of "~" is a
// malformed pointer rather than a literal byte.
func unescapeToken(t string) (string, error) {
	if !strings.Contains(t, "~") {
		return t, nil
	}
	var b strings.Builder
	b.Grow(len(t))
	for i := 0; i < len(t); i++ {
		if t[i] != '~' {
			b.WriteByte(t[i])
			continue
		}
		if i+1 >= len(t) {
			return "", errors.New("selection: dangling '~' escape in JSON Pointer")
		}
		switch t[i+1] {
		case '0':
			b.WriteByte('~')
		case '1':
			b.WriteByte('/')
		default:
			return "", fmt.Errorf("selection: invalid escape %q in JSON Pointer", t[i:i+2])
		}
		i++
	}
	return b.String(), nil
}

// lookup walks the unescaped pointer tokens. The present flag distinguishes
// an explicit null value (nil, true) from a missing path (nil, false).
func lookup(root any, tokens []string) (v any, present bool) {
	v = root
	for _, tok := range tokens {
		switch c := v.(type) {
		case map[string]any:
			next, ok := c[tok]
			if !ok {
				return nil, false
			}
			v = next
		case []any:
			idx, ok := arrayIndex(tok, len(c))
			if !ok {
				return nil, false
			}
			v = c[idx]
		default:
			return nil, false
		}
	}
	return v, true
}

// arrayIndex interprets a pointer token as an RFC 6901 array index: "0", or
// digits without a leading zero, in range. "-" (append index) never matches.
func arrayIndex(tok string, length int) (int, bool) {
	if tok == "" || tok == "-" || len(tok) > 1 && tok[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] < '0' || tok[i] > '9' {
			return 0, false
		}
	}
	idx, err := strconv.Atoi(tok)
	if err != nil || idx < 0 || idx >= length {
		return 0, false
	}
	return idx, true
}

// normalizeScalar validates that the decoded value is a scalar and produces
// its canonical encoding for the cursor scope.
func normalizeScalar(v any) (scalar any, canon string, err error) {
	switch t := v.(type) {
	case nil:
		return nil, "null", nil
	case string:
		raw, mErr := json.Marshal(t)
		if mErr != nil {
			return nil, "", fmt.Errorf("selection: %w", mErr)
		}
		return t, string(raw), nil
	case bool:
		if t {
			return t, "true", nil
		}
		return t, "false", nil
	case json.Number:
		r, pErr := parseNumber(t.String())
		if pErr != nil {
			return nil, "", pErr
		}
		return r, canonicalRat(r), nil
	default:
		return nil, "", errors.New("selection: value must be a JSON scalar (string, number, boolean or null)")
	}
}

// scalarEqual compares a filter scalar against a value decoded from a record.
// Kinds must agree; two json.Numbers compare by exact numeric value.
func scalarEqual(want, got any) bool {
	switch w := want.(type) {
	case nil:
		return got == nil
	case string:
		g, ok := got.(string)
		return ok && g == w
	case bool:
		g, ok := got.(bool)
		return ok && g == w
	case *big.Rat:
		g, ok := got.(json.Number)
		if !ok {
			return false
		}
		gr, err := parseNumber(g.String())
		if err != nil {
			return false
		}
		return w.Cmp(gr) == 0
	default:
		return false
	}
}

// parseNumber converts a JSON number literal into an exact *big.Rat, handling
// sign, fraction and exponent without float64 rounding.
func parseNumber(s string) (*big.Rat, error) {
	orig := s
	neg := false
	if s != "" && (s[0] == '-' || s[0] == '+') {
		neg = s[0] == '-'
		s = s[1:]
	}
	exp := 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		n, err := strconv.Atoi(s[i+1:])
		if err != nil || n < -maxNumberExp || n > maxNumberExp {
			return nil, fmt.Errorf("selection: number exponent out of range in %q", orig)
		}
		exp = n
		s = s[:i]
	}
	intPart, fracPart, _ := strings.Cut(s, ".")
	if intPart == "" {
		return nil, fmt.Errorf("selection: malformed number %q", orig)
	}
	digits, ok := new(big.Int).SetString(intPart+fracPart, 10)
	if !ok {
		return nil, fmt.Errorf("selection: malformed number %q", orig)
	}
	scale := exp - len(fracPart)
	r := new(big.Rat).SetInt(digits)
	if scale != 0 {
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(absInt(scale))), nil)
		q := new(big.Rat).SetInt(pow)
		if scale > 0 {
			r.Mul(r, q)
		} else {
			r.Quo(r, q)
		}
	}
	if neg {
		r.Neg(r)
	}
	return r, nil
}

// canonicalRat renders an exact finite decimal without exponent or
// insignificant trailing zeros, so equal numeric values share one spelling.
func canonicalRat(r *big.Rat) string {
	num, den := r.Num(), r.Denom()
	if den.Cmp(big.NewInt(1)) == 0 {
		return num.String()
	}
	// A number reduced from a JSON literal has a denominator 2^a*5^b, hence
	// a finite decimal. k decimal places make 10^k divisible by den.
	a := primeExponent(den, big.NewInt(2))
	b := primeExponent(den, big.NewInt(5))
	k := a
	if b > k {
		k = b
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(k)), nil)
	scaled := new(big.Int).Mul(num, pow)
	if new(big.Int).Mod(scaled, den).Sign() != 0 {
		return r.FloatString(k) // unreachable for JSON-derived numbers
	}
	scaled.Quo(scaled, den)

	s := scaled.String()
	minus := strings.HasPrefix(s, "-")
	if minus {
		s = s[1:]
	}
	if len(s) <= k {
		s = strings.Repeat("0", k+1-len(s)) + s
	}
	intPart, fracPart := s[:len(s)-k], s[len(s)-k:]
	fracPart = strings.TrimRight(fracPart, "0")
	out := intPart
	if fracPart != "" {
		out += "." + fracPart
	}
	if minus && out != "0" {
		out = "-" + out
	}
	return out
}

// primeExponent returns the largest e for which p^e divides n. It does not
// mutate n.
func primeExponent(n, p *big.Int) int {
	quo, prod := new(big.Int), new(big.Int)
	e := 0
	for {
		quo.Quo(n, p)
		prod.Mul(quo, p)
		if prod.Cmp(n) != 0 {
			return e
		}
		n = quo
		e++
	}
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
