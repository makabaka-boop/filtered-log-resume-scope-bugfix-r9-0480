package selection

import (
	"math/big"
	"testing"
)

func mustParse(t *testing.T, field, value string) *Predicate {
	t.Helper()
	p, err := Parse(field, value)
	if err != nil {
		t.Fatalf("Parse(%q,%q): %v", field, value, err)
	}
	if p == nil {
		t.Fatalf("Parse(%q,%q) returned nil predicate", field, value)
	}
	return p
}

func TestParseValidation(t *testing.T) {
	if p, err := Parse("", ""); err != nil || p != nil {
		t.Fatalf("empty args = (%v,%v), want (nil,nil)", p, err)
	}
	for _, tc := range []struct{ field, value string }{
		{"/level", ""}, // value without field/field without value
		{"", `"error"`},
		{"level", `"x"`},         // pointer must start with /
		{"/a~", `"x"`},           // dangling escape
		{"/a~2", `"x"`},          // bad escape
		{"/level", `x`},          // invalid JSON
		{"/level", `[1]`},        // array is not a scalar
		{"/level", `{}`},         // object is not a scalar
		{"/level", `1e99999999`}, // exponent out of range
	} {
		if _, err := Parse(tc.field, tc.value); err == nil {
			t.Errorf("Parse(%q,%q) succeeded, want error", tc.field, tc.value)
		}
	}
}

func TestScalarIdentity(t *testing.T) {
	// String "5" vs number 5 vs bool/null: distinct kinds never match.
	str5 := mustParse(t, "/v", `"5"`)
	num5 := mustParse(t, "/v", `5`)
	falseP := mustParse(t, "/v", `false`)
	nullP := mustParse(t, "/v", `null`)

	if str5.Match([]byte(`{"v":5}`)) {
		t.Error(`string "5" matched number 5`)
	}
	if num5.Match([]byte(`{"v":"5"}`)) {
		t.Error(`number 5 matched string "5"`)
	}
	if num5.Match([]byte(`{"v":true}`)) {
		t.Error(`number 5 matched boolean true`)
	}
	if !nullP.Match([]byte(`{"v":null}`)) {
		t.Error(`null filter did not match explicit null`)
	}
	if nullP.Match([]byte(`{"v":false}`)) || nullP.Match([]byte(`{}`)) {
		t.Error("null filter matched non-null or missing field")
	}
	if falseP.Match([]byte(`{"v":null}`)) || falseP.Match([]byte(`{}`)) {
		t.Error("false filter matched null or missing field")
	}
	// Missing field never equals null, and is never a match.
	for _, p := range []*Predicate{str5, num5, falseP, nullP} {
		if p.Match([]byte(`{"other":1}`)) {
			t.Errorf("predicate %s/%v matched missing field", p.Pointer, p.Value)
		}
	}
}

func TestExactNumericIdentity(t *testing.T) {
	p := mustParse(t, "/n", `100`)
	hit := []string{`{"n":100}`, `{"n":100.0}`, `{"n":1e2}`, `{"n":0.1e3}`, `{"n":1000e-1}`}
	miss := []string{
		`{"n":"100"}`, `{"n":true}`, `{"n":null}`, `{"n":[100]}`,
		`{"n":100.5}`, `{"n":99}`,
		// Only equal under float64 rounding, never as exact rationals:
		`{"n":9007199254740993}`, `{"n":9007199254740992}`,
	}
	for _, raw := range hit {
		if !p.Match([]byte(raw)) {
			t.Errorf("100 should match %s", raw)
		}
	}
	bigEq := mustParse(t, "/n", `9007199254740993`)
	for _, raw := range miss {
		if p.Match([]byte(raw)) {
			t.Errorf("100 should not match %s", raw)
		}
	}
	// 2^53+1 parses exactly and matches its own value, not 2^53.
	if !bigEq.Match([]byte(`{"n":9007199254740993}`)) {
		t.Error("large integer literal did not match itself")
	}
	if bigEq.Match([]byte(`{"n":9007199254740992}`)) {
		t.Error("distinct 53-bit integers collapsed like float64")
	}
	// Negative and fractional values.
	frac := mustParse(t, "/n", `-0.25`)
	if !frac.Match([]byte(`{"n":-0.25}`)) || !frac.Match([]byte(`{"n":-2.5e-1}`)) {
		t.Error("fractional filter missed equal spellings")
	}
	if frac.Match([]byte(`{"n":0.25}`)) {
		t.Error("sign ignored in numeric compare")
	}
	// Zero sign/spellings all the same number.
	zero := mustParse(t, "/n", `0`)
	for _, s := range []string{`0`, `-0`, `0.0`, `0e0`, `-0.000`} {
		if !zero.Match([]byte(`{"n":` + s + `}`)) {
			t.Errorf("0 did not match %s", s)
		}
	}
	// Decoded value type assertion.
	if _, ok := p.Value.(*big.Rat); !ok {
		t.Fatalf("predicate value type = %T, want *big.Rat", p.Value)
	}
}

func TestNestedPointerAndArrays(t *testing.T) {
	p := mustParse(t, "/meta/level", `"error"`)
	hits := []string{
		`{"meta":{"level":"error"}}`,
		`{"meta":{"level":"error","x":1}}`,
	}
	for _, raw := range hits {
		if !p.Match([]byte(raw)) {
			t.Errorf("nested miss on %s", raw)
		}
	}
	for _, raw := range []string{
		`{"meta":null}`, // parent null -> missing
		`{"meta":[]}`,   // key into array -> missing
		`{"meta":"x"}`,  // key into string -> missing
		`{"meta":{"lvl":"error"}}`,
		`[]`, `"str"`, `42`, `true`, // non-object root
		`{"meta":{"level":"warn"}}`,
	} {
		if p.Match([]byte(raw)) {
			t.Errorf("nested falsely matched %s", raw)
		}
	}

	ap := mustParse(t, "/tags/1", `"b"`)
	if !ap.Match([]byte(`{"tags":["a","b","c"]}`)) {
		t.Error("array index 1 did not hit")
	}
	for _, raw := range []string{
		`{"tags":["b"]}`, // out of range
		`{"tags":"ab"}`,  // not an array
		`{"tags":[]}`,
		`{"tags":["a",null]}`, // null != string
	} {
		if ap.Match([]byte(raw)) {
			t.Errorf("array pointer falsely matched %s", raw)
		}
	}
	// Root-array access and RFC 6901 index rules ("01" invalid, "-" no hit).
	root := mustParse(t, "/0/k", `true`)
	if !root.Match([]byte(`[{"k":true}]`)) {
		t.Error("root array + nested key did not hit")
	}
	badIdx := mustParse(t, "/a/01", `1`)
	if badIdx.Match([]byte(`{"a":[1]}`)) {
		t.Error(`leading-zero index "01" should never match`)
	}
	append := mustParse(t, "/a/-", `1`)
	if append.Match([]byte(`{"a":[1]}`)) {
		t.Error(`append token "-" should never match`)
	}
}

func TestPointerEscapes(t *testing.T) {
	// ~1 -> "/", ~0 -> "~".
	p := mustParse(t, "/a~1b/c~0d", `7`)
	if !p.Match([]byte(`{"a/b":{"c~d":7}}`)) {
		t.Error("~0/~1 escaped pointer did not resolve")
	}
	if p.Match([]byte(`{"a~1b":{}}`)) {
		t.Error("escaped pointer matched a literal key")
	}
}

func TestScopeBinding(t *testing.T) {
	// Equal numeric spellings -> one filter identity.
	same := []struct{ field, value string }{
		{"/n", `1`}, {"/n", `1.0`}, {"/n", `1e0`}, {"/n", `10e-1`},
	}
	base := mustParse(t, same[0].field, same[0].value).Scope()
	for _, tc := range same[1:] {
		if s := mustParse(t, tc.field, tc.value).Scope(); s != base {
			t.Errorf("scope(%s) = %s, want %s", tc.value, s, base)
		}
	}
	// Every differing condition gets its own scope.
	distinct := []struct{ field, value string }{
		{"/n", `2`},
		{"/m", `1`},
		{"/n", `"1"`},
		{"/n", `true`},
		{"/n", `null`},
	}
	scopes := map[string]string{base: "1"}
	for _, tc := range distinct {
		s := mustParse(t, tc.field, tc.value).Scope()
		if _, dup := scopes[s]; dup {
			t.Errorf("scope collision between %v and %v", tc, scopes[s])
		}
		scopes[s] = tc.value
	}
	var nilP *Predicate
	if nilP.Scope() != "" {
		t.Error("nil predicate scope must be empty")
	}
}
