package selection

import "testing"

func TestParseRequiresBoth(t *testing.T) {
	if _, err := Parse("/a", ""); err == nil {
		t.Fatal("field without value must error")
	}
	if _, err := Parse("", `"x"`); err == nil {
		t.Fatal("value without field must error")
	}
	p, err := Parse("", "")
	if err != nil || p != nil {
		t.Fatalf("empty params must mean no filter, got %v %v", p, err)
	}
}

func TestParseValueMustBeScalar(t *testing.T) {
	for _, v := range []string{`[1]`, `{"a":1}`, ``, `unterminated`, `1 x`, `'str'`, `tru`} {
		if _, err := Parse("/a", v); err == nil {
			t.Fatalf("non-scalar value %q unexpectedly accepted", v)
		}
	}
	for _, v := range []string{`"s"`, `1`, `1.5`, `-3e2`, `true`, `false`, `null`} {
		if _, err := Parse("/a", v); err != nil {
			t.Fatalf("scalar value %q rejected: %v", v, err)
		}
	}
}

func TestParsePointer(t *testing.T) {
	cases := []struct {
		pointer string
		tokens  []string
		ok      bool
	}{
		{"/", []string{}, true},
		{"/a/b", []string{"a", "b"}, true},
		{"/a~1b", []string{"a/b"}, true},
		{"/a~0b", []string{"a~b"}, true},
		{"/~01", []string{"~1"}, true},
		{"/~1~0", []string{"/~"}, true},
		{"", nil, false},
		{"a", nil, false},
		{"/a~2", nil, false},
		{"/a~", nil, false},
	}
	for _, tc := range cases {
		got, err := ParsePointer(tc.pointer)
		if tc.ok != (err == nil) {
			t.Fatalf("ParsePointer(%q) err=%v, want ok=%v", tc.pointer, err, tc.ok)
		}
		if !tc.ok {
			continue
		}
		if len(got) != len(tc.tokens) {
			t.Fatalf("ParsePointer(%q) = %v, want %v", tc.pointer, got, tc.tokens)
		}
		for i := range got {
			if got[i] != tc.tokens[i] {
				t.Fatalf("ParsePointer(%q) = %v, want %v", tc.pointer, got, tc.tokens)
			}
		}
	}
}

func TestMatchScalarIdentity(t *testing.T) {
	// Same textual value across distinct scalar types must not collide.
	cases := []struct {
		name  string
		field string
		value string
		line  string
		want  bool
	}{
		{"string one", "/v", `"1"`, `{"v":"1"}`, true},
		{"string vs number", "/v", `"1"`, `{"v":1}`, false},
		{"number vs string", "/v", `1`, `{"v":"1"}`, false},
		{"int equal", "/v", `1`, `{"v":1}`, true},
		{"numeric identity 1.0", "/v", `1.0`, `{"v":1}`, true},
		{"numeric identity 1e0", "/v", `1e0`, `{"v":1}`, true},
		{"numeric identity reverse", "/v", `1`, `{"v":1.000}`, true},
		{"fraction equal", "/v", `0.5`, `{"v":0.50}`, true},
		{"fraction unequal", "/v", `0.5`, `{"v":0.05}`, false},
		{"negative exponent", "/v", `0.01`, `{"v":1e-2}`, true},
		{"big int beyond float64 identity", "/v", `9007199254740993`, `{"v":9007199254740993}`, true},
		{"big int neighbours differ", "/v", `9007199254740993`, `{"v":9007199254740994}`, false},
		{"true", "/v", `true`, `{"v":true}`, true},
		{"true vs string", "/v", `true`, `{"v":"true"}`, false},
		{"true vs one", "/v", `true`, `{"v":1}`, false},
		{"false vs null", "/v", `false`, `{"v":null}`, false},
		{"null explicit", "/v", `null`, `{"v":null}`, true},
		{"null vs missing", "/v", `null`, `{"w":1}`, false},
		{"null vs string", "/v", `null`, `{"v":"null"}`, false},
		{"missing field", "/v", `"x"`, `{"w":"x"}`, false},
		{"nested object value", "/meta/level", `"error"`, `{"meta":{"level":"error"}}`, true},
		{"nested missing", "/meta/level", `"error"`, `{"meta":{"other":"error"}}`, false},
		{"escaped slash key", "/a~1b", `1`, `{"a/b":1}`, true},
		{"escaped tilde key", "/a~0b", `1`, `{"a~b":1}`, true},
		{"array index hit", "/items/1/level", `"error"`, `{"items":[{"level":"info"},{"level":"error"}]}`, true},
		{"array index miss", "/items/0/level", `"error"`, `{"items":[{"level":"info"},{"level":"error"}]}`, false},
		{"array leading zero rejected as path", "/items/01", `1`, `{"items":[1]}`, false},
		{"array out of bounds", "/items/9", `1`, `{"items":[1]}`, false},
		{"array negative rejected", "/items/-1", `1`, `{"items":[1]}`, false},
		{"object not scalar equal", "/v", `1`, `{"v":{"a":1}}`, false},
		{"array not scalar equal", "/v", `1`, `{"v":[1]}`, false},
		{"null through object", "/a/b", `null`, `{"a":null}`, false},
		{"empty string equality", "/v", `""`, `{"v":""}`, true},
		{"empty string vs missing", "/v", `""`, `{"w":""}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Parse(tc.field, tc.value)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := p.Match([]byte(tc.line)); got != tc.want {
				t.Errorf("Match(%s with %s=%s) = %v, want %v", tc.line, tc.field, tc.value, got, tc.want)
			}
		})
	}

	// "/" points at the whole document; a scalar target only matches when the
	// document itself is that scalar.
	root, err := Parse("/", `1`)
	if err != nil {
		t.Fatal(err)
	}
	if root.Match([]byte(`1`)) != true {
		t.Error("root scalar match failed")
	}
	if root.Match([]byte(`{"a":1}`)) != false {
		t.Error("root pointer must not match object against scalar")
	}
}

func TestMatchIsTypeSafeAfterJSONRoundTrip(t *testing.T) {
	// The record arrives json.Compact-ed; matching must behave identically.
	p, err := Parse("/level", `"error"`)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Match([]byte(`{ "level" : "error" , "n" : 1 }`)) {
		t.Error("compacted spacing must still match")
	}
}

func TestScopeBindsCondition(t *testing.T) {
	sc := func(field, value string) string {
		p, err := Parse(field, value)
		if err != nil {
			t.Fatalf("parse %s=%s: %v", field, value, err)
		}
		return p.Scope()
	}
	// Numerically equal renderings share a scope.
	if sc("/v", "1") != sc("/v", "1.0") || sc("/v", "1") != sc("/v", "1e0") {
		t.Fatal("numeric scope identity broken")
	}
	// Different scalar types never share a scope.
	distinct := []string{
		sc("/v", `"1"`), sc("/v", `1`), sc("/v", `true`), sc("/v", `null`),
	}
	for i := 0; i < len(distinct); i++ {
		for j := i + 1; j < len(distinct); j++ {
			if distinct[i] == distinct[j] {
				t.Fatalf("scalar scopes %d and %d collide", i, j)
			}
		}
	}
	// Different pointer or value changes the scope.
	if sc("/a", `1`) == sc("/b", `1`) {
		t.Fatal("pointer must participate in scope")
	}
	if sc("/a", `1`) == sc("/a", `2`) {
		t.Fatal("value must participate in scope")
	}
	// Missing-field matching cannot be smuggled through a null filter scope.
	// (The scopes above already cover null vs everything.)
	var nilP *Predicate
	if nilP.Scope() != "" {
		t.Fatal("nil predicate scope must be empty")
	}
}
