package selection

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
)

type Predicate struct {
	Pointer string
	Value   any
}

func Parse(pointer, value string) (*Predicate, error) {
	if pointer == "" && value == "" {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal([]byte(value), &v); err != nil {
		return nil, err
	}
	return &Predicate{pointer, v}, nil
}
func (p *Predicate) Scope() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(p.Pointer+fmt.Sprint(p.Value))))
}
func (p *Predicate) Match(raw []byte) bool {
	if p == nil {
		return true
	}
	var v map[string]any
	json.Unmarshal(raw, &v)
	return fmt.Sprint(v[strings.TrimPrefix(p.Pointer, "/")]) == fmt.Sprint(p.Value)
}
