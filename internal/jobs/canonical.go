package jobs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// CanonicalJSON re-encodes a JSON document into a deterministic byte sequence:
// object keys sorted, insignificant whitespace removed, strings escaped
// identically.
//
// It normalizes *representation*, not *value*. Numbers keep their original
// literal (via json.Number) for two reasons: decoding into float64 would silently
// destroy precision on large integers, and 1 versus 1.0 is a difference the
// caller actually wrote. Two requests that differ only in numeric spelling are
// therefore treated as different requests, which is the safe direction — it
// produces a conflict rather than silently replaying a different job.
func CanonicalJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}
	// Reject trailing content such as `{} {}`, which would otherwise be silently
	// truncated to the first document.
	if dec.More() {
		return nil, fmt.Errorf("unexpected trailing content after json value")
	}

	var buf bytes.Buffer
	if err := writeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if t {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case json.Number:
		buf.WriteString(t.String())
	case string:
		return writeCanonicalString(buf, t)
	case []any:
		buf.WriteByte('[')
		for i, item := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonicalString(buf, k); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := writeCanonical(buf, t[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("unsupported json value of type %T", v)
	}
	return nil
}

// writeCanonicalString delegates escaping to encoding/json so that the encoding
// matches what the database and every client already agree on. HTML escaping is
// disabled because it is a browser-safety transform, not part of JSON identity,
// and leaving it on would make the fingerprint depend on whether a payload
// happened to contain "<".
func writeCanonicalString(buf *bytes.Buffer, s string) error {
	var tmp bytes.Buffer
	enc := json.NewEncoder(&tmp)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return fmt.Errorf("encode string: %w", err)
	}
	buf.WriteString(strings.TrimRight(tmp.String(), "\n"))
	return nil
}
