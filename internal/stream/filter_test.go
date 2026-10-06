package stream_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"logfollow/internal/logdir"
	"logfollow/internal/testutil"
)

func filterQuery(field, value string) string {
	q := url.Values{}
	q.Set("field", field)
	q.Set("value", value)
	return q.Encode()
}

func expectReject(t *testing.T, baseURL, rawQuery, token string, wantStatus int, wantReason string) {
	t.Helper()
	target := baseURL + "/stream"
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Last-Event-ID", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != wantStatus || !strings.Contains(string(body), wantReason) {
		t.Fatalf("resume = %d %s, want %d containing %q", resp.StatusCode, body, wantStatus, wantReason)
	}
}

// TestFilterCheckpointsAndOffsets is the byte-evidence test for a filtered
// subscription: hits, bad lines and content-free checkpoints all report the
// end-of-full-line byte offset of the ORIGINAL file; a half line advances
// nothing until its newline arrives. Bad JSON is never filtered away.
func TestFilterCheckpointsAndOffsets(t *testing.T) {
	p, dir := newProducer(t, "run-filter")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.ConnectPath(ctx, filterQuery("/level", `"error"`), ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	var off int64
	appendLine := func(raw string) {
		t.Helper()
		if err := p.AppendLine([]byte(raw)); err != nil {
			t.Fatal(err)
		}
		off += int64(len(raw)) + 1
	}
	next := func(want string) testutil.SSEEvent {
		t.Helper()
		ev, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
			return e.Event == want
		})
		if err != nil {
			t.Fatalf("wait %s: %v", want, err)
		}
		if got := testutil.RequireIntField(t, ev, "offset"); got != off {
			t.Fatalf("%s offset = %d, want full-line end %d", want, got, off)
		}
		return ev
	}

	// 1. Complete line that does not match -> checkpoint, no original content.
	appendLine(`{"seq":1,"level":"info"}`)
	cp1 := next("checkpoint")
	var data map[string]json.RawMessage
	if err := json.Unmarshal(cp1.Data, &data); err != nil {
		t.Fatal(err)
	}
	if _, present := data["record"]; present {
		t.Fatalf("checkpoint must not carry record content: %s", cp1.Data)
	}
	if _, present := data["line"]; present {
		t.Fatalf("checkpoint must not carry line content: %s", cp1.Data)
	}

	// 2. Complete line with invalid JSON -> bad_json despite the filter.
	appendLine(`{"level":"error", broken`)
	bad := next("bad_json")
	if line, _ := bad.Field("line"); !strings.Contains(line.(string), "broken") {
		t.Fatalf("bad_json line = %v", line)
	}

	// 3. Matching nested-shape record at top level.
	appendLine(`{"seq":3,"level":"error"}`)
	rec := next("record")
	recMap, _ := rec.Field("record")
	if recMap.(map[string]any)["seq"] != float64(3) {
		t.Fatalf("record = %s", rec.Data)
	}

	// Every emitted id decodes and carries the same non-empty filter scope;
	// checkpoints and records are interchangeable as resume tokens.
	var scope string
	for _, ev := range []testutil.SSEEvent{cp1, bad, rec} {
		c, err := logdir.DecodeCursor(ev.ID)
		if err != nil {
			t.Fatalf("decode id: %v", err)
		}
		if c.Scope == "" {
			t.Fatalf("filtered event id has empty scope: %s", ev.ID)
		}
		if scope == "" {
			scope = c.Scope
		} else if c.Scope != scope {
			t.Fatalf("scope drift: %q vs %q", c.Scope, scope)
		}
	}

	// 4. Half a line: no progress of any kind may be emitted.
	p.Append([]byte(`{"seq":4,"level":"err`))
	if got := testutil.Drain(cli.Events(), 150*time.Millisecond); len(got) != 0 {
		t.Fatalf("half line produced progress: %+v", got)
	}
	p.Append([]byte(`or"}` + "\n"))
	off4 := off + int64(len(`{"seq":4,"level":"error"}`)) + 1
	off = off4
	// "error" match once the WHOLE line exists, and the offset spans it fully.
	next("record")

	// 5. A non-matching half-line must likewise not checkpoint early.
	p.Append([]byte(`{"seq":5,"level":"inf`))
	if got := testutil.Drain(cli.Events(), 150*time.Millisecond); len(got) != 0 {
		t.Fatalf("non-matching half line produced progress: %+v", got)
	}
	p.Append([]byte(`o"}` + "\n"))
	off = off4 + int64(len(`{"seq":5,"level":"info"}`)) + 1
	next("checkpoint")
}

// TestFilterNestedPointerAndTypes covers nested/array pointers and the
// string-vs-number distinction over real HTTP events.
func TestFilterNestedPointerAndTypes(t *testing.T) {
	p, dir := newProducer(t, "run-types")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.ConnectPath(ctx, filterQuery("/meta/level", `"error"`), ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	must := func(raw string) {
		if err := p.AppendLine([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	must(`{"meta":{"level":"info"}}`)
	if ev, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "checkpoint"
	}); err != nil {
		t.Fatalf("nested miss checkpoint: %v", err)
	} else if seg := testutil.RequireIntField(t, ev, "segment"); seg != 1 {
		t.Fatalf("seg = %d", seg)
	}
	must(`{"meta":{"level":"error","n":7}}`)
	hit, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("nested hit: %v", err)
	}
	if m, _ := hit.Field("record"); m.(map[string]any)["meta"].(map[string]any)["n"] != float64(7) {
		t.Fatalf("hit payload = %s", hit.Data)
	}
	// Missing nested field is not a hit.
	must(`{"meta":{}}`)
	if _, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "checkpoint"
	}); err != nil {
		t.Fatalf("missing nested -> checkpoint: %v", err)
	}
}

// TestFilterNumericIdentityHTTP: numeric equality is by exact value, so a
// filter of 1 matches 1.0 but the string "1" is a checkpoint.
func TestFilterNumericIdentityHTTP(t *testing.T) {
	p, dir := newProducer(t, "run-num")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.ConnectPath(ctx, filterQuery("/v", `1`), ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	must := func(raw string) {
		if err := p.AppendLine([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	must(`{"v":"1"}`) // string: miss
	if ev, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "checkpoint"
	}); err != nil {
		t.Fatalf("string 1 must not match number 1: %v", err)
	} else if _, present := ev.Field("record"); present {
		t.Fatal("checkpoint leaked content")
	}
	must(`{"v":1.0}`) // numerically equal: hit
	if _, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	}); err != nil {
		t.Fatalf("1.0 must match numeric filter 1: %v", err)
	}
	must(`{"v":null}`) // null is not 1
	if _, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "checkpoint"
	}); err != nil {
		t.Fatalf("null must not match number 1: %v", err)
	}
}

// TestCheckpointResumeAndScopeLock proves: (a) a checkpoint token resumes a
// filtered stream exactly at the following full line with no replay/gap;
// (b) the token is rejected when the filter changes or is dropped; (c) an
// unfiltered token cannot start a filtered subscription; (d) a fresh filtered
// subscriber sees pre-existing matching history (no silent tail jump).
func TestCheckpointResumeAndScopeLock(t *testing.T) {
	p, dir := newProducer(t, "run-scope")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := filterQuery("/level", `"error"`)

	// Pre-existing history, written before any subscriber.
	l1 := `{"seq":1,"level":"info"}` + "\n"
	l2 := `{"seq":2,"level":"error"}` + "\n"
	l3 := `{"seq":3,"level":"info"}` + "\n"
	for _, l := range []string{l1, l2, l3} {
		if err := p.Append([]byte(l)); err != nil {
			t.Fatal(err)
		}
	}

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.ConnectPath(ctx, q, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Fresh filtered subscriber must see the history as checkpoint, record,
	// checkpoint (not start at the tail).
	cp1, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "checkpoint"
	})
	if err != nil {
		t.Fatal(err)
	}
	if off := testutil.RequireIntField(t, cp1, "offset"); off != int64(len(l1)) {
		t.Fatalf("cp1 offset = %d want %d", off, len(l1))
	}
	rec2, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool { return e.Event == "record" })
	if err != nil {
		t.Fatal(err)
	}
	cp3, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool { return e.Event == "checkpoint" })
	if err != nil {
		t.Fatal(err)
	}
	if off := testutil.RequireIntField(t, cp3, "offset"); off != int64(len(l1)+len(l2)+len(l3)) {
		t.Fatalf("cp3 offset = %d", off)
	}
	cli.Close()

	// Offline growth: another hit and a miss.
	l4 := `{"seq":4,"level":"error"}` + "\n"
	l5 := `{"seq":5,"level":"info"}` + "\n"
	for _, l := range []string{l4, l5} {
		if err := p.Append([]byte(l)); err != nil {
			t.Fatal(err)
		}
	}

	// Resume from the checkpoint token: first event must be record seq 4 at
	// exactly l1+l2+l3+l4, with no replay of seq 2.
	cli2 := testutil.NewSSEClient(srv.URL, 0)
	if err := cli2.ConnectPath(ctx, q, cp3.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	defer cli2.Close()
	next4, err := testutil.WaitForEvent(cli2.Events(), eventTimeout, func(e testutil.SSEEvent) bool { return e.Event == "record" })
	if err != nil {
		t.Fatal(err)
	}
	wantOff := int64(len(l1) + len(l2) + len(l3) + len(l4))
	if off := testutil.RequireIntField(t, next4, "offset"); off != wantOff {
		t.Fatalf("resume offset = %d want %d", off, wantOff)
	}
	if m, _ := next4.Field("record"); m.(map[string]any)["seq"] != float64(4) {
		t.Fatalf("resumed record = %s", next4.Data)
	}
	if _, err := testutil.WaitForEvent(cli2.Events(), eventTimeout, func(e testutil.SSEEvent) bool { return e.Event == "checkpoint" }); err != nil {
		t.Fatalf("post-resume checkpoint: %v", err)
	}
	cli2.Close()

	// Cursor scope lock: every condition mismatch is an explicit 409, never a
	// silent acceptance that would skip history visible under the new filter.
	expectReject(t, srv.URL, "", cp3.ID, http.StatusConflict, "scope_mismatch")               // no filter
	expectReject(t, srv.URL, "", rec2.ID, http.StatusConflict, "scope_mismatch")              // record token, no filter
	expectReject(t, srv.URL, filterQuery("/level", `"warn"`), cp3.ID, 409, "scope_mismatch")  // other value
	expectReject(t, srv.URL, filterQuery("/other", `"error"`), cp3.ID, 409, "scope_mismatch") // other field

	// Same condition resumes cleanly (200; check only the handshake).
	if resp, err := http.Get(srv.URL + "/stream?" + q + "&cursor=" + url.QueryEscape(cp3.ID)); err != nil {
		t.Fatal(err)
	} else {
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("same-scope resume status = %d, want 200", resp.StatusCode)
		}
	}

	// An unfiltered (v1, scope-less) cursor cannot open a filtered subscription.
	plain := logdir.EncodeCursor(logdir.Cursor{RunID: "run-scope", Segment: 1, Offset: 0})
	expectReject(t, srv.URL, q, plain, http.StatusConflict, "scope_mismatch")
}

// TestInvalidFilterParams: malformed filter definitions are 400 before the
// stream opens.
func TestInvalidFilterParams(t *testing.T) {
	_, dir := newProducer(t, "run-badfilter")
	srv := testServer(t, dir)
	for _, q := range []string{
		"field=/level",               // value missing
		`value=%22error%22`,          // field missing
		"field=/level&value=notjson", // value is not JSON
		"field=/level&value=%5B1%5D", // array value
		"field=no-leading-slash&value=1",
		"field=/a~2b&value=1", // bad pointer escape
	} {
		resp, err := http.Get(srv.URL + "/stream?" + q)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "invalid_filter") {
			t.Fatalf("query %q -> %d %s, want 400 invalid_filter", q, resp.StatusCode, body)
		}
	}
}

// TestFilteredFatalEvidence ensures terminators still carry explicit evidence
// under a filter, and the fatal cursor is the last FULL-line boundary.
func TestFilteredFatalEvidence(t *testing.T) {
	p, dir := newProducer(t, "run-ffatal")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q := filterQuery("/level", `"error"`)

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.ConnectPath(ctx, q, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	good := `{"seq":1,"level":"error"}` + "\n"
	dangling := `{"seq":2,"level":"error"` // no newline, sealed away
	if err := p.Append([]byte(good)); err != nil {
		t.Fatal(err)
	}
	if _, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool { return e.Event == "record" }); err != nil {
		t.Fatal(err)
	}
	if err := p.Append([]byte(dangling)); err != nil {
		t.Fatal(err)
	}
	// Half line yields no event...
	if got := testutil.Drain(cli.Events(), 120*time.Millisecond); len(got) != 0 {
		t.Fatalf("dangling half line emitted: %+v", got)
	}
	if err := p.Roll(); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendJSON(3, "seg2"); err != nil {
		t.Fatal(err)
	}
	// ...and sealing it is reported as truncation; cursor offset stays at the
	// end of the last complete line, never inside the dangling bytes.
	fatal, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "fatal"
	})
	if err != nil {
		t.Fatalf("fatal: %v", err)
	}
	if !strings.Contains(string(fatal.Data), logdir.KindTruncated) {
		t.Fatalf("fatal = %s", fatal.Data)
	}
	var body struct {
		Cursor struct {
			Seg   int    `json:"seg"`
			Off   int64  `json:"off"`
			Scope string `json:"scope"`
		} `json:"cursor"`
	}
	if err := json.Unmarshal(fatal.Data, &body); err != nil {
		t.Fatal(err)
	}
	if body.Cursor.Seg != 1 || body.Cursor.Off != int64(len(good)) {
		t.Fatalf("fatal cursor = seg %d off %d, want seg1 off %d", body.Cursor.Seg, body.Cursor.Off, len(good))
	}
	if body.Cursor.Scope == "" {
		t.Fatal("fatal cursor must retain filter scope")
	}
	if _, ok := <-cli.Events(); ok {
		t.Fatal("stream must close after fatal")
	}
}

// TestFilteredResumeDeletedHistory: a filtered resume into a deleted segment
// stays an explicit 409 segment_missing (not masked by the scope check).
func TestFilteredResumeDeletedHistory(t *testing.T) {
	p, dir := newProducer(t, "run-delf")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.AppendLine([]byte(`{"level":"error"}`)); err != nil {
		t.Fatal(err)
	}
	if err := p.Roll(); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendLine([]byte(`{"level":"error"}`)); err != nil {
		t.Fatal(err)
	}
	q := filterQuery("/level", `"error"`)
	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.ConnectPath(ctx, q, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	tok := ""
	if ev, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	}); err != nil {
		t.Fatal(err)
	} else {
		tok = ev.ID
	}
	cli.Close()
	if err := p.Delete(1); err != nil {
		t.Fatal(err)
	}
	expectReject(t, srv.URL, q, tok, http.StatusConflict, "segment_missing")
}
