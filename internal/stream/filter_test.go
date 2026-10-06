package stream_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"logfollow/internal/testutil"
)

// filteredQuery builds the raw query string for a field subscription.
func filteredQuery(pointer, value string) string {
	return "field=" + url.QueryEscape(pointer) + "&value=" + url.QueryEscape(value)
}

func connectFilter(t *testing.T, ctx context.Context, baseURL, pointer, value, cursor string) *testutil.SSEClient {
	t.Helper()
	cli := testutil.NewSSEClient(baseURL, 0)
	if err := cli.ConnectQuery(ctx, filteredQuery(pointer, value), cursor); err != nil {
		t.Fatalf("connect filter: %v", err)
	}
	t.Cleanup(cli.Close)
	return cli
}

func mustWait(t *testing.T, ch <-chan testutil.SSEEvent, pred func(testutil.SSEEvent) bool, what string) testutil.SSEEvent {
	t.Helper()
	ev, err := testutil.WaitForEvent(ch, eventTimeout, pred)
	if err != nil {
		t.Fatalf("wait %s: %v", what, err)
	}
	return ev
}

func isEvent(kind string) func(testutil.SSEEvent) bool {
	return func(e testutil.SSEEvent) bool { return e.Event == kind }
}

func eventOffset(t *testing.T, ev testutil.SSEEvent) int64 {
	t.Helper()
	return testutil.RequireIntField(t, ev, "offset")
}

// TestFilterHitsCheckpointsAndBadJSON drives a real NDJSON file through a
// filtered subscription: hits carry the record, complete misses carry a
// content-free checkpoint at the same byte boundary, and bad JSON is still
// delivered as an error rather than being filtered away.
func TestFilterHitsCheckpointsAndBadJSON(t *testing.T) {
	p, dir := newProducer(t, "run-F")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := connectFilter(t, ctx, srv.URL, "/meta/level", `"error"`, "")

	lines := []string{
		`{"meta":{"level":"error"},"msg":"e1"}` + "\n", // hit
		`{"meta":{"level":"info"},"msg":"i1"}` + "\n",  // checkpoint
		`{"meta":{"level":"error"},"msg":"e2"}` + "\n", // hit
		`{"meta":null,"msg":"n1"}` + "\n",              // missing (null parent)
		`{"msg":"nolevel"}` + "\n",                     // missing field
		`{"meta":{"level":500}}` + "\n",                // number, not string
		`{"oops":` + "\n",                              // bad JSON: still delivered
		`{"meta":{"level":"error"},"msg":"e3"}` + "\n", // hit survives bad json
	}
	for _, l := range lines {
		if err := p.Append([]byte(l)); err != nil {
			t.Fatal(err)
		}
	}

	// Event 1: record hit.
	ev1 := mustWait(t, cli.Events(), isEvent("record"), "first hit")
	if off := eventOffset(t, ev1); off != int64(len(lines[0])) {
		t.Fatalf("hit1 offset = %d, want %d", off, len(lines[0]))
	}
	rec, _ := ev1.Field("record")
	if rec.(map[string]any)["msg"] != "e1" {
		t.Fatalf("hit1 record = %s", ev1.Data)
	}
	if testutil.RequireIntField(t, ev1, "segment") != 1 {
		t.Fatalf("hit1 segment = %s", ev1.Data)
	}
	if !strings.HasPrefix(ev1.ID, "v2:") {
		t.Fatalf("filtered cursor = %q, want v2: token", ev1.ID)
	}

	// Event 2: checkpoint with no original line content.
	cp1 := mustWait(t, cli.Events(), isEvent("checkpoint"), "checkpoint")
	if off := eventOffset(t, cp1); off != int64(len(lines[0])+len(lines[1])) {
		t.Fatalf("checkpoint offset = %d, want full-line end %d", off, len(lines[0])+len(lines[1]))
	}
	var cpBody map[string]any
	if err := json.Unmarshal(cp1.Data, &cpBody); err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"record", "line", "error"} {
		if _, ok := cpBody[banned]; ok {
			t.Fatalf("checkpoint must not carry %q: %s", banned, cp1.Data)
		}
	}
	if cp1.ID == "" || !strings.HasPrefix(cp1.ID, "v2:") {
		t.Fatalf("checkpoint must carry a v2 resumable id, got %q", cp1.ID)
	}

	// Event 3: second hit.
	ev2 := mustWait(t, cli.Events(), isEvent("record"), "second hit")
	if r, _ := ev2.Field("record"); r.(map[string]any)["msg"] != "e2" {
		t.Fatalf("hit2 = %s", ev2.Data)
	}
	var wantEnd int64
	for _, l := range lines[:3] {
		wantEnd += int64(len(l))
	}
	if off := eventOffset(t, ev2); off != wantEnd {
		t.Fatalf("hit2 offset = %d, want %d", off, wantEnd)
	}

	// Events 4-6: three checkpoints (null parent, missing field, number).
	var lastCP testutil.SSEEvent
	for i, what := range []string{"null-parent", "missing", "wrong-type"} {
		lastCP = mustWait(t, cli.Events(), isEvent("checkpoint"), what+" checkpoint")
		var sum int64
		for _, l := range lines[:4+i] {
			sum += int64(len(l))
		}
		if off := eventOffset(t, lastCP); off != sum {
			t.Fatalf("%s offset = %d, want %d (complete line end)", what, off, sum)
		}
	}

	// Event 7: bad JSON is delivered even under a filter.
	bad := mustWait(t, cli.Events(), isEvent("bad_json"), "bad_json")
	if line, _ := bad.Field("line"); line != `{"oops":` {
		t.Fatalf("bad line = %v", bad.Data)
	}
	var badEnd int64
	for _, l := range lines[:7] {
		badEnd += int64(len(l))
	}
	if off := eventOffset(t, bad); off != badEnd {
		t.Fatalf("bad_json offset = %d, want %d", off, badEnd)
	}

	// Event 8: following continues after the bad line.
	ev3 := mustWait(t, cli.Events(), isEvent("record"), "hit after bad_json")
	if r, _ := ev3.Field("record"); r.(map[string]any)["msg"] != "e3" {
		t.Fatalf("hit3 = %s", ev3.Data)
	}
	var allEnd int64
	for _, l := range lines {
		allEnd += int64(len(l))
	}
	if off := eventOffset(t, ev3); off != allEnd {
		t.Fatalf("hit3 offset = %d, want %d", off, allEnd)
	}
}

// TestFilterNumericAndNullScalarIdentity checks, over HTTP, that numeric
// spellings share identity, string/number stay distinct, and null differs
// from a missing field.
func TestFilterNumericAndNullScalarIdentity(t *testing.T) {
	p, dir := newProducer(t, "run-N")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, l := range []string{
		`{"code":200}` + "\n",
		`{"code":200.0}` + "\n",
		`{"code":2e2}` + "\n",
		`{"code":"200"}` + "\n",
		`{"code":null}` + "\n",
		`{"other":200}` + "\n",
		`{"code":201}` + "\n",
	} {
		if err := p.Append([]byte(l)); err != nil {
			t.Fatal(err)
		}
	}

	// Numeric: three spellings of the same value hit (records); the string,
	// null, missing-field and 201 lines arrive as content-free checkpoints.
	cli := connectFilter(t, ctx, srv.URL, "/code", `200`, "")
	var records, checkpoints int
	drained := testutil.Drain(cli.Events(), 500*time.Millisecond)
	for _, ev := range drained {
		switch ev.Event {
		case "record":
			records++
		case "checkpoint":
			checkpoints++
		default:
			t.Fatalf("unexpected event %s: %s", ev.Event, ev.Data)
		}
	}
	if records != 3 {
		t.Fatalf("numeric hits = %d, want 3 (200 / 200.0 / 2e2)", records)
	}
	if checkpoints != 4 {
		t.Fatalf("checkpoints = %d, want 4 (string, null, missing, 201)", checkpoints)
	}

	// null filter: only the explicit null line hits.
	cli2 := connectFilter(t, ctx, srv.URL, "/code", `null`, "")
	var nullRecords int
	drain2 := testutil.Drain(cli2.Events(), 300*time.Millisecond)
	for _, ev := range drain2 {
		if ev.Event == "record" {
			nullRecords++
		}
	}
	if nullRecords != 1 {
		t.Fatalf("null filter records = %d, want 1 (events %d)", nullRecords, len(drain2))
	}

	// Invalid scalar parameter shapes are 400.
	for _, q := range []string{
		"field=/code", // value missing
		"value=200",   // field missing
		"field=" + url.QueryEscape("code") + "&value=200", // pointer lacks "/"
		"field=/code&value=notjson",                       // value is not JSON
		"field=/code&value=[1,2]",                         // value is not a scalar
	} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/stream?"+q, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("query %q status = %d, want 400", q, resp.StatusCode)
		}
	}
}

// TestCheckpointResumeAcrossSegments verifies that a checkpoint cursor is a
// byte position in the original file, resumes idempotently under the same
// filter, and carries across segment rolls without splicing.
func TestCheckpointResumeAcrossSegments(t *testing.T) {
	p, dir := newProducer(t, "run-R")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Segment 1: miss, hit, miss.
	s1 := [][]byte{
		[]byte(`{"lvl":"info","seq":1}` + "\n"),
		[]byte(`{"lvl":"error","seq":2}` + "\n"),
		[]byte(`{"lvl":"debug","seq":3}` + "\n"),
	}
	for _, b := range s1 {
		if err := p.Append(b); err != nil {
			t.Fatal(err)
		}
	}
	cli := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, "")
	_ = mustWait(t, cli.Events(), isEvent("checkpoint"), "s1 miss1")
	_ = mustWait(t, cli.Events(), isEvent("record"), "s1 hit1")
	cp := mustWait(t, cli.Events(), isEvent("checkpoint"), "s1 miss2 cursor")

	var s1Len int64
	for _, b := range s1 {
		s1Len += int64(len(b))
	}
	if off := eventOffset(t, cp); off != s1Len {
		t.Fatalf("checkpoint at sealed segment end = %d, want %d", off, s1Len)
	}
	if testutil.RequireIntField(t, cp, "segment") != 1 {
		t.Fatal("checkpoint segment = not 1")
	}
	cli.Close()

	// Roll: segment 1 seals, segment 2 grows.
	if err := p.Roll(); err != nil {
		t.Fatal(err)
	}
	s2 := [][]byte{
		[]byte(`{"lvl":"warn","seq":4}` + "\n"),
		[]byte(`{"lvl":"error","seq":5}` + "\n"),
	}
	for _, b := range s2 {
		if err := p.Append(b); err != nil {
			t.Fatal(err)
		}
	}

	// Resume from the checkpoint: nothing from segment 1 is replayed; the
	// segment-2 miss then hit arrive.
	cli2 := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, cp.ID)
	cp2 := mustWait(t, cli2.Events(), isEvent("checkpoint"), "s2 miss after resume")
	if testutil.RequireIntField(t, cp2, "segment") != 2 {
		t.Fatalf("post-roll segment = %s, want 2", cp2.Data)
	}
	if off := eventOffset(t, cp2); off != int64(len(s2[0])) {
		t.Fatalf("s2 first-line checkpoint offset = %d, want %d", off, len(s2[0]))
	}
	hit := mustWait(t, cli2.Events(), isEvent("record"), "s2 hit after resume")
	if r, _ := hit.Field("record"); r.(map[string]any)["seq"] != float64(5) {
		t.Fatalf("resume replay/skip wrong: %s", hit.Data)
	}
	if off := eventOffset(t, hit); off != int64(len(s2[0])+len(s2[1])) {
		t.Fatalf("s2 hit offset = %d, want original-file byte end", off)
	}

	// Re-resuming from the same checkpoint is idempotent: same events again.
	cli3 := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, cp.ID)
	_ = mustWait(t, cli3.Events(), isEvent("checkpoint"), "idempotent s2 checkpoint")
	again := mustWait(t, cli3.Events(), isEvent("record"), "idempotent s2 hit")
	if r, _ := again.Field("record"); r.(map[string]any)["seq"] != float64(5) {
		t.Fatalf("idempotent resume changed history: %s", again.Data)
	}
}

// TestHalfLineMakesNoProgress: an incomplete miss produces no checkpoint and
// no cursor; progress appears only when the line completes, pointing at the
// full line's ending byte.
func TestHalfLineMakesNoProgress(t *testing.T) {
	p, dir := newProducer(t, "run-H")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, "")
	if err := p.Append([]byte(`{"lvl":"info","par`)); err != nil {
		t.Fatal(err)
	}
	if got := testutil.Drain(cli.Events(), 200*time.Millisecond); len(got) != 0 {
		t.Fatalf("half line produced progress events: %+v", got)
	}
	// Complete the miss.
	if err := p.Append([]byte(`t":1}` + "\n")); err != nil {
		t.Fatal(err)
	}
	cp := mustWait(t, cli.Events(), isEvent("checkpoint"), "completed miss")
	full := `{"lvl":"info","part":1}` + "\n"
	if off := eventOffset(t, cp); off != int64(len(full)) {
		t.Fatalf("checkpoint offset = %d, want full line end %d (no half-line advance)", off, len(full))
	}
	// A trailing half of what WILL be a hit must also wait.
	if err := p.Append([]byte(`{"lvl":"erro`)); err != nil {
		t.Fatal(err)
	}
	if got := testutil.Drain(cli.Events(), 200*time.Millisecond); len(got) != 0 {
		t.Fatalf("half of future hit emitted early: %+v", got)
	}
	if err := p.Append([]byte(`r","x":2}` + "\n")); err != nil {
		t.Fatal(err)
	}
	hit := mustWait(t, cli.Events(), isEvent("record"), "completed hit")
	want := int64(len(full) + len(`{"lvl":"error","x":2}`+"\n"))
	if off := eventOffset(t, hit); off != want {
		t.Fatalf("hit offset = %d, want %d", off, want)
	}
}

// TestFilterCursorScopeBinding: a cursor is valid only for its exact
// condition. Different pointer/value or an unfiltered request must be
// rejected instead of silently resuming (which would drop visible history).
func TestFilterCursorScopeBinding(t *testing.T) {
	p, dir := newProducer(t, "run-S")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Append([]byte(`{"lvl":"error"}` + "\n")); err != nil {
		t.Fatal(err)
	}

	// Unfiltered cursor.
	plain := testutil.NewSSEClient(srv.URL, 0)
	if err := plain.Connect(ctx, ""); err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	plainEv := mustWait(t, plain.Events(), isEvent("record"), "unfiltered record")
	if !strings.HasPrefix(plainEv.ID, "v1:") {
		t.Fatalf("unfiltered cursor = %q, want v1:", plainEv.ID)
	}

	// Filtered cursor.
	filt := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, "")
	filtEv := mustWait(t, filt.Events(), isEvent("record"), "filtered record")

	expectReject := func(rawQuery, cursor, label string) {
		t.Helper()
		cli := testutil.NewSSEClient(srv.URL, 0)
		err := cli.ConnectQuery(ctx, rawQuery, cursor)
		he, ok := err.(*testutil.HTTPError)
		if !ok {
			cli.Close()
			t.Fatalf("%s: expected HTTP rejection, got %v", label, err)
		}
		defer cli.Close()
		if he.Status != http.StatusConflict {
			t.Fatalf("%s: status = %d body=%s, want 409", label, he.Status, he.Body)
		}
		if !strings.Contains(string(he.Body), "scope_mismatch") {
			t.Fatalf("%s: body = %s, want scope_mismatch", label, he.Body)
		}
	}

	// Filtered cursor on unfiltered request.
	expectReject("", filtEv.ID, "filtered -> unfiltered")
	// Unfiltered cursor on filtered request.
	expectReject(filteredQuery("/lvl", `"error"`), plainEv.ID, "unfiltered -> filtered")
	// Same pointer, different value.
	expectReject(filteredQuery("/lvl", `"warn"`), filtEv.ID, "changed value")
	// Different pointer, same value.
	expectReject(filteredQuery("/other", `"error"`), filtEv.ID, "changed pointer")
	// Equal numeric identity: 1 vs 1.0 must be the SAME scope (accepted).
	p2, dir2 := newProducer(t, "run-S2")
	srv2 := testServer(t, dir2)
	if err := p2.Append([]byte(`{"n":1}` + "\n")); err != nil {
		t.Fatal(err)
	}
	c1 := connectFilter(t, ctx, srv2.URL, "/n", `1`, "")
	idEv := mustWait(t, c1.Events(), isEvent("record"), "numeric cursor")
	cliEq := testutil.NewSSEClient(srv2.URL, 0)
	if err := cliEq.ConnectQuery(ctx, filteredQuery("/n", `1.0`), idEv.ID); err != nil {
		t.Fatalf("equal numeric spelling rejected: %v", err)
	}
	defer cliEq.Close()

	// A malformed token stays a 400 regardless of filter.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/stream?"+filteredQuery("/lvl", `"error"`)+"&cursor=garbage", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("garbage cursor status = %d, want 400", resp.StatusCode)
	}
}

// TestFilterTruncationAndGapEvidence: with a filter active, sealed-segment
// truncation and segment gaps still terminate with explicit fatal evidence
// (the filter must not let gaps slip past as "no matches").
func TestFilterTruncationAndGapEvidence(t *testing.T) {
	t.Run("truncated sealed segment", func(t *testing.T) {
		p, dir := newProducer(t, "run-T")
		srv := testServer(t, dir)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Complete miss line, then a half line that never gets a newline.
		if err := p.Append([]byte(`{"lvl":"info","a":1}` + "\n")); err != nil {
			t.Fatal(err)
		}
		if err := p.Append([]byte(`{"lvl":"info","half`)); err != nil {
			t.Fatal(err)
		}
		cli := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, "")
		cp := mustWait(t, cli.Events(), isEvent("checkpoint"), "miss before half")
		before := eventOffset(t, cp)
		if err := p.Roll(); err != nil { // seals segment 1 with its trailing half-line
			t.Fatal(err)
		}
		if err := p.Append([]byte(`{"lvl":"error","after":true}` + "\n")); err != nil {
			t.Fatal(err)
		}
		fatal := mustWait(t, cli.Events(), isEvent("fatal"), "truncation fatal")
		if !strings.Contains(string(fatal.Data), `"truncated"`) {
			t.Fatalf("fatal = %s, want truncated", fatal.Data)
		}
		// Diagnostic cursor remains the last complete-line boundary.
		if !strings.Contains(string(fatal.Data), `"off":`+strconv.FormatInt(before, 10)) {
			t.Fatalf("fatal cursor not at last checkpoint %d: %s", before, fatal.Data)
		}
		// The segment-2 hit beyond the truncation is never delivered.
		if got := testutil.Drain(cli.Events(), 150*time.Millisecond); len(got) != 0 {
			t.Fatalf("events delivered past truncation: %+v", got)
		}
	})

	t.Run("history gap", func(t *testing.T) {
		p, dir := newProducer(t, "run-G")
		srv := testServer(t, dir)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := p.Append([]byte(`{"lvl":"info"}` + "\n")); err != nil {
			t.Fatal(err)
		}
		if err := p.Roll(); err != nil {
			t.Fatal(err)
		}
		if err := p.Roll(); err != nil { // active is 3, 2 exists
			t.Fatal(err)
		}
		if err := p.Delete(2); err != nil {
			t.Fatal(err)
		}
		if err := p.Append([]byte(`{"lvl":"error","late":1}` + "\n")); err != nil {
			t.Fatal(err)
		}

		// Fresh filtered connection: the gap is fatal, never a silent skip to
		// the matching line in segment 3.
		cli := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, "")
		fatal := mustWait(t, cli.Events(), func(e testutil.SSEEvent) bool {
			return e.Event == "fatal" || e.Event == "record" && hasMsg(e, "late")
		}, "fatal or leaked record")
		if ev := fatal.Event; ev != "fatal" || !strings.Contains(string(fatal.Data), "segment_gap") {
			t.Fatalf("gap not reported: %s %s", ev, fatal.Data)
		}
	})

	t.Run("resume into gap is 409", func(t *testing.T) {
		p, dir := newProducer(t, "run-G2")
		srv := testServer(t, dir)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.Append([]byte(`{"lvl":"error"}` + "\n")); err != nil {
			t.Fatal(err)
		}
		cli := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, "")
		ev := mustWait(t, cli.Events(), isEvent("record"), "seg1 cursor")
		cli.Close()
		if err := p.Roll(); err != nil {
			t.Fatal(err)
		}
		if err := p.Roll(); err != nil {
			t.Fatal(err)
		}
		if err := p.Delete(2); err != nil {
			t.Fatal(err)
		}
		c := testutil.NewSSEClient(srv.URL, 0)
		err := c.ConnectQuery(ctx, filteredQuery("/lvl", `"error"`), ev.ID)
		he, ok := err.(*testutil.HTTPError)
		if !ok || he.Status != http.StatusConflict || !strings.Contains(string(he.Body), "segment_gap") {
			if c != nil {
				c.Close()
			}
			t.Fatalf("resume into gap = %v, want 409 segment_gap", err)
		}
	})
}

// TestFilterFileReplacementAndTruncate keeps the replacement/copy-truncate
// guarantees intact while a filter is active.
func TestFilterFileReplacementAndTruncate(t *testing.T) {
	p, dir := newProducer(t, "run-P")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Append([]byte(`{"lvl":"info","v":1}` + "\n")); err != nil {
		t.Fatal(err)
	}
	cli := connectFilter(t, ctx, srv.URL, "/lvl", `"error"`, "")
	_ = mustWait(t, cli.Events(), isEvent("checkpoint"), "initial checkpoint")

	if err := p.ReplaceActive([]byte(`{"lvl":"error","v":2}` + "\n")); err != nil {
		t.Fatal(err)
	}
	fatal := mustWait(t, cli.Events(), isEvent("fatal"), "rename-over fatal")
	if !strings.Contains(string(fatal.Data), "segment_replaced") {
		t.Fatalf("replace fatal = %s, want segment_replaced", fatal.Data)
	}

	// In-place shrink on a fresh subscription.
	p2, dir2 := newProducer(t, "run-P2")
	srv2 := testServer(t, dir2)
	if err := p2.Append([]byte(strings.Repeat(`{"lvl":"info"}`+"\n", 4))); err != nil {
		t.Fatal(err)
	}
	cli2 := connectFilter(t, ctx, srv2.URL, "/lvl", `"error"`, "")
	_ = mustWait(t, cli2.Events(), isEvent("checkpoint"), "checkpoint before shrink")
	if err := p2.TruncateActive(0); err != nil {
		t.Fatal(err)
	}
	fatal2 := mustWait(t, cli2.Events(), isEvent("fatal"), "copy-truncate fatal")
	if !strings.Contains(string(fatal2.Data), "segment_replaced") {
		t.Fatalf("shrink fatal = %s, want segment_replaced", fatal2.Data)
	}
}

func hasMsg(e testutil.SSEEvent, msg string) bool {
	r, ok := e.Field("record")
	if !ok {
		return false
	}
	m, ok := r.(map[string]any)
	return ok && m["msg"] == msg
}
