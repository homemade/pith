package dedupe_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/homemade/pith/dedupe"
	"github.com/joineduptech/doc/sequencerec"
)

// Hasher abstracts the hash step so it can be recorded as its own
// participant in the sequence diagram. Production code typically
// calls sha256 inline; factoring it out here surfaces both the JSON
// input and the hex output in the recorded interactions.
type Hasher interface {
	Hash(canon []byte) string
}

type sha256Hasher struct{}

func (sha256Hasher) Hash(canon []byte) string {
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:8])
}

// recHasher records Hash calls into a [sequencerec.Recorder] before
// delegating to the wrapped implementation.
type recHasher struct {
	inner Hasher
	rec   *sequencerec.Recorder
	name  string
}

func (r *recHasher) Hash(canon []byte) string {
	out := r.inner.Hash(canon)
	r.rec.Record(r.name, "Hash", []any{rawJSON(canon)}, []any{out})
	return out
}

// rawJSON renders a JSON document without Go-style %q escaping when
// the recorder formats argument values. The recorder's value
// formatter wraps plain string args in quotes; named types that
// satisfy [fmt.Stringer] short-circuit that path.
type rawJSON []byte

func (r rawJSON) String() string { return string(r) }

// recDeduper records Deduper calls before delegating.
type recDeduper struct {
	inner dedupe.Deduper
	rec   *sequencerec.Recorder
	name  string
}

func (r *recDeduper) SeenInWindow(ctx context.Context, key string) (bool, error) {
	seen, err := r.inner.SeenInWindow(ctx, key)
	r.rec.Record(r.name, "SeenInWindow", []any{key}, []any{seen, err})
	return seen, err
}

func (r *recDeduper) RecordSent(ctx context.Context, key string, ttl time.Duration) error {
	err := r.inner.RecordSent(ctx, key, ttl)
	r.rec.Record(r.name, "RecordSent", []any{key, ttl}, []any{err})
	return err
}

// TestContentHashKey exercises the content-hash-in-key pattern across
// four labelled scenarios, capturing the call sequence per scenario
// into sequence_test.md (same basename as this file). Both the
// Hasher and the Deduper are wrapped in recording decorators so the
// diagram shows the canonical JSON → hex hash → dedupe-key chain
// for each scenario.
func TestContentHashKey(t *testing.T) {
	rec := sequencerec.New()
	t.Cleanup(func() { rec.WriteMermaid(t) })

	// Tag setup events with the first scenario's name so the
	// construction calls appear in that diagram section.
	rec.SetScope("first send is recorded")

	d := &recDeduper{
		inner: dedupe.NewMemoryDeduper(),
		rec:   rec,
		name:  "Deduper",
	}
	rec.Record("Deduper", "new", nil, []any{rawJSON("Deduper")})

	h := &recHasher{
		inner: sha256Hasher{},
		rec:   rec,
		name:  "Hasher",
	}
	rec.Record("Hasher", "new", nil, []any{rawJSON("Hasher")})

	rec.SetScope("")

	ctx := context.Background()
	ttl := time.Hour

	hashKey := func(scope string, body map[string]any) string {
		keys := make([]string, 0, len(body))
		for k := range body {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ordered := make([][2]any, 0, len(keys))
		for _, k := range keys {
			ordered = append(ordered, [2]any{k, body[k]})
		}
		canon, _ := json.Marshal(ordered)
		return scope + ":" + h.Hash(canon)
	}

	// handle runs the dedupe-then-record dance for one call and asserts
	// the outcome. The Recorder captures the actual sequence; the
	// assertion is what makes this a real test.
	handle := func(t *testing.T, scope string, body map[string]any, wantSent bool) {
		t.Helper()
		key := hashKey(scope, body)
		rec.Note("key = " + key)

		seen, err := d.SeenInWindow(ctx, key)
		if err != nil {
			t.Fatalf("SeenInWindow: %v", err)
		}
		if wantSent && seen {
			t.Fatalf("expected to send, but key already seen")
		}
		if !wantSent && !seen {
			t.Fatalf("expected to skip (already seen), but key was new")
		}
		if !seen {
			if err := d.RecordSent(ctx, key, ttl); err != nil {
				t.Fatalf("RecordSent: %v", err)
			}
		}
	}

	rec.Run(t, "first send is recorded", func(t *testing.T) {
		handle(t, "p-1", map[string]any{"goal": 1000, "raised": 350}, true)
	})

	rec.Run(t, "exact duplicate is suppressed", func(t *testing.T) {
		handle(t, "p-1", map[string]any{"goal": 1000, "raised": 350}, false)
	})

	rec.Run(t, "content change is sent", func(t *testing.T) {
		handle(t, "p-1", map[string]any{"goal": 1000, "raised": 425}, true)
	})

	rec.Run(t, "different scope is sent", func(t *testing.T) {
		handle(t, "p-2", map[string]any{"goal": 1000, "raised": 425}, true)
	})
}
