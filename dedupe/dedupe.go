// Package dedupe provides short-window suppression of repeated
// operations keyed by a caller-supplied string.
//
// The contract is record-on-success: callers register a key only after
// the gated operation has actually succeeded, so transport failures
// don't lock out legitimate retries.
//
// A common key-construction pattern is to hash the stable content of
// the operation and prefix the hash with any scoping identifiers the
// application needs (tenant, resource, …). See
// [Example_contentHashKey].
package dedupe

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Deduper tracks recently-recorded keys within a time-to-live window.
type Deduper interface {
	// SeenInWindow reports whether key has a non-expired entry
	// recorded by a prior successful operation.
	//
	//   true  → suppress (caller should skip the operation).
	//   false → proceed (caller should perform the operation, then
	//                    call RecordSent on success).
	//
	// Backing-store failures must be treated as fail-open by callers
	// (return false), so a dedupe outage degrades to "operation
	// proceeds" rather than dropping legitimate work.
	SeenInWindow(ctx context.Context, key string) (bool, error)

	// RecordSent registers key with the given TTL. Idempotent — a
	// second RecordSent for the same key refreshes the expiry.
	RecordSent(ctx context.Context, key string, ttl time.Duration) error
}

// MemoryDeduper is an in-process Deduper backed by a sync.Map. It is
// best-effort within one process; entries recorded in one process are
// invisible to others. Use a shared-backing implementation when
// cross-process coordination is required.
type MemoryDeduper struct {
	entries sync.Map // key: string → value: time.Time (expiry instant)
	ops     atomic.Uint64
}

// NewMemoryDeduper returns a ready-to-use MemoryDeduper.
func NewMemoryDeduper() *MemoryDeduper {
	return &MemoryDeduper{}
}

// SeenInWindow reports whether key has a non-expired entry. Expired
// entries are treated as a miss.
func (m *MemoryDeduper) SeenInWindow(_ context.Context, key string) (bool, error) {
	v, ok := m.entries.Load(key)
	if !ok {
		return false, nil
	}
	expiry := v.(time.Time)
	if !time.Now().Before(expiry) {
		m.entries.Delete(key)
		return false, nil
	}
	return true, nil
}

// RecordSent stores key with expiry now+ttl. Overwrites any existing
// entry for the same key (refreshes the expiry).
func (m *MemoryDeduper) RecordSent(_ context.Context, key string, ttl time.Duration) error {
	m.entries.Store(key, time.Now().Add(ttl))
	if m.ops.Add(1)%1000 == 0 {
		m.sweep()
	}
	return nil
}

// sweep removes all expired entries. Triggered every ~1000 writes by
// RecordSent so memory doesn't grow unbounded for never-revisited keys.
func (m *MemoryDeduper) sweep() {
	now := time.Now()
	m.entries.Range(func(k, v interface{}) bool {
		if !now.Before(v.(time.Time)) {
			m.entries.Delete(k)
		}
		return true
	})
}
