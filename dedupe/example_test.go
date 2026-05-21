package dedupe_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/homemade/pith/dedupe"
)

// Example_contentHashKey shows the typical key-construction pattern
// for deduplicating operations whose stable bytes are derivable from
// the application's own data.
//
// The pattern:
//
//  1. Canonicalise the operation's content (sorted keys for stable
//     JSON) so map iteration order doesn't leak into the hash.
//  2. sha256 the canonical bytes and hex-encode a prefix as the
//     content fingerprint.
//  3. Build the key from the scope (here, profileID) plus the hash.
//  4. Skip when SeenInWindow returns true; otherwise perform the
//     operation and RecordSent on success.
func Example_contentHashKey() {
	d := dedupe.NewMemoryDeduper()
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
		sum := sha256.Sum256(canon)
		return scope + ":" + hex.EncodeToString(sum[:8])
	}

	handle := func(scope string, body map[string]any) {
		key := hashKey(scope, body)
		seen, _ := d.SeenInWindow(ctx, key)
		if seen {
			fmt.Printf("skip: %s raised=%v\n", scope, body["raised"])
			return
		}
		fmt.Printf("send: %s raised=%v\n", scope, body["raised"])
		_ = d.RecordSent(ctx, key, ttl)
	}

	// First call for this scope+content: recorded.
	handle("p-1", map[string]any{"goal": 1000, "raised": 350})
	// Same scope+content as above: suppressed.
	handle("p-1", map[string]any{"goal": 1000, "raised": 350})
	// Content change (raised: 350 → 425) → different hash, proceeds.
	handle("p-1", map[string]any{"goal": 1000, "raised": 425})
	// Same body, different scope (p-2) → different key, proceeds.
	handle("p-2", map[string]any{"goal": 1000, "raised": 425})

	// Output:
	// send: p-1 raised=350
	// skip: p-1 raised=350
	// send: p-1 raised=425
	// send: p-2 raised=425
}
