package memory_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/homemade/pith/sendstate/memory"
)

// Example_contentHashDedupe shows the typical content-dedupe pattern:
// suppress re-sending an operation whose content is byte-identical to
// the last successful send for the same key.
//
// The pattern:
//
//  1. Use a stable scope identifier (here, profileID) as the key —
//     the "slot" each operation lands in.
//  2. Canonicalise the operation's content (sorted keys for stable
//     JSON) so map iteration order doesn't leak into the hash.
//  3. sha256 the canonical bytes and hex-encode a prefix as the
//     content fingerprint.
//  4. Read the entry for the key and skip when [sendstate.Entry.Seen] reports
//     the same fingerprint; otherwise perform the operation and
//     RecordAsSent on success.
//
// Same scope + same content is suppressed; a content change for the
// same scope, or the same content under a different scope, proceeds.
func Example_contentHashDedupe() {
	store := memory.New(24 * time.Hour)
	ctx := context.Background()

	contentHash := func(body map[string]any) string {
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
		return hex.EncodeToString(sum[:8])
	}

	handle := func(scope string, body map[string]any) {
		content := contentHash(body)
		entry, _ := store.ReadEntry(ctx, scope)
		if entry.Seen(content) {
			fmt.Printf("skip: %s raised=%v\n", scope, body["raised"])
			return
		}
		fmt.Printf("send: %s raised=%v\n", scope, body["raised"])
		_ = store.RecordAsSent(ctx, scope, content)
	}

	// First call for this scope+content: recorded.
	handle("p-1", map[string]any{"goal": 1000, "raised": 350})
	// Same scope+content as above: suppressed.
	handle("p-1", map[string]any{"goal": 1000, "raised": 350})
	// Content change (raised: 350 → 425) → different hash, proceeds.
	handle("p-1", map[string]any{"goal": 1000, "raised": 425})
	// Same body, different scope (p-2) → different slot, proceeds.
	handle("p-2", map[string]any{"goal": 1000, "raised": 425})

	// Output:
	// send: p-1 raised=350
	// skip: p-1 raised=350
	// send: p-1 raised=425
	// send: p-2 raised=425
}
