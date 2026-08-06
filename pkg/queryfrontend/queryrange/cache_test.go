package queryrange

import (
	"context"
	"testing"
	"time"
)

func TestCacheKeyChangesWithShardConfig(t *testing.T) {
	shardState := NewShardState()
	cache := NewQueryRangeCache(shardState)

	tenantID := "tenant-1"
	query := "rate(http_requests_total[5m])"
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	step := 15 * time.Second

	// Set initial shard configuration: 3 shards
	shardState.SetConfig(ShardConfig{
		TenantID: tenantID,
		Shards:   []ShardID{"shard-1", "shard-2", "shard-3"},
		Version:  1,
	})
	shardState.CompleteTransition(tenantID)

	key1, err := cache.BuildCacheKey(tenantID, query, start, end, step)
	if err != nil {
		t.Fatalf("failed to build cache key: %v", err)
	}

	// Cache a result under the old shard config
	ctx := context.Background()
	cache.Set(ctx, key1, []byte("result-v1"))

	// Verify the cached result can be retrieved
	result, found := cache.Get(ctx, key1)
	if !found {
		t.Error("expected cache hit under same shard config")
	}
	if string(result) != "result-v1" {
		t.Errorf("expected 'result-v1', got %q", string(result))
	}

	// Now change shard configuration: scale out to 5 shards
	shardState.SetConfig(ShardConfig{
		TenantID: tenantID,
		Shards:   []ShardID{"shard-1", "shard-2", "shard-3", "shard-4", "shard-5"},
		Version:  2,
	})

	key2, err := cache.BuildCacheKey(tenantID, query, start, end, step)
	if err != nil {
		t.Fatalf("failed to build cache key after rebalance: %v", err)
	}

	// Cache keys must differ when shard config changes
	if key1.String() == key2.String() {
		t.Errorf("cache keys must differ after shard rebalance! old: %s new: %s", key1.String(), key2.String())
	}

	// During transition, cache should be bypassed for data completeness
	_, found = cache.Get(ctx, key1)
	if found {
		t.Error("during shard transition, cache should be bypassed to guarantee data completeness")
	}

	// After transition completes, stale entries from old epoch remain but
	// new queries use the new epoch key. InvalidateStaleEpoch can proactively
	// clean up old entries. Without invalidation, old epoch entries can still
	// be retrieved with their original keys (no collision with new keys).
	shardState.CompleteTransition(tenantID)

	// New queries use the new key with current epoch
	cache.Set(ctx, key2, []byte("result-v2"))
	result2, found := cache.Get(ctx, key2)
	if !found {
		t.Error("expected cache hit with new shard config")
	}
	if string(result2) != "result-v2" {
		t.Errorf("expected 'result-v2', got %q", string(result2))
	}

	// Proactive invalidation of stale epoch entries
	cache.InvalidateStaleEpoch(tenantID)
	_, found = cache.Get(ctx, key1)
	if found {
		t.Error("after InvalidateStaleEpoch, old cache key should not return results")
	}
}

func TestCacheKeyIncludesShardEpoch(t *testing.T) {
	shardState := NewShardState()
	tenantID := "tenant-2"
	query := "sum(rate(http_requests_total[5m]))"
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	step := 30 * time.Second

	shardState.SetConfig(ShardConfig{TenantID: tenantID, Shards: []ShardID{"a", "b"}, Version: 1})
	shardState.CompleteTransition(tenantID)
	cache := NewQueryRangeCache(shardState)
	key1, _ := cache.BuildCacheKey(tenantID, query, start, end, step)

	shardState.SetConfig(ShardConfig{TenantID: tenantID, Shards: []ShardID{"a", "b"}, Version: 3})
	shardState.CompleteTransition(tenantID)
	key2, _ := cache.BuildCacheKey(tenantID, query, start, end, step)

	if key1.String() == key2.String() {
		t.Error("cache keys must differ when version/epoch changes, even with same shard list")
	}
}

func TestDifferentTenantsHaveDifferentKeys(t *testing.T) {
	shardState := NewShardState()
	cache := NewQueryRangeCache(shardState)
	query := "up"
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	step := time.Minute

	shardState.SetConfig(ShardConfig{TenantID: "tenant-A", Shards: []ShardID{"s1", "s2"}, Version: 1})
	shardState.CompleteTransition("tenant-A")
	shardState.SetConfig(ShardConfig{TenantID: "tenant-B", Shards: []ShardID{"s1", "s2"}, Version: 1})
	shardState.CompleteTransition("tenant-B")

	keyA, _ := cache.BuildCacheKey("tenant-A", query, start, end, step)
	keyB, _ := cache.BuildCacheKey("tenant-B", query, start, end, step)

	if keyA.String() == keyB.String() {
		t.Error("different tenants must have different cache keys")
	}
}

func TestInvalidateStaleEpoch(t *testing.T) {
	shardState := NewShardState()
	cache := NewQueryRangeCache(shardState)
	tenantID := "tenant-3"
	ctx := context.Background()

	shardState.SetConfig(ShardConfig{TenantID: tenantID, Shards: []ShardID{"s1", "s2"}, Version: 1})
	shardState.CompleteTransition(tenantID)
	key1, _ := cache.BuildCacheKey(tenantID, "query1", time.Now(), time.Now().Add(time.Hour), time.Minute)
	cache.Set(ctx, key1, []byte("data1"))

	shardState.SetConfig(ShardConfig{TenantID: tenantID, Shards: []ShardID{"s1", "s2", "s3"}, Version: 2})
	shardState.CompleteTransition(tenantID)

	if cache.Size() != 1 {
		t.Errorf("expected 1 entry before invalidation, got %d", cache.Size())
	}
	cache.InvalidateStaleEpoch(tenantID)
	if cache.Size() != 0 {
		t.Errorf("expected 0 entries after stale epoch invalidation, got %d", cache.Size())
	}
}

func TestCacheBypassDuringTransition(t *testing.T) {
	shardState := NewShardState()
	cache := NewQueryRangeCache(shardState)
	tenantID := "tenant-4"
	ctx := context.Background()

	shardState.SetConfig(ShardConfig{TenantID: tenantID, Shards: []ShardID{"s1"}, Version: 1})
	shardState.CompleteTransition(tenantID)
	key, _ := cache.BuildCacheKey(tenantID, "query", time.Now(), time.Now().Add(time.Hour), time.Minute)
	cache.Set(ctx, key, []byte("cached-data"))

	shardState.SetConfig(ShardConfig{TenantID: tenantID, Shards: []ShardID{"s1", "s2"}, Version: 2})
	// NOT calling CompleteTransition - tenant is transitioning
	_, found := cache.Get(ctx, key)
	if found {
		t.Error("cache must be bypassed during shard transition to prevent partial results")
	}

	shardState.CompleteTransition(tenantID)
	newKey, _ := cache.BuildCacheKey(tenantID, "query", time.Now(), time.Now().Add(time.Hour), time.Minute)
	cache.Set(ctx, newKey, []byte("new-data"))
	result, found := cache.Get(ctx, newKey)
	if !found {
		t.Error("cache should work after transition completes")
	}
	if string(result) != "new-data" {
		t.Errorf("expected 'new-data', got %q", string(result))
	}
}

func TestShardConfigHashIsDeterministic(t *testing.T) {
	cfg1 := ShardConfig{TenantID: "t1", Shards: []ShardID{"shard-b", "shard-a", "shard-c"}, Version: 1}
	cfg2 := ShardConfig{TenantID: "t1", Shards: []ShardID{"shard-c", "shard-b", "shard-a"}, Version: 1}
	if cfg1.Hash() != cfg2.Hash() {
		t.Error("hash must be order-independent: same shards in different order must produce same hash")
	}
	cfg3 := ShardConfig{TenantID: "t1", Shards: []ShardID{"shard-a", "shard-b", "shard-c"}, Version: 2}
	if cfg1.Hash() == cfg3.Hash() {
		t.Error("hash must change when version changes")
	}
}
