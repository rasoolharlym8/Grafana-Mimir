package queryrange

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CacheEntry represents a cached query fragment result.
type CacheEntry struct {
	Key       CacheKey
	Data      []byte
	CreatedAt time.Time
}

// QueryRangeCache provides caching for query range fragments.
// It is epoch-aware: cache keys incorporate the shard configuration hash,
// preventing stale fragments from being merged during shard rebalancing.
type QueryRangeCache struct {
	mu         sync.RWMutex
	entries    map[string]CacheEntry
	shardState *ShardState
}

// NewQueryRangeCache creates a new epoch-aware query range cache.
func NewQueryRangeCache(shardState *ShardState) *QueryRangeCache {
	return &QueryRangeCache{
		entries:    make(map[string]CacheEntry),
		shardState: shardState,
	}
}

// BuildCacheKey generates an epoch-aware cache key for a query fragment.
// The key includes the tenant's current shard configuration hash. If the
// shard configuration changes (e.g., during rebalancing), the hash changes
// and old cache entries are naturally orphaned — preventing collision.
func (c *QueryRangeCache) BuildCacheKey(tenantID, query string, start, end time.Time, step time.Duration) (CacheKey, error) {
	config, ok := c.shardState.GetConfig(tenantID)
	if !ok {
		return CacheKey{}, fmt.Errorf("no shard configuration found for tenant %q", tenantID)
	}

	return CacheKey{
		TenantID:   tenantID,
		Query:      query,
		Start:      start,
		End:        end,
		Step:       step,
		ShardEpoch: config.Hash(),
	}, nil
}

// Get retrieves a cached entry. During a shard transition, it always returns
// a cache miss to guarantee data completeness — stale fragments from old
// shard assignments are never returned.
func (c *QueryRangeCache) Get(ctx context.Context, key CacheKey) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// During shard transitions, bypass cache to ensure complete results.
	// This prevents returning partial results with HTTP 200 OK due to
	// mismatched cached fragments from different shard configurations.
	if c.shardState.IsTransitioning(key.TenantID) {
		return nil, false
	}

	entry, exists := c.entries[key.String()]
	if !exists {
		return nil, false
	}
	return entry.Data, true
}

// Set stores a query result in the cache.
func (c *QueryRangeCache) Set(ctx context.Context, key CacheKey, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key.String()] = CacheEntry{
		Key:       key,
		Data:      data,
		CreatedAt: time.Now(),
	}
}

// InvalidateTenant removes all cached entries for a tenant.
// Useful during shard transitions to proactively clear stale fragments.
func (c *QueryRangeCache) InvalidateTenant(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, entry := range c.entries {
		if entry.Key.TenantID == tenantID {
			delete(c.entries, k)
		}
	}
}

// InvalidateStaleEpoch removes all cache entries for a tenant that were
// created under a different shard epoch than the current one.
func (c *QueryRangeCache) InvalidateStaleEpoch(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	config, ok := c.shardState.GetConfig(tenantID)
	if !ok {
		return
	}
	currentEpoch := config.Hash()

	for k, entry := range c.entries {
		if entry.Key.TenantID == tenantID && entry.Key.ShardEpoch != currentEpoch {
			delete(c.entries, k)
		}
	}
}

// Size returns the number of cached entries.
func (c *QueryRangeCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
