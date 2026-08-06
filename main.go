package main

import (
	"context"
	"fmt"
	"time"

	"github.com/laurentketterle-hub/Grafana-Mimir/pkg/queryfrontend/queryrange"
)

func main() {
	fmt.Println("=== Grafana Mimir Query Frontend - Epoch-Aware Cache Demo ===")

	shardState := queryrange.NewShardState()
	cache := queryrange.NewQueryRangeCache(shardState)

	tenantID := "tenant-1"
	query := `rate(http_requests_total[5m])`
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	step := 15 * time.Second
	ctx := context.Background()

	// Phase 1: Initial shard configuration (3 shards)
	fmt.Println("\n--- Phase 1: Initial shard config (3 shards) ---")
	shardState.SetConfig(queryrange.ShardConfig{
		TenantID: tenantID,
		Shards:   []queryrange.ShardID{"shard-1", "shard-2", "shard-3"},
		Version:  1,
	})
	shardState.CompleteTransition(tenantID)

	key1, _ := cache.BuildCacheKey(tenantID, query, start, end, step)
	fmt.Printf("Cache key (v1): %s\n", key1.String())

	cache.Set(ctx, key1, []byte(`{"metrics":["http_requests_total"],"values":[[1,100],[2,200]]}`))
	fmt.Println("Cached result under v1 shard config")

	result, found := cache.Get(ctx, key1)
	fmt.Printf("Cache hit (v1): %v, data: %s\n", found, string(result))

	// Phase 2: Shard rebalance (scale out to 5 shards)
	fmt.Println("\n--- Phase 2: Shard rebalance (scale out to 5 shards) ---")
	shardState.SetConfig(queryrange.ShardConfig{
		TenantID: tenantID,
		Shards:   []queryrange.ShardID{"shard-1", "shard-2", "shard-3", "shard-4", "shard-5"},
		Version:  2,
	})
	// Tenant is now transitioning - cache bypassed

	key2, _ := cache.BuildCacheKey(tenantID, query, start, end, step)
	fmt.Printf("Cache key (v2): %s\n", key2.String())

	_, found = cache.Get(ctx, key1)
	fmt.Printf("Stale v1 key hit during transition: %v (should be false - bypassed)\n", found)

	// Show that keys are different - prevents collision
	if key1.String() != key2.String() {
		fmt.Println("✓ Cache keys differ between v1 and v2 - no collision possible!")
	}

	// Phase 3: Transition complete
	fmt.Println("\n--- Phase 3: Transition complete ---")
	shardState.CompleteTransition(tenantID)

	cache.Set(ctx, key2, []byte(`{"metrics":["http_requests_total"],"values":[[1,100],[2,200],[3,300]]}`))
	result2, found2 := cache.Get(ctx, key2)
	fmt.Printf("Cache hit (v2): %v, data: %s\n", found2, string(result2))

	fmt.Println("\n=== Summary ===")
	fmt.Println("✓ Epoch-aware cache keys prevent fragment collision during shard rebalancing")
	fmt.Println("✓ Cache bypassed during transitions to guarantee data completeness")
	fmt.Println("✓ Stale entries naturally orphaned via epoch rotation")
}
