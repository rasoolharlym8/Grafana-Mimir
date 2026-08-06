package queryrange

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ShardID represents a single shard identifier in the tenant's shard configuration.
type ShardID string

// ShardConfig represents the active shard assignment for a tenant.
// When this configuration changes (e.g., during scale-out/scale-in),
// the epoch changes and cache keys are naturally rotated.
type ShardConfig struct {
	TenantID string
	Shards   []ShardID
	Version  int64 // monotonically increasing version/epoch
}

// Hash returns a deterministic hash of the shard configuration.
// Changes when shards are added, removed, or reordered.
func (sc ShardConfig) Hash() string {
	shardIDs := make([]string, len(sc.Shards))
	for i, s := range sc.Shards {
		shardIDs[i] = string(s)
	}
	sort.Strings(shardIDs)
	input := fmt.Sprintf("%s:%s:%d", sc.TenantID, strings.Join(shardIDs, ","), sc.Version)
	h := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", h[:8]) // first 8 bytes as hex
}

// CacheKey represents a fully-qualified cache key for a query fragment.
// Incorporates tenant, query, time range, step, and shard configuration hash
// to prevent collisions during shard rebalancing.
type CacheKey struct {
	TenantID    string
	Query       string
	Start       time.Time
	End         time.Time
	Step        time.Duration
	ShardEpoch  string // hash of the shard configuration at time of caching
}

// String returns the canonical cache key string.
func (ck CacheKey) String() string {
	return fmt.Sprintf("%s:%s:%d:%d:%d:%s",
		ck.TenantID,
		ck.Query,
		ck.Start.Unix(),
		ck.End.Unix(),
		ck.Step.Milliseconds(),
		ck.ShardEpoch,
	)
}

// ShardState tracks the current shard configuration and epoch for a tenant.
type ShardState struct {
	mu            sync.RWMutex
	configs       map[string]ShardConfig // tenantID -> config
	transitioning map[string]bool        // tenantID -> is transitioning
}

// NewShardState creates a new shard state tracker.
func NewShardState() *ShardState {
	return &ShardState{
		configs:       make(map[string]ShardConfig),
		transitioning: make(map[string]bool),
	}
}

// SetConfig updates the shard configuration for a tenant and marks it as transitioning.
func (ss *ShardState) SetConfig(config ShardConfig) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	old, exists := ss.configs[config.TenantID]
	if !exists || old.Hash() != config.Hash() {
		ss.transitioning[config.TenantID] = true
	}
	ss.configs[config.TenantID] = config
}

// GetConfig returns the current shard configuration for a tenant.
func (ss *ShardState) GetConfig(tenantID string) (ShardConfig, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	cfg, ok := ss.configs[tenantID]
	return cfg, ok
}

// IsTransitioning returns true if the tenant is currently undergoing a shard rebalance.
func (ss *ShardState) IsTransitioning(tenantID string) bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.transitioning[tenantID]
}

// CompleteTransition marks a tenant's shard transition as complete.
func (ss *ShardState) CompleteTransition(tenantID string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.transitioning[tenantID] = false
}
