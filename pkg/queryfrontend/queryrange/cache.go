package queryrange

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

// Request represents a PromQL query request.
type Request struct {
	Query     string
	Start     int64
	End       int64
	Step      int64
	TenantID  string
	Hash      string
}

// ShardConfig represents the current shard assignment for a tenant.
type ShardConfig struct {
	TenantID   string
	ShardIDs   []int
	Epoch      int64
	RoutingHash string
}

// ShardStateManager manages shard configurations atomically.
// Uses RWMutex to ensure thread-safe reads during concurrent query loads.
type ShardStateManager struct {
	mu      sync.RWMutex
	configs map[string]*ShardConfig // tenantID -> config
}

func NewShardStateManager() *ShardStateManager {
	return &ShardStateManager{
		configs: make(map[string]*ShardConfig),
	}
}

// GetShardConfig returns the current shard config for a tenant.
// Uses RLock for concurrent read access.
func (m *ShardStateManager) GetShardConfig(tenantID string) *ShardConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.configs[tenantID]
}

// UpdateShardConfig atomically updates the shard config for a tenant.
// Uses Lock to ensure atomic write during rebalancing.
func (m *ShardStateManager) UpdateShardConfig(cfg *ShardConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.configs[cfg.TenantID] = cfg
}

// GenerateCacheKey creates a cache key that incorporates the tenant's
// current shard configuration, ensuring that cache keys naturally
// rotate when a rebalance occurs.
//
// The key format is: tenantID:epoch:routingHash:requestHash
// This ensures:
// 1. Different shard configs produce different keys (no collision)
// 2. Same shard config + same request produces same key (cache hit)
// 3. Rebalance invalidates old cache entries automatically
func GenerateCacheKey(req Request, shardCfg *ShardConfig) string {
	if shardCfg == nil {
		// No shard config: use a default epoch
		return fmt.Sprintf("%s:0:default:%s", req.TenantID, hashRequest(req))
	}
	return fmt.Sprintf("%s:%d:%s:%s",
		req.TenantID,
		shardCfg.Epoch,
		shardCfg.RoutingHash,
		hashRequest(req),
	)
}

// hashRequest creates a deterministic hash of the request parameters.
func hashRequest(req Request) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s:%d:%d:%d:%s", req.Query, req.Start, req.End, req.Step, req.Hash)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ValidateShardConsistency checks if the shard config has changed
// between when the query started and when the cache write is attempted.
// If the config changed, the cache write should be aborted.
func ValidateShardConsistency(startEpoch int64, currentCfg *ShardConfig) bool {
	if currentCfg == nil {
		return startEpoch == 0
	}
	return startEpoch == currentCfg.Epoch
}
