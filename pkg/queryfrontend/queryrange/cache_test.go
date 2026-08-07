package queryrange

import (
	"sync"
	"testing"
)

func TestCacheKeysDifferByShardConfig(t *testing.T) {
	req := Request{
		Query:    "up",
		Start:    1000,
		End:      2000,
		Step:     15,
		TenantID: "tenant-A",
		Hash:     "abc123",
	}

	cfgOld := &ShardConfig{
		TenantID:    "tenant-A",
		ShardIDs:    []int{1, 2, 3},
		Epoch:       1,
		RoutingHash: "hash-old",
	}

	cfgNew := &ShardConfig{
		TenantID:    "tenant-A",
		ShardIDs:    []int{2, 3, 4},
		Epoch:       2,
		RoutingHash: "hash-new",
	}

	keyOld := GenerateCacheKey(req, cfgOld)
	keyNew := GenerateCacheKey(req, cfgNew)

	if keyOld == keyNew {
		t.Errorf("cache keys should differ when shard config changes: old=%s new=%s", keyOld, keyNew)
	}
}

func TestCacheKeysSameForSameShardConfig(t *testing.T) {
	req := Request{
		Query:    "up",
		Start:    1000,
		End:      2000,
		Step:     15,
		TenantID: "tenant-A",
		Hash:     "abc123",
	}

	cfg := &ShardConfig{
		TenantID:    "tenant-A",
		ShardIDs:    []int{1, 2, 3},
		Epoch:       1,
		RoutingHash: "hash-1",
	}

	key1 := GenerateCacheKey(req, cfg)
	key2 := GenerateCacheKey(req, cfg)

	if key1 != key2 {
		t.Errorf("cache keys should be same for same shard config: key1=%s key2=%s", key1, key2)
	}
}

func TestValidateShardConsistency(t *testing.T) {
	cfg := &ShardConfig{Epoch: 5}

	if !ValidateShardConsistency(5, cfg) {
		t.Error("should be consistent when epochs match")
	}

	if ValidateShardConsistency(4, cfg) {
		t.Error("should be inconsistent when epochs differ")
	}

	if !ValidateShardConsistency(0, nil) {
		t.Error("should be consistent when both are zero/nil")
	}
}

func TestShardStateManagerConcurrent(t *testing.T) {
	mgr := NewShardStateManager()

	cfg := &ShardConfig{
		TenantID:    "tenant-X",
		ShardIDs:    []int{1, 2},
		Epoch:       1,
		RoutingHash: "hash-x",
	}
	mgr.UpdateShardConfig(cfg)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)

		go func() {
			defer wg.Done()
			_ = mgr.GetShardConfig("tenant-X")
		}()

		go func() {
			defer wg.Done()
			mgr.UpdateShardConfig(&ShardConfig{
				TenantID:    "tenant-X",
				Epoch:       2,
				RoutingHash: "hash-y",
			})
		}()
	}
	wg.Wait()

	final := mgr.GetShardConfig("tenant-X")
	if final == nil {
		t.Fatal("expected non-nil config after concurrent updates")
	}
}
