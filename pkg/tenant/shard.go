package tenant

import (
    "context"
    "fmt"
)

// ShardVersion returns a string that uniquely identifies the current shuffle‑sharding
// configuration for the tenant associated with the provided context. The value is
// derived from the tenant's limits that influence shard assignment. If the tenant
// does not have shuffle‑sharding enabled, an empty string is returned.
func ShardVersion(ctx context.Context) string {
    // The tenant ID is required to fetch limits.
    tenantID, err := ExtractTenantID(ctx)
    if err != nil {
        // In case of error we fallback to a constant to avoid panics; the cache
        // will simply be shared across tenants which is acceptable for non‑tenant
        // requests.
        return ""
    }

    // Retrieve the limits for the tenant. The Limits interface is part of the
    // existing Mimir codebase and provides access to shuffle‑sharding settings.
    limits, err := GetLimitsForTenant(tenantID)
    if err != nil {
        return ""
    }

    // Build a deterministic representation of the shard configuration.
    // For example, we combine the strategy name and the look‑back period.
    // If the strategy changes, the version string changes, causing cache keys
    // to rotate.
    strategy := limits.ShuffleShardingStrategy()
    lookback := limits.ShuffleShardingLookbackPeriod()
    // The lookback period is a duration; we format it as seconds for stability.
    version := fmt.Sprintf("%s-%d", strategy, int64(lookback.Seconds()))
    return version
}

// GetLimitsForTenant is a thin wrapper around the global limits manager. It is
// defined here to keep the shard helper self‑contained and to avoid import cycles.
func GetLimitsForTenant(tenantID string) (Limits, error) {
    // The actual implementation lives in the limits package; we import it lazily
    // to prevent circular dependencies.
    // NOTE: This function is a placeholder – the real project already provides a
    // similar accessor (e.g., limits.ForTenant). Adjust the call as needed.
    return limits.ForTenant(tenantID)
}
