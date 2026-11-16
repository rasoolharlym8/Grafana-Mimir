package queryrange

import (
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "net/http"
    "strings"

    "github.com/go-kit/log"
    "github.com/prometheus/common/model"
    "github.com/prometheus/prometheus/promql/parser"

    "github.com/grafana/mimir/pkg/tenant"
)

// generateCacheKey creates a deterministic cache key for a given request.
// It now incorporates the tenant's shuffle‑sharding version to avoid collisions
// when the tenant's shard assignment changes.
func generateCacheKey(r *http.Request, logger log.Logger) (string, error) {
    // Basic components: method, URL, query string, and tenant ID.
    var sb strings.Builder
    sb.WriteString(r.Method)
    sb.WriteString("|")
    sb.WriteString(r.URL.Path)
    sb.WriteString("|")
    sb.WriteString(r.URL.RawQuery)
    sb.WriteString("|")

    // Tenant ID is mandatory for multi‑tenant caching.
    tenantID, err := tenant.ExtractTenantID(r.Context())
    if err != nil {
        return "", fmt.Errorf("cannot extract tenant ID: %w", err)
    }
    sb.WriteString(string(tenantID))
    sb.WriteString("|")

    // NEW: Append the tenant's current shard version.
    // The version is a deterministic string that changes whenever the
    // shuffle‑sharding assignment for the tenant changes (e.g., during a rebalance).
    shardVersion := tenant.ShardVersion(r.Context())
    sb.WriteString(shardVersion)
    sb.WriteString("|")

    // Include the PromQL query text (if present) to ensure different queries
    // do not collide.
    if q := r.URL.Query().Get("query"); q != "" {
        // Normalise the query to avoid cosmetic differences.
        normalized, err := parser.ParseExpr(q)
        if err == nil {
            sb.WriteString(normalized.String())
        } else {
            sb.WriteString(q)
        }
        sb.WriteString("|")
    }

    // Hash the accumulated string to produce a fixed‑length key.
    hash := sha256.Sum256([]byte(sb.String()))
    key := hex.EncodeToString(hash[:])
    return key, nil
}

// The rest of the file remains unchanged.
