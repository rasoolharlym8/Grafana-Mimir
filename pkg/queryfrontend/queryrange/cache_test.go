package queryrange

import (
    "context"
    "net/http"
    "net/url"
    "testing"

    "github.com/go-kit/log"
    "github.com/grafana/mimir/pkg/tenant"
)

// mockTenantContext returns a request with a tenant ID and a configurable shard version.
func mockTenantContext(t *testing.T, tenantID string, version string) *http.Request {
    t.Helper()
    // Create a simple GET request.
    req, err := http.NewRequest("GET", "/query_range", nil)
    if err != nil {
        t.Fatalf("failed to create request: %v", err)
    }
    // Attach tenant ID via the standard header used by Mimir.
    req.Header.Set("X-Scope-OrgID", tenantID)

    // Inject a fake shard version into the context using a custom value.
    ctx := tenant.InjectShardVersion(context.Background(), version)
    req = req.WithContext(ctx)
    return req
}

func TestGenerateCacheKey_IncludesShardVersion(t *testing.T) {
    logger := log.NewNopLogger()

    // Two requests with identical query but different shard versions.
    r1 := mockTenantContext(t, "tenant-a", "strategyA-60")
    r2 := mockTenantContext(t, "tenant-a", "strategyB-120")

    // Add the same query parameter to both.
    q := url.Values{}
    q.Set("query", "up")
    r1.URL.RawQuery = q.Encode()
    r2.URL.RawQuery = q.Encode()

    key1, err := generateCacheKey(r1, logger)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    key2, err := generateCacheKey(r2, logger)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if key1 == key2 {
        t.Fatalf("cache keys should differ when shard version changes: %s vs %s", key1, key2)
    }
}
