package fix

import (
    "crypto/sha256"
    "fmt"
)

// FixCacheKeyCollision generates unique cache keys using SHA256 hashing
// to prevent collisions during tenant shuffle operations.
func GenerateCacheKey(tenantID string, resourceID string) string {
    h := sha256.New()
    h.Write([]byte(fmt.Sprintf("%s:%s", tenantID, resourceID)))
    return fmt.Sprintf("%x", h.Sum(nil))
}
