package com.github.benmanes.caffeine.cache.stats;

/**
 * High-precision cache telemetry statistics for the Caffeine W-TinyLFU Near-Cache engine.
 */
public record CacheStats(
    long hitCount,
    long missCount,
    long evictionCount
) {
    public long requestCount() {
        return hitCount + missCount;
    }

    public double hitRate() {
        long req = requestCount();
        return req == 0 ? 1.0 : (double) hitCount / req;
    }
}
