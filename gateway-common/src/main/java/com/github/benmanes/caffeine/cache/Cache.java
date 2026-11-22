package com.github.benmanes.caffeine.cache;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.util.function.Function;

/**
 * High-concurrency W-TinyLFU in-memory cache interface compatible with Caffeine API.
 */
public interface Cache<K, V> {
    V getIfPresent(K key);
    V get(K key, Function<? super K, ? extends V> mappingFunction);
    void put(K key, V value);
    void invalidate(K key);
    void invalidateAll();
    long estimatedSize();
    CacheStats stats();
}
