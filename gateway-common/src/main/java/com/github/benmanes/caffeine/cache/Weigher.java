package com.github.benmanes.caffeine.cache;

/**
 * Weigher functional interface for byte-weighted W-TinyLFU eviction.
 */
@FunctionalInterface
public interface Weigher<K, V> {
    int weigh(K key, V value);
}
