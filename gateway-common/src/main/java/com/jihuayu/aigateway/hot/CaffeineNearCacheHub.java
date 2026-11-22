package com.jihuayu.aigateway.hot;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.jihuayu.aigateway.hot.HotContract.KeyContext;
import com.jihuayu.aigateway.hot.HotContract.Snapshot;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Two-Tiered Distributed Near-Cache Hub powered by Caffeine (W-TinyLFU) sitting in front of Redis L2.
 *
 * <p>Provides 4 specialized L1 cache domains + SingleFlight stampede protection + Stale-While-Revalidate
 * fallback during Redis master-replica failover or brief network partitions:
 * <ol>
 *   <li>{@code authKeyCache}: API Key authentication context ({@code sha256(token) -> KeyContext}) with
 *       sub-microsecond lookup and Redis Pub/Sub invalidation.</li>
 *   <li>{@code snapshotCache}: Immutable control-plane {@link Snapshot} indexed by {@code revision}, paired
 *       with an atomic {@code activeRevision} pointer for 0-RTT Stage 1a pre-auth snapshot resolution.</li>
 *   <li>{@code responseCache}: Weight-bounded (256 MB) W-TinyLFU L1 LLM response cache sitting in front of
 *       Redis L2 ({@code cache:exact:{hash}}) with SingleFlight coalescing of identical concurrent requests.</li>
 *   <li>{@code secretDecryptCache}: Pre-decrypted upstream provider secrets keyed by {@code sha256(ciphertext)}
 *       to eliminate per-request AES-256-GCM decryption overhead.</li>
 * </ol>
 */
public final class CaffeineNearCacheHub {

    private final Cache<String, KeyContext> authKeyCache;
    private final Map<String, KeyContext> staleAuthFallback = new ConcurrentHashMap<>();

    private final Cache<String, Snapshot> snapshotCache;
    private final AtomicReference<String> activeRevision = new AtomicReference<>("");
    private final AtomicReference<Snapshot> lastKnownGoodSnapshot = new AtomicReference<>();

    private final Cache<String, String> responseCache;
    private final Map<String, CompletableFuture<String>> singleFlightInflight = new ConcurrentHashMap<>();

    private final Cache<String, String> secretDecryptCache;

    private final AtomicLong singleFlightCoalescedCount = new AtomicLong(0);
    private final AtomicLong staleFallbackHits = new AtomicLong(0);
    private final AtomicLong pubSubInvalidations = new AtomicLong(0);

    public CaffeineNearCacheHub() {
        this.authKeyCache = Caffeine.newBuilder()
            .maximumSize(50_000)
            .expireAfterWrite(Duration.ofSeconds(30))
            .recordStats()
            .build();

        this.snapshotCache = Caffeine.newBuilder()
            .maximumSize(32)
            .expireAfterAccess(Duration.ofMinutes(30))
            .recordStats()
            .build();

        this.responseCache = Caffeine.newBuilder()
            .maximumWeight(128L * 1024L * 1024L) // 128 MB L1 W-TinyLFU window
            .weigher((String k, String v) -> k.length() + (v != null ? v.getBytes(StandardCharsets.UTF_8).length : 0))
            .expireAfterWrite(Duration.ofMinutes(10))
            .recordStats()
            .build();

        this.secretDecryptCache = Caffeine.newBuilder()
            .maximumSize(4_096)
            .expireAfterAccess(Duration.ofMinutes(30))
            .recordStats()
            .build();
    }

    public KeyContext getAuthKey(String keyHash) {
        String rev = activeRevision.get();
        if (rev == null || rev.isEmpty()) {
            return null;
        }
        KeyContext ctx = authKeyCache.getIfPresent(rev + ":" + keyHash);
        if (ctx != null) {
            if (ctx.runtimeExpiresAt() != null && ctx.runtimeExpiresAt() <= System.currentTimeMillis()) {
                authKeyCache.invalidate(rev + ":" + keyHash);
                staleAuthFallback.remove(keyHash);
                return null;
            }
            return ctx;
        }
        return null;
    }

    public void putAuthKey(KeyContext ctx) {
        if (ctx == null || ctx.runtimeRevision() == null || ctx.runtimeKeyHash() == null) {
            return;
        }
        activeRevision.compareAndSet("", ctx.runtimeRevision());
        authKeyCache.put(ctx.runtimeRevision() + ":" + ctx.runtimeKeyHash(), ctx);
        staleAuthFallback.put(ctx.runtimeKeyHash(), ctx);
    }

    /**
     * Stale-While-Revalidate fallback when Redis L2 is temporarily unreachable during failover.
     */
    public KeyContext getStaleAuthKeyOnRedisFailure(String keyHash) {
        KeyContext stale = staleAuthFallback.get(keyHash);
        if (stale != null) {
            if (stale.runtimeExpiresAt() != null && stale.runtimeExpiresAt() <= System.currentTimeMillis()) {
                staleAuthFallback.remove(keyHash);
                return null;
            }
            staleFallbackHits.incrementAndGet();
            return stale;
        }
        return null;
    }

    public Snapshot getSnapshot(String revision) {
        if (revision == null || revision.isBlank()) {
            return lastKnownGoodSnapshot.get();
        }
        return snapshotCache.getIfPresent(revision);
    }

    public Snapshot getActiveSnapshotFast() {
        String rev = activeRevision.get();
        if (rev != null && !rev.isBlank()) {
            Snapshot snap = snapshotCache.getIfPresent(rev);
            if (snap != null) {
                return snap;
            }
        }
        return lastKnownGoodSnapshot.get();
    }

    public void putSnapshot(String revision, Snapshot snapshot) {
        if (revision == null || revision.isBlank() || snapshot == null) {
            return;
        }
        snapshotCache.put(revision, snapshot);
        lastKnownGoodSnapshot.set(snapshot);
        String prev = activeRevision.getAndSet(revision);
        if (prev != null && !prev.isEmpty() && !prev.equals(revision)) {
            // Revision rotated: clear stale auth entries from previous revision
            authKeyCache.invalidateAll();
            staleAuthFallback.clear();
            pubSubInvalidations.incrementAndGet();
        }
    }

    public String getActiveRevision() {
        return activeRevision.get();
    }

    public String getResponseL1(String cacheKey) {
        return responseCache.getIfPresent(cacheKey);
    }

    public void putResponseL1(String cacheKey, String body) {
        if (cacheKey != null && body != null && !body.isBlank()) {
            responseCache.put(cacheKey, body);
        }
    }

    /**
     * SingleFlight request coalescing: if multiple virtual threads simultaneously miss L1/L2 cache
     * for the exact same key, only the first caller executes {@code loader} while concurrent followers
     * share the same {@link CompletableFuture}.
     */
    public String executeSingleFlight(String cacheKey, Supplier<String> loader) {
        String existing = responseCache.getIfPresent(cacheKey);
        if (existing != null) {
            return existing;
        }
        CompletableFuture<String> myFuture = new CompletableFuture<>();
        CompletableFuture<String> prior = singleFlightInflight.putIfAbsent(cacheKey, myFuture);
        if (prior != null) {
            singleFlightCoalescedCount.incrementAndGet();
            try {
                return prior.join();
            } catch (Exception e) {
                return loader.get();
            }
        }
        try {
            String loaded = loader.get();
            if (loaded != null && !loaded.isBlank()) {
                responseCache.put(cacheKey, loaded);
            }
            myFuture.complete(loaded);
            return loaded;
        } catch (Throwable t) {
            myFuture.completeExceptionally(t);
            throw t;
        } finally {
            singleFlightInflight.remove(cacheKey, myFuture);
        }
    }

    public String getOrDecryptSecret(String cipherText, Supplier<String> decryptor) {
        if (cipherText == null || cipherText.isBlank()) {
            return "";
        }
        return secretDecryptCache.get(cipherText, _ -> decryptor.get());
    }

    public void invalidateKeyHash(String keyHash) {
        if (keyHash == null) return;
        staleAuthFallback.remove(keyHash);
        String rev = activeRevision.get();
        if (rev != null && !rev.isBlank()) {
            authKeyCache.invalidate(rev + ":" + keyHash);
        }
        pubSubInvalidations.incrementAndGet();
    }

    public void invalidateAll() {
        authKeyCache.invalidateAll();
        staleAuthFallback.clear();
        snapshotCache.invalidateAll();
        responseCache.invalidateAll();
        secretDecryptCache.invalidateAll();
        activeRevision.set("");
        lastKnownGoodSnapshot.set(null);
        pubSubInvalidations.incrementAndGet();
    }

    public CacheStats authStats() {
        return authKeyCache.stats();
    }

    public CacheStats responseStats() {
        return responseCache.stats();
    }

    public long singleFlightCoalescedCount() {
        return singleFlightCoalescedCount.get();
    }

    public long staleFallbackHits() {
        return staleFallbackHits.get();
    }

    public long pubSubInvalidations() {
        return pubSubInvalidations.get();
    }
}
