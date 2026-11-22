package com.jihuayu.aigateway.plugin;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Built-in Semantic Response Cache Plugin (#37) upgraded with Caffeine W-TinyLFU L1 scope management.
 *
 * <p>Computes a 64-dimensional L2-normalized character/word n-gram feature embedding vector over the prompt text
 * and evaluates cosine similarity against project+model scoped cached entries at {@code POST_ADMIT_PRE_DISPATCH}.
 * When similarity &ge; {@code similarityThreshold} (default {@code 0.85}), short-circuits with
 * {@code x-gateway-cache: SEMANTIC_HIT} and settles at {@code 0} micro-USD.
 */
public class SemanticCachePlugin implements GatewayPlugin {

    public static final String ID = "builtin:semantic-cache";
    private static final int VECTOR_DIM = 64;

    private record SemanticEntry(
        float[] vector,
        String promptText,
        String responseBody,
        long expiresAtMillis
    ) {}

    private final Cache<String, List<SemanticEntry>> entriesByScope = Caffeine.newBuilder()
        .maximumSize(4_096)
        .expireAfterAccess(Duration.ofMinutes(20))
        .recordStats()
        .build();

    @Override
    public String id() {
        return ID;
    }

    @Override
    public String name() {
        return "semantic-cache";
    }

    @Override
    public String type() {
        return "BUILTIN";
    }

    @Override
    public int priority() {
        return 25;
    }

    @Override
    public PluginResult onPostAdmitPreDispatch(PluginExecutionContext ctx) {
        if (ctx.stream() && Boolean.FALSE.equals(ctx.pluginConfig().get("cacheStreaming"))) {
            return PluginResult.Continue.INSTANCE;
        }
        String prompt = ctx.extractPromptText();
        if (prompt == null || prompt.isBlank()) {
            return PluginResult.Continue.INSTANCE;
        }
        double threshold = 0.85;
        Object thCfg = ctx.pluginConfig().get("similarityThreshold");
        if (thCfg instanceof Number n && n.doubleValue() > 0.0 && n.doubleValue() <= 1.0) {
            threshold = n.doubleValue();
        }
        String scopeKey = ctx.projectId() + ":" + ctx.endpoint() + ":" + ctx.modelAlias();
        float[] queryVec = embedText(prompt);
        ctx.attributes().put("semantic_cache.vector", queryVec);
        ctx.attributes().put("semantic_cache.scope", scopeKey);

        List<SemanticEntry> list = entriesByScope.getIfPresent(scopeKey);
        if (list != null && !list.isEmpty()) {
            long now = System.currentTimeMillis();
            SemanticEntry best = null;
            double bestSim = -1.0;
            for (SemanticEntry entry : list) {
                if (entry.expiresAtMillis() <= now) continue;
                double sim = cosineSimilarity(queryVec, entry.vector());
                if (sim >= threshold && sim > bestSim) {
                    bestSim = sim;
                    best = entry;
                }
            }
            if (best != null) {
                String simFormatted = String.format(Locale.ROOT, "%.4f", bestSim);
                return PluginResult.shortCircuit(
                    200,
                    best.responseBody(),
                    Map.of(
                        "x-gateway-cache", "SEMANTIC_HIT",
                        "x-gateway-semantic-similarity", simFormatted
                    )
                );
            }
        }
        return PluginResult.Continue.INSTANCE;
    }

    @Override
    public PluginResult onResponseContent(PluginExecutionContext ctx) {
        if (ctx.responseStatus() != 200 || (ctx.stream() && Boolean.FALSE.equals(ctx.pluginConfig().get("cacheStreaming")))) {
            return PluginResult.Continue.INSTANCE;
        }
        String body = ctx.responseBody();
        if (body == null || body.isBlank()) {
            return PluginResult.Continue.INSTANCE;
        }
        String prompt = ctx.extractPromptText();
        if (prompt == null || prompt.isBlank()) {
            return PluginResult.Continue.INSTANCE;
        }
        float[] vec = ctx.attributes().get("semantic_cache.vector") instanceof float[] v ? v : embedText(prompt);
        String scopeKey = ctx.attributes().get("semantic_cache.scope") instanceof String s
            ? s
            : (ctx.projectId() + ":" + ctx.endpoint() + ":" + ctx.modelAlias());
        long ttlSeconds = 300L;
        Object ttlCfg = ctx.pluginConfig().get("ttlSeconds");
        if (ttlCfg instanceof Number n && n.longValue() > 0) {
            ttlSeconds = n.longValue();
        }
        List<SemanticEntry> bucket = entriesByScope.get(scopeKey, _ -> new CopyOnWriteArrayList<>());
        if (bucket.size() >= 128) {
            bucket.remove(0);
        }
        bucket.add(new SemanticEntry(vec, prompt, body, System.currentTimeMillis() + ttlSeconds * 1000L));
        return PluginResult.Continue.INSTANCE;
    }

    public static float[] embedText(String text) {
        float[] vec = new float[VECTOR_DIM];
        String normalized = text.toLowerCase(Locale.ROOT)
            .replaceAll("[^\\p{L}\\p{N}\\s]", " ")
            .replaceAll("\\s+", " ")
            .trim();
        if (normalized.isEmpty()) {
            return vec;
        }
        String[] words = normalized.split(" ");
        for (String w : words) {
            if (w.isEmpty()) continue;
            int h1 = Math.floorMod(w.hashCode(), VECTOR_DIM);
            vec[h1] += 2.0f;
            for (int i = 0; i <= w.length() - 3; i++) {
                String trigram = w.substring(i, i + 3);
                int h2 = Math.floorMod(trigram.hashCode(), VECTOR_DIM);
                vec[h2] += 1.0f;
            }
        }
        for (int i = 0; i < words.length - 1; i++) {
            String bigram = words[i] + "_" + words[i + 1];
            int hb = Math.floorMod(bigram.hashCode(), VECTOR_DIM);
            vec[hb] += 1.5f;
        }
        double normSq = 0.0;
        for (float v : vec) normSq += v * v;
        if (normSq > 0.0) {
            float invNorm = (float) (1.0 / Math.sqrt(normSq));
            for (int i = 0; i < VECTOR_DIM; i++) {
                vec[i] *= invNorm;
            }
        }
        return vec;
    }

    public static double cosineSimilarity(float[] a, float[] b) {
        if (a == null || b == null || a.length != b.length) return 0.0;
        double dot = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
        }
        return dot;
    }

    public void clearCache() {
        entriesByScope.invalidateAll();
    }
}
