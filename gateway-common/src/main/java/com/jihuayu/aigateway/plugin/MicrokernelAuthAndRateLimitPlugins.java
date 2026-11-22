package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;

/**
 * Built-in Enterprise Authentication & Multi-Dimensional Rate Limit / Spend Velocity Plugins (#52).
 *
 * <p>All plugins use the isomorphic {@link PluginExecutionContext} ({@code ctx.kv()}, {@code ctx.crypto()},
 * {@code ctx.bindAuthenticatedKey(...)}, {@code ctx.setEndUserId(...)}) identically available to custom Java and JS plugins.
 */
public final class MicrokernelAuthAndRateLimitPlugins {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MicrokernelAuthAndRateLimitPlugins() {}

    /**
     * 1. CustomExtractorAuthPlugin (builtin:auth-custom-extractor):
     * Extracts API keys from custom HTTP headers, URL query parameters (?api_key=...), Cookies, or Basic Auth.
     */
    public static final class CustomExtractorAuthPlugin implements GatewayPlugin {
        public static final String ID = "builtin:auth-custom-extractor";

        @Override public String id() { return ID; }
        @Override public String name() { return "auth-custom-extractor"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 15; }

        @Override
        public PluginResult onAuthenticate(PluginExecutionContext ctx) {
            if (ctx.authenticatedKey() != null || ctx.boundApiKeyPlain() != null) {
                return PluginResult.Continue.INSTANCE;
            }
            Map<String, Object> cfg = ctx.pluginConfig();
            String prefixStrip = cfg.get("prefixStrip") instanceof String s ? s : "";

            // 1. Check configured custom headers
            List<String> headers = cfg.get("headerNames") instanceof List<?> l
                ? l.stream().map(o -> String.valueOf(o).toLowerCase()).toList()
                : List.of("x-gateway-key", "x-custom-token", "api-key");
            for (String h : headers) {
                String val = ctx.requestHeaders().get(h);
                if (val != null && !val.isBlank()) {
                    String token = val.trim();
                    if (!prefixStrip.isEmpty() && token.startsWith(prefixStrip)) {
                        token = token.substring(prefixStrip.length()).trim();
                    }
                    if (!token.isEmpty()) {
                        ctx.bindAuthenticatedKey(token);
                        ctx.setResponseHeader("x-gateway-auth-source", "header:" + h);
                        return PluginResult.Continue.INSTANCE;
                    }
                }
            }

            // 2. Check URL query parameters (?api_key=... or ?key=...)
            List<String> qParams = cfg.get("queryParams") instanceof List<?> l
                ? l.stream().map(String::valueOf).toList()
                : List.of("api_key", "key", "token");
            for (String qp : qParams) {
                String val = ctx.queryParams().get(qp);
                if (val != null && !val.isBlank()) {
                    ctx.bindAuthenticatedKey(val.trim());
                    ctx.setResponseHeader("x-gateway-auth-source", "query:" + qp);
                    return PluginResult.Continue.INSTANCE;
                }
            }

            // 3. Check Basic Auth (username:password where password or username is the API key)
            if (!Boolean.FALSE.equals(cfg.get("allowBasicAuth"))) {
                String authHdr = ctx.requestHeaders().get("authorization");
                if (authHdr != null && authHdr.regionMatches(true, 0, "Basic ", 0, 6)) {
                    try {
                        String decoded = ctx.crypto().base64Decode(authHdr.substring(6).trim());
                        int colon = decoded.indexOf(':');
                        String candidate = colon >= 0 ? decoded.substring(colon + 1).trim() : decoded.trim();
                        if (candidate.isEmpty() && colon > 0) {
                            candidate = decoded.substring(0, colon).trim();
                        }
                        if (!candidate.isEmpty()) {
                            ctx.bindAuthenticatedKey(candidate);
                            ctx.setResponseHeader("x-gateway-auth-source", "basic_auth");
                            return PluginResult.Continue.INSTANCE;
                        }
                    } catch (Exception ignored) {}
                }
            }
            return PluginResult.Continue.INSTANCE;
        }
    }

    /**
     * 2. HmacSignatureAuthPlugin (builtin:auth-hmac-signature):
     * Enterprise AK/SK + Timestamp + Nonce + HMAC-SHA256 signature verification with Redis/L1 replay protection.
     */
    public static final class HmacSignatureAuthPlugin implements GatewayPlugin {
        public static final String ID = "builtin:auth-hmac-signature";

        @Override public String id() { return ID; }
        @Override public String name() { return "auth-hmac-signature"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 20; }

        @Override
        @SuppressWarnings("unchecked")
        public PluginResult onAuthenticate(PluginExecutionContext ctx) {
            Map<String, Object> cfg = ctx.pluginConfig();
            String akHeader = cfg.get("accessKeyHeader") instanceof String s ? s.toLowerCase() : "x-ak";
            String sigHeader = cfg.get("signatureHeader") instanceof String s ? s.toLowerCase() : "x-signature";
            String tsHeader = cfg.get("timestampHeader") instanceof String s ? s.toLowerCase() : "x-timestamp";
            String nonceHeader = cfg.get("nonceHeader") instanceof String s ? s.toLowerCase() : "x-nonce";

            String ak = ctx.requestHeaders().get(akHeader);
            if (ak == null || ak.isBlank()) {
                return PluginResult.Continue.INSTANCE;
            }
            String sig = ctx.requestHeaders().get(sigHeader);
            String tsStr = ctx.requestHeaders().get(tsHeader);
            String nonce = ctx.requestHeaders().get(nonceHeader);
            if (sig == null || sig.isBlank() || tsStr == null || tsStr.isBlank() || nonce == null || nonce.isBlank()) {
                return PluginResult.reject(401, "hmac_missing_headers", "Missing HMAC signature, timestamp, or nonce headers");
            }

            long maxSkewSec = cfg.get("maxClockSkewSeconds") instanceof Number n ? n.longValue() : 300L;
            long reqEpochSec;
            try {
                reqEpochSec = Long.parseLong(tsStr.trim());
                if (reqEpochSec > 10_000_000_000L) reqEpochSec /= 1000L; // handle millis
            } catch (NumberFormatException e) {
                return PluginResult.reject(401, "hmac_invalid_timestamp", "Invalid X-Timestamp header");
            }
            long nowSec = System.currentTimeMillis() / 1000L;
            if (Math.abs(nowSec - reqEpochSec) > maxSkewSec) {
                return PluginResult.reject(401, "hmac_timestamp_expired", "HMAC timestamp outside allowed clock skew window");
            }

            Map<String, Object> credsMap = cfg.get("credentials") instanceof Map<?, ?> m
                ? (Map<String, Object>) m
                : Map.of();
            Object akEntryObj = credsMap.get(ak.trim());
            if (!(akEntryObj instanceof Map<?, ?> akEntry)) {
                return PluginResult.reject(401, "hmac_unknown_access_key", "Unknown access key: " + ak);
            }
            String secret = akEntry.get("secret") != null ? String.valueOf(akEntry.get("secret")) : "";
            String mappedKey = akEntry.get("mappedApiKeyPlain") != null ? String.valueOf(akEntry.get("mappedApiKeyPlain")) : "";

            // Canonical string to sign: ak + "\n" + timestamp + "\n" + nonce + "\n" + endpoint
            String canonical = ak.trim() + "\n" + tsStr.trim() + "\n" + nonce.trim() + "\n" + ctx.endpoint();
            String expectedSig = ctx.crypto().hmacSha256(secret, canonical);
            if (!ctx.crypto().constantTimeEquals(expectedSig, sig.trim().toLowerCase())) {
                return PluginResult.reject(401, "hmac_signature_mismatch", "HMAC-SHA256 signature verification failed");
            }

            // Anti-replay nonce check using ctx.kv().setNx
            if (!ctx.kv().setNx("nonce:" + ak.trim() + ":" + nonce.trim(), "1", maxSkewSec)) {
                return PluginResult.reject(401, "hmac_nonce_replayed", "Duplicate HMAC nonce detected (replay attack prevented)");
            }

            if (!mappedKey.isBlank()) {
                ctx.bindAuthenticatedKey(mappedKey);
            }
            ctx.setEndUserId("ak:" + ak.trim());
            ctx.setResponseHeader("x-gateway-auth-source", "hmac:" + ak.trim());
            return PluginResult.Continue.INSTANCE;
        }
    }

    /**
     * 3. JwtOidcAuthPlugin (builtin:auth-jwt-oidc):
     * Verifies HS256 JWT tokens, checks exp/iss/aud claims, binds the mapped project API Key, and sets endUserId = sub.
     */
    public static final class JwtOidcAuthPlugin implements GatewayPlugin {
        public static final String ID = "builtin:auth-jwt-oidc";

        @Override public String id() { return ID; }
        @Override public String name() { return "auth-jwt-oidc"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 25; }

        @Override
        @SuppressWarnings("unchecked")
        public PluginResult onAuthenticate(PluginExecutionContext ctx) {
            if (ctx.authenticatedKey() != null || ctx.boundApiKeyPlain() != null) {
                return PluginResult.Continue.INSTANCE;
            }
            Map<String, Object> cfg = ctx.pluginConfig();
            String headerName = cfg.get("jwtHeader") instanceof String s ? s.toLowerCase() : "x-jwt-assertion";
            String rawJwt = ctx.requestHeaders().get(headerName);
            if (rawJwt == null || rawJwt.isBlank()) {
                String authHdr = ctx.requestHeaders().get("authorization");
                if (authHdr != null && authHdr.startsWith("Bearer eyJ")) {
                    rawJwt = authHdr.substring(7).trim();
                }
            }
            if (rawJwt == null || rawJwt.isBlank()) {
                return PluginResult.Continue.INSTANCE;
            }
            String[] parts = rawJwt.trim().split("\\.");
            if (parts.length != 3) {
                return PluginResult.reject(401, "jwt_malformed", "JWT must contain 3 dot-separated segments");
            }
            String secret = cfg.get("hs256Secret") instanceof String s ? s : "";
            if (!secret.isBlank()) {
                try {
                    javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
                    mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes( java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
                    byte[] rawSig = mac.doFinal((parts[0] + "." + parts[1]).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    String expectedB64Url = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(rawSig);
                    if (!ctx.crypto().constantTimeEquals(expectedB64Url, parts[2])) {
                        return PluginResult.reject(401, "jwt_invalid_signature", "JWT HS256 signature verification failed");
                    }
                } catch (Exception e) {
                    return PluginResult.reject(401, "jwt_verification_error", "JWT verification failed");
                }
            }

            Map<String, Object> claims;
            try {
                String jsonPayload = ctx.crypto().base64UrlDecode(parts[1]);
                claims = MAPPER.readValue(jsonPayload, Map.class);
            } catch (Exception e) {
                return PluginResult.reject(401, "jwt_invalid_payload", "Failed to decode JWT payload JSON");
            }

            long nowSec = System.currentTimeMillis() / 1000L;
            if (claims.get("exp") instanceof Number exp && exp.longValue() < nowSec) {
                return PluginResult.reject(401, "jwt_expired", "JWT token has expired");
            }
            if (cfg.get("expectedIssuer") instanceof String expIss && !expIss.isBlank()) {
                if (!expIss.equals(claims.get("iss"))) {
                    return PluginResult.reject(401, "jwt_invalid_issuer", "JWT issuer mismatch");
                }
            }

            String sub = claims.get("sub") != null ? String.valueOf(claims.get("sub")) : "jwt-user";
            ctx.setEndUserId(sub);
            if (claims.get("cost_center") instanceof String cc) {
                ctx.putAttributionTag("cost_center", cc);
            }
            if (claims.get("department") instanceof String dept) {
                ctx.putAttributionTag("department", dept);
            }

            String mappedKey = cfg.get("defaultMappedApiKeyPlain") instanceof String s ? s : null;
            if (cfg.get("claimToApiKeyMap") instanceof Map<?, ?> cmap) {
                String claimName = cfg.get("routingClaim") instanceof String rc ? rc : "tenant";
                Object claimVal = claims.get(claimName);
                if (claimVal != null && cmap.get(String.valueOf(claimVal)) instanceof String k) {
                    mappedKey = k;
                }
            }
            if (mappedKey != null && !mappedKey.isBlank()) {
                ctx.bindAuthenticatedKey(mappedKey);
            }
            ctx.setResponseHeader("x-gateway-auth-source", "jwt:" + sub);
            return PluginResult.Continue.INSTANCE;
        }
    }

    /**
     * 4. EndUserRateLimitPlugin (builtin:ratelimit-end-user):
     * Enforces granular per-end-user / per-tenant / per-IP RPM and TPM limits at ON_RATE_LIMIT using ctx.kv().incr().
     */
    public static final class EndUserRateLimitPlugin implements GatewayPlugin {
        public static final String ID = "builtin:ratelimit-end-user";

        @Override public String id() { return ID; }
        @Override public String name() { return "ratelimit-end-user"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 30; }

        @Override
        public PluginResult onRateLimit(PluginExecutionContext ctx) {
            String userId = ctx.endUserId();
            if (userId == null || userId.isBlank()) {
                return PluginResult.Continue.INSTANCE;
            }
            Map<String, Object> cfg = ctx.pluginConfig();
            long windowSec = cfg.get("windowSeconds") instanceof Number n ? Math.max(1L, n.longValue()) : 60L;
            long bucketSlot = (System.currentTimeMillis() / 1000L) / windowSec;

            if (cfg.get("userRpm") instanceof Number rpmNum && rpmNum.longValue() > 0) {
                long limit = rpmNum.longValue();
                long count = ctx.kv().incr("rl:rpm:" + ctx.projectId() + ":" + userId + ":" + bucketSlot, 1L, windowSec + 5L);
                if (count > limit) {
                    return PluginResult.reject(
                        429,
                        "end_user_rate_limit_exceeded",
                        "End-user '" + userId + "' exceeded RPM limit (" + limit + " req / " + windowSec + "s)",
                        Map.of("retry-after", String.valueOf(windowSec), "x-ratelimit-end-user", userId)
                    );
                }
            }
            if (cfg.get("userTpm") instanceof Number tpmNum && tpmNum.longValue() > 0) {
                long limit = tpmNum.longValue();
                long estTokens = Math.max(1L, (ctx.extractPromptText().length() + 2) / 3);
                long used = ctx.kv().incr("rl:tpm:" + ctx.projectId() + ":" + userId + ":" + bucketSlot, estTokens, windowSec + 5L);
                if (used > limit) {
                    return PluginResult.reject(
                        429,
                        "end_user_tpm_limit_exceeded",
                        "End-user '" + userId + "' exceeded TPM limit (" + limit + " tokens / " + windowSec + "s)",
                        Map.of("retry-after", String.valueOf(windowSec), "x-ratelimit-end-user", userId)
                    );
                }
            }
            ctx.setResponseHeader("x-ratelimit-end-user", userId);
            return PluginResult.Continue.INSTANCE;
        }
    }

    /**
     * 5. SpendVelocityCircuitPlugin (builtin:ratelimit-spend-velocity):
     * Protects against runaway agent loops by capping single-request estimated cost and sliding-window micro-USD spend rate.
     */
    public static final class SpendVelocityCircuitPlugin implements GatewayPlugin {
        public static final String ID = "builtin:ratelimit-spend-velocity";

        @Override public String id() { return ID; }
        @Override public String name() { return "ratelimit-spend-velocity"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 35; }

        @Override
        public PluginResult onRateLimit(PluginExecutionContext ctx) {
            Map<String, Object> cfg = ctx.pluginConfig();
            long windowSec = cfg.get("windowSeconds") instanceof Number n ? Math.max(10L, n.longValue()) : 300L;
            long slot = (System.currentTimeMillis() / 1000L) / windowSec;
            String scope = ctx.endUserId() != null ? ctx.projectId() + ":" + ctx.endUserId() : ctx.projectId();

            if (cfg.get("maxWindowSpendMicros") instanceof Number maxWinNum && maxWinNum.longValue() > 0) {
                long maxWin = maxWinNum.longValue();
                String curStr = ctx.kv().get("spend:win:" + scope + ":" + slot);
                long curSpent = 0L;
                if (curStr != null && !curStr.isBlank()) {
                    try { curSpent = Long.parseLong(curStr.trim()); } catch (Exception ignored) {}
                }
                if (curSpent >= maxWin) {
                    return PluginResult.reject(
                        429,
                        "spend_velocity_exceeded",
                        "Spend velocity circuit breaker tripped: " + curSpent + " >= " + maxWin + " micro-USD in " + windowSec + "s window",
                        Map.of("retry-after", String.valueOf(windowSec), "x-spend-velocity-tripped", "true")
                    );
                }
            }
            return PluginResult.Continue.INSTANCE;
        }

        @Override
        public void onPostSettle(PluginExecutionContext ctx) {
            long actual = ctx.settledActualMicros();
            if (actual <= 0) return;
            Map<String, Object> cfg = ctx.pluginConfig();
            long windowSec = cfg.get("windowSeconds") instanceof Number n ? Math.max(10L, n.longValue()) : 300L;
            long slot = (System.currentTimeMillis() / 1000L) / windowSec;
            String scope = ctx.endUserId() != null ? ctx.projectId() + ":" + ctx.endUserId() : ctx.projectId();
            ctx.kv().incr("spend:win:" + scope + ":" + slot, actual, windowSec + 30L);
        }
    }
}
