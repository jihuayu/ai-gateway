package com.jihuayu.aigateway.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.config.GatewayConfig;
import com.jihuayu.aigateway.control.ControlAuthService.SessionInfo;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotContract;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.security.SecurityUtils;
import com.jihuayu.aigateway.security.SecurityUtils.ProviderUrlException;
import jakarta.servlet.http.HttpServletRequest;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.net.ssl.SSLException;
import javax.sql.DataSource;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProvidersController {
    private static final Set<String> SUPPORTED_PROTOCOLS = Set.of("openai", "anthropic", "gemini");
    private static final Map<String, String> TEST_MESSAGES = Map.of(
        "auth_failed", "Upstream rejected credentials",
        "endpoint_not_found", "Models endpoint not found",
        "rate_limited", "Upstream rate limited the test",
        "redirect_rejected", "Upstream returned a redirect; redirects are not followed",
        "upstream_error", "Upstream returned a server error",
        "timeout", "Connection timed out",
        "network_error", "Network or TLS connection failed",
        "failed", "Connection test failed"
    );

    private final DataSource pool;
    private final ControlAuthService authService;
    private final HotRuntime hotRuntime;
    private final GatewayConfig config;
    private final ObjectMapper mapper;

    public ProvidersController(
        DataSource pool,
        ControlAuthService authService,
        HotRuntime hotRuntime,
        GatewayConfig config,
        ObjectMapper mapper
    ) {
        this.pool = pool;
        this.authService = authService;
        this.hotRuntime = hotRuntime;
        this.config = config;
        this.mapper = mapper;
    }

    @GetMapping("/api/providers")
    public Map<String, Object> listProviders(HttpServletRequest req) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT id::text, name, protocol, base_url, enabled, last_test_status, last_test_at, last_test_error, created_at, updated_at
                   FROM providers
                  WHERE workspace_id = ?::uuid
                  ORDER BY name
                 """)) {
            ps.setString(1, auth.workspaceId());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(readProviderPublicRow(rs));
                }
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to list providers");
        }
        return Map.of("items", items);
    }

    @PostMapping("/api/providers")
    public Map<String, Object> createProvider(HttpServletRequest req, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin"));
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        String name = requireString(body.get("name"), "name", 1, 100);
        String baseUrl = requireString(body.get("base_url"), "base_url", 1, 2000);
        String secret = requireSecretOrKeyList(body);
        String protocol = body.get("protocol") != null ? String.valueOf(body.get("protocol")).trim().toLowerCase() : "openai";
        if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
            throw new GatewayException(400, "validation_error", "protocol: must be one of openai, anthropic, gemini");
        }
        boolean enabled = body.get("enabled") == null || Boolean.TRUE.equals(body.get("enabled"));

        checkUrl(baseUrl);
        String id = UUID.randomUUID().toString();
        Map<String, Object> created;
        try (Connection conn = pool.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO providers (id, workspace_id, name, protocol, base_url, encrypted_secret, enabled)
                VALUES (?::uuid, ?::uuid, ?, ?, ?, ?, ?)
                """)) {
                ps.setString(1, id);
                ps.setString(2, auth.workspaceId());
                ps.setString(3, name);
                ps.setString(4, protocol);
                ps.setString(5, baseUrl);
                ps.setString(6, SecurityUtils.encrypt(secret, config.getEncryptionKey()));
                ps.setBoolean(7, enabled);
                ps.executeUpdate();
            }
            authService.audit(conn, auth, null, "provider.create", "provider", id, Map.of(
                "name", name, "protocol", protocol, "base_url", baseUrl, "enabled", enabled
            ));
            created = loadProviderPublic(conn, id);
        } catch (SQLException sqle) {
            if ("23505".equals(sqle.getSQLState())) {
                throw new GatewayException(409, "provider_exists", "Provider name already exists");
            }
            throw new GatewayException(500, "internal_error", "Failed to create provider");
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create provider");
        }
        hotRuntime.publish();
        return created;
    }

    @PatchMapping("/api/providers/{id}")
    public Map<String, Object> updateProvider(HttpServletRequest req, @PathVariable("id") String id, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin"));
        requireUuidOr404(id);
        if (body == null || body.isEmpty()) {
            throw new GatewayException(400, "validation_error", "No fields to update");
        }
        try (Connection conn = pool.getConnection()) {
            ensureProviderExists(conn, id, auth.workspaceId());
            List<String> sets = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            Map<String, Object> changes = new LinkedHashMap<>();

            if (body.containsKey("name") && body.get("name") != null) {
                String name = requireString(body.get("name"), "name", 1, 100);
                sets.add("name = ?");
                params.add(name);
                changes.put("name", name);
            }
            if (body.containsKey("protocol") && body.get("protocol") != null) {
                String protocol = String.valueOf(body.get("protocol")).trim().toLowerCase();
                if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
                    throw new GatewayException(400, "validation_error", "protocol: must be one of openai, anthropic, gemini");
                }
                sets.add("protocol = ?");
                params.add(protocol);
                changes.put("protocol", protocol);
            }
            if (body.containsKey("base_url") && body.get("base_url") != null) {
                String baseUrl = requireString(body.get("base_url"), "base_url", 1, 2000);
                checkUrl(baseUrl);
                sets.add("base_url = ?");
                params.add(baseUrl);
                changes.put("base_url", baseUrl);
            }
            if (body.containsKey("enabled") && body.get("enabled") != null) {
                if (!(body.get("enabled") instanceof Boolean b)) {
                    throw new GatewayException(400, "validation_error", "enabled must be boolean");
                }
                sets.add("enabled = ?");
                params.add(b);
                changes.put("enabled", b);
            }
            if ((body.containsKey("secret") && body.get("secret") != null) || (body.containsKey("secrets") && body.get("secrets") != null)) {
                String secret = requireSecretOrKeyList(body);
                sets.add("encrypted_secret = ?");
                params.add(SecurityUtils.encrypt(secret, config.getEncryptionKey()));
                changes.put("secret", "replaced");
            }
            if (sets.isEmpty()) {
                throw new GatewayException(400, "validation_error", "No fields to update");
            }
            try (PreparedStatement ps = conn.prepareStatement("UPDATE providers SET " + String.join(", ", sets) + ", updated_at = now() WHERE id = ?::uuid")) {
                for (int i = 0; i < params.size(); i++) {
                    Object p = params.get(i);
                    if (p instanceof Boolean b) ps.setBoolean(i + 1, b);
                    else ps.setString(i + 1, String.valueOf(p));
                }
                ps.setString(params.size() + 1, id);
                ps.executeUpdate();
            }
            authService.audit(conn, auth, null, "provider.update", "provider", id, changes);
            Map<String, Object> updated = loadProviderPublic(conn, id);
            hotRuntime.publish();
            return updated;
        } catch (GatewayException ge) {
            throw ge;
        } catch (SQLException sqle) {
            if ("23505".equals(sqle.getSQLState())) {
                throw new GatewayException(409, "provider_exists", "Provider name already exists");
            }
            throw new GatewayException(500, "internal_error", "Failed to update provider");
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to update provider");
        }
    }

    private String requireSecretOrKeyList(Map<String, Object> body) {
        if (body.get("secrets") instanceof List<?> list && !list.isEmpty()) {
            List<String> keys = new ArrayList<>();
            for (Object item : list) {
                String k = requireString(item, "secrets[]", 1, 2048);
                keys.add(k);
            }
            try {
                return mapper.writeValueAsString(keys);
            } catch (Exception e) {
                throw new GatewayException(400, "validation_error", "Invalid secrets array");
            }
        }
        return requireString(body.get("secret"), "secret", 1, 8192);
    }

    @DeleteMapping("/api/providers/{id}")
    public Map<String, Object> deleteProvider(HttpServletRequest req, @PathVariable("id") String id) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin"));
        requireUuidOr404(id);
        try (Connection conn = pool.getConnection()) {
            ensureProviderExists(conn, id, auth.workspaceId());
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT 1 FROM route_versions rv
                  JOIN model_targets mt ON mt.provider_id = ?::uuid
                 WHERE rv.targets::text LIKE '%' || mt.id::text || '%'
                 LIMIT 1
                """)) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        throw new GatewayException(409, "provider_in_use", "Provider targets are referenced by a route version");
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM providers WHERE id = ?::uuid")) {
                ps.setString(1, id);
                ps.executeUpdate();
            }
            authService.audit(conn, auth, null, "provider.delete", "provider", id, Map.of());
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to delete provider");
        }
        hotRuntime.publish();
        return Map.of("ok", true);
    }

    @PostMapping("/api/providers/{id}/test")
    public Map<String, Object> testProvider(HttpServletRequest req, @PathVariable("id") String id) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin"));
        requireUuidOr404(id);
        String baseUrl;
        String encryptedSecret;
        try (Connection conn = pool.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT base_url, encrypted_secret FROM providers WHERE id = ?::uuid AND workspace_id = ?::uuid")) {
                ps.setString(1, id);
                ps.setString(2, auth.workspaceId());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new GatewayException(404, "provider_not_found", "Provider not found");
                    }
                    baseUrl = rs.getString(1);
                    encryptedSecret = rs.getString(2);
                }
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to read provider");
        }

        String secret;
        try {
            secret = SecurityUtils.decrypt(encryptedSecret, config.getEncryptionKey());
            if (secret.startsWith("[")) {
                List<?> list = mapper.readValue(secret, List.class);
                if (!list.isEmpty()) secret = String.valueOf(list.get(0));
            }
        } catch (Exception e) {
            throw new GatewayException(500, "credential_error", "Provider credential could not be decrypted");
        }

        TestResult testResult;
        try {
            testResult = executeTestUpstream(baseUrl, secret);
        } catch (GatewayException ge) {
            try (Connection conn = pool.getConnection();
                 PreparedStatement ps = conn.prepareStatement("UPDATE providers SET last_test_status = 'invalid_url', last_test_at = now(), last_test_error = ? WHERE id = ?::uuid")) {
                ps.setString(1, ge.getMessage());
                ps.setString(2, id);
                ps.executeUpdate();
            } catch (Exception ignored) {}
            throw ge;
        }

        String msg = "ok".equals(testResult.status()) ? null : TEST_MESSAGES.getOrDefault(testResult.status(), "Connection test failed");
        try (Connection conn = pool.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement("UPDATE providers SET last_test_status = ?, last_test_at = now(), last_test_error = ? WHERE id = ?::uuid")) {
                ps.setString(1, testResult.status());
                ps.setString(2, msg);
                ps.setString(3, id);
                ps.executeUpdate();
            }
            authService.audit(conn, auth, null, "provider.test", "provider", id, Map.of(
                "status", testResult.status(), "latencyMs", testResult.latencyMs()
            ));
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to save test status");
        }
        hotRuntime.publish();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", testResult.status());
        out.put("latency_ms", testResult.latencyMs());
        out.put("message", msg);
        out.put("tested_at", Instant.now().toString());
        return out;
    }

    @GetMapping("/api/providers/{id}/targets")
    public Map<String, Object> listTargets(HttpServletRequest req, @PathVariable("id") String id) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        requireUuidOr404(id);
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection()) {
            ensureProviderExists(conn, id, auth.workspaceId());
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT id::text, provider_id::text, upstream_model, input_price, output_price,
                       cached_input_price, cache_write_price, reasoning_price,
                       capabilities::text, enabled, created_at, updated_at
                  FROM model_targets
                 WHERE provider_id = ?::uuid
                 ORDER BY upstream_model
                """)) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        items.add(readTargetRow(rs));
                    }
                }
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to list model targets");
        }
        return Map.of("items", items);
    }

    @PostMapping("/api/providers/{id}/targets")
    public Map<String, Object> createTarget(HttpServletRequest req, @PathVariable("id") String id, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin"));
        requireUuidOr404(id);
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        String upstreamModel = requireString(body.get("upstream_model"), "upstream_model", 1, 200);
        long inputPrice = requirePrice(body.get("input_price"), "input_price");
        long outputPrice = requirePrice(body.get("output_price"), "output_price");
        Long cachedInputPrice = optionalPrice(body.get("cached_input_price"), "cached_input_price");
        Long cacheWritePrice = optionalPrice(body.get("cache_write_price"), "cache_write_price");
        Long reasoningPrice = optionalPrice(body.get("reasoning_price"), "reasoning_price");
        List<String> capabilities = parseCapabilities(body.get("capabilities"));
        boolean enabled = body.get("enabled") == null || Boolean.TRUE.equals(body.get("enabled"));
        String targetId = UUID.randomUUID().toString();

        Map<String, Object> created;
        try (Connection conn = pool.getConnection()) {
            ensureProviderExists(conn, id, auth.workspaceId());
            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO model_targets (id, provider_id, upstream_model, input_price, output_price, cached_input_price, cache_write_price, reasoning_price, capabilities, enabled)
                VALUES (?::uuid, ?::uuid, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                RETURNING id::text, provider_id::text, upstream_model, input_price, output_price, cached_input_price, cache_write_price, reasoning_price, capabilities::text, enabled, created_at, updated_at
                """)) {
                ps.setString(1, targetId);
                ps.setString(2, id);
                ps.setString(3, upstreamModel);
                ps.setLong(4, inputPrice);
                ps.setLong(5, outputPrice);
                setNullableLong(ps, 6, cachedInputPrice);
                setNullableLong(ps, 7, cacheWritePrice);
                setNullableLong(ps, 8, reasoningPrice);
                ps.setString(9, mapper.writeValueAsString(capabilities));
                ps.setBoolean(10, enabled);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    created = readTargetRow(rs);
                }
            }
            Map<String, Object> changes = new LinkedHashMap<>();
            changes.put("provider_id", id);
            changes.put("upstream_model", upstreamModel);
            changes.put("input_price", inputPrice);
            changes.put("output_price", outputPrice);
            changes.put("cached_input_price", cachedInputPrice);
            changes.put("cache_write_price", cacheWritePrice);
            changes.put("reasoning_price", reasoningPrice);
            changes.put("capabilities", capabilities);
            changes.put("enabled", enabled);
            authService.audit(conn, auth, null, "target.create", "model_target", targetId, changes);
        } catch (GatewayException ge) {
            throw ge;
        } catch (SQLException sqle) {
            if ("23505".equals(sqle.getSQLState())) {
                throw new GatewayException(409, "target_exists", "Upstream model already configured for this provider");
            }
            throw new GatewayException(500, "internal_error", "Failed to create target");
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create target");
        }
        hotRuntime.publish();
        return created;
    }

    @PatchMapping("/api/targets/{id}")
    public Map<String, Object> updateTarget(HttpServletRequest req, @PathVariable("id") String id, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin"));
        requireUuidOr404(id);
        if (body == null || body.isEmpty()) {
            throw new GatewayException(400, "validation_error", "No fields to update");
        }
        try (Connection conn = pool.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT mt.id FROM model_targets mt JOIN providers p ON p.id = mt.provider_id
                 WHERE mt.id = ?::uuid AND p.workspace_id = ?::uuid
                """)) {
                ps.setString(1, id);
                ps.setString(2, auth.workspaceId());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new GatewayException(404, "target_not_found", "Model target not found");
                    }
                }
            }
            List<String> sets = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            if (body.containsKey("upstream_model") && body.get("upstream_model") != null) {
                sets.add("upstream_model = ?");
                params.add(requireString(body.get("upstream_model"), "upstream_model", 1, 200));
            }
            if (body.containsKey("input_price") && body.get("input_price") != null) {
                sets.add("input_price = ?");
                params.add(requirePrice(body.get("input_price"), "input_price"));
            }
            if (body.containsKey("output_price") && body.get("output_price") != null) {
                sets.add("output_price = ?");
                params.add(requirePrice(body.get("output_price"), "output_price"));
            }
            if (body.containsKey("cached_input_price")) {
                sets.add("cached_input_price = ?");
                params.add(optionalPrice(body.get("cached_input_price"), "cached_input_price"));
            }
            if (body.containsKey("cache_write_price")) {
                sets.add("cache_write_price = ?");
                params.add(optionalPrice(body.get("cache_write_price"), "cache_write_price"));
            }
            if (body.containsKey("reasoning_price")) {
                sets.add("reasoning_price = ?");
                params.add(optionalPrice(body.get("reasoning_price"), "reasoning_price"));
            }
            if (body.containsKey("enabled") && body.get("enabled") != null) {
                if (!(body.get("enabled") instanceof Boolean b)) throw new GatewayException(400, "validation_error", "enabled must be boolean");
                sets.add("enabled = ?");
                params.add(b);
            }
            if (body.containsKey("capabilities") && body.get("capabilities") != null) {
                List<String> caps = parseCapabilities(body.get("capabilities"));
                sets.add("capabilities = ?::jsonb");
                params.add(mapper.writeValueAsString(caps));
            }
            if (sets.isEmpty()) {
                throw new GatewayException(400, "validation_error", "No fields to update");
            }
            Map<String, Object> updated;
            try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE model_targets SET " + String.join(", ", sets) + ", updated_at = now() WHERE id = ?::uuid " +
                "RETURNING id::text, provider_id::text, upstream_model, input_price, output_price, cached_input_price, cache_write_price, reasoning_price, capabilities::text, enabled, created_at, updated_at")) {
                for (int i = 0; i < params.size(); i++) {
                    Object p = params.get(i);
                    if (p == null) ps.setNull(i + 1, Types.BIGINT);
                    else if (p instanceof Long l) ps.setLong(i + 1, l);
                    else if (p instanceof Boolean b) ps.setBoolean(i + 1, b);
                    else ps.setString(i + 1, String.valueOf(p));
                }
                ps.setString(params.size() + 1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    updated = readTargetRow(rs);
                }
            }
            authService.audit(conn, auth, null, "target.update", "model_target", id, body);
            hotRuntime.publish();
            return updated;
        } catch (GatewayException ge) {
            throw ge;
        } catch (SQLException sqle) {
            if ("23505".equals(sqle.getSQLState())) {
                throw new GatewayException(409, "target_exists", "Upstream model already configured for this provider");
            }
            throw new GatewayException(500, "internal_error", "Failed to update target");
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to update target");
        }
    }

    private record TestResult(String status, long latencyMs) {}

    private TestResult executeTestUpstream(String baseUrl, String secret) {
        checkUrl(baseUrl);
        URI endpoint;
        try {
            URI u = new URI(baseUrl.trim());
            String path = u.getPath() != null ? u.getPath() : "";
            if (!path.endsWith("/")) path += "/";
            path += "models";
            endpoint = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), path, null, null);
        } catch (Exception e) {
            throw new GatewayException(400, "invalid_provider_url", "provider URL is invalid");
        }
        long started = System.currentTimeMillis();
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) endpoint.toURL().openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(8000);
            conn.setReadTimeout(8000);
            conn.setRequestProperty("Authorization", "Bearer " + secret);
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("User-Agent", "ai-gateway-connection-test/1");
            int code = conn.getResponseCode();
            long lat = Math.max(0L, System.currentTimeMillis() - started);
            String status = "failed";
            if (code >= 200 && code < 300) status = "ok";
            else if (code == 401 || code == 403) status = "auth_failed";
            else if (code == 404) status = "endpoint_not_found";
            else if (code == 429) status = "rate_limited";
            else if (code >= 300 && code < 400) status = "redirect_rejected";
            else if (code >= 500) status = "upstream_error";
            return new TestResult(status, lat);
        } catch (SocketTimeoutException ste) {
            return new TestResult("timeout", Math.max(0L, System.currentTimeMillis() - started));
        } catch (SSLException ssle) {
            return new TestResult("network_error", Math.max(0L, System.currentTimeMillis() - started));
        } catch (Exception e) {
            return new TestResult("network_error", Math.max(0L, System.currentTimeMillis() - started));
        } finally {
            if (conn != null) {
                try { conn.disconnect(); } catch (Exception ignored) {}
            }
        }
    }

    private void checkUrl(String url) {
        try {
            SecurityUtils.validateProviderUrl(url, config.isAllowPrivateUpstream());
        } catch (ProviderUrlException pue) {
            throw new GatewayException(400, pue.code, pue.getMessage());
        }
    }

    private void ensureProviderExists(Connection conn, String id, String workspaceId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM providers WHERE id = ?::uuid AND workspace_id = ?::uuid")) {
            ps.setString(1, id);
            ps.setString(2, workspaceId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new GatewayException(404, "provider_not_found", "Provider not found");
                }
            }
        }
    }

    private Map<String, Object> loadProviderPublic(Connection conn, String id) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT id::text, name, protocol, base_url, enabled, last_test_status, last_test_at, last_test_error, created_at, updated_at
              FROM providers WHERE id = ?::uuid
            """)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return readProviderPublicRow(rs);
            }
        }
    }

    private Map<String, Object> readProviderPublicRow(ResultSet rs) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getString(1));
        row.put("name", rs.getString(2));
        row.put("protocol", rs.getString(3));
        row.put("base_url", rs.getString(4));
        row.put("enabled", rs.getBoolean(5));
        row.put("last_test_status", rs.getString(6));
        Timestamp tAt = rs.getTimestamp(7);
        row.put("last_test_at", tAt != null ? tAt.toInstant().toString() : null);
        row.put("last_test_error", rs.getString(8));
        Timestamp cAt = rs.getTimestamp(9);
        Timestamp uAt = rs.getTimestamp(10);
        row.put("created_at", cAt != null ? cAt.toInstant().toString() : null);
        row.put("updated_at", uAt != null ? uAt.toInstant().toString() : null);
        return row;
    }

    private Map<String, Object> readTargetRow(ResultSet rs) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getString(1));
        row.put("provider_id", rs.getString(2));
        row.put("upstream_model", rs.getString(3));
        row.put("input_price", rs.getLong(4));
        row.put("output_price", rs.getLong(5));
        long cIn = rs.getLong(6);
        row.put("cached_input_price", rs.wasNull() ? null : cIn);
        long cWr = rs.getLong(7);
        row.put("cache_write_price", rs.wasNull() ? null : cWr);
        long rPr = rs.getLong(8);
        row.put("reasoning_price", rs.wasNull() ? null : rPr);
        String capJson = rs.getString(9);
        row.put("capabilities", capJson != null ? mapper.readValue(capJson, List.class) : List.of());
        row.put("enabled", rs.getBoolean(10));
        Timestamp cAt = rs.getTimestamp(11);
        Timestamp uAt = rs.getTimestamp(12);
        row.put("created_at", cAt != null ? cAt.toInstant().toString() : null);
        row.put("updated_at", uAt != null ? uAt.toInstant().toString() : null);
        return row;
    }

    private static void setNullableLong(PreparedStatement ps, int index, Long val) throws Exception {
        if (val == null) ps.setNull(index, Types.BIGINT);
        else ps.setLong(index, val);
    }

    private static void requireUuidOr404(String id) {
        if (!ControlAuthService.isUuid(id)) {
            throw new GatewayException(404, "not_found", "Resource not found");
        }
    }

    private static String requireString(Object val, String field, int min, int max) {
        if (!(val instanceof String s)) throw new GatewayException(400, "validation_error", field + " is required");
        String clean = s.trim();
        if (clean.length() < min || clean.length() > max) {
            throw new GatewayException(400, "validation_error", field + " length invalid");
        }
        return clean;
    }

    private static long requirePrice(Object val, String field) {
        if (!(val instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue())) {
            throw new GatewayException(400, "validation_error", field + " must be an integer");
        }
        long l = n.longValue();
        if (l < 0 || l > HotContract.MAX_SAFE_INTEGER) {
            throw new GatewayException(400, "validation_error", field + " out of range");
        }
        return l;
    }

    private static Long optionalPrice(Object val, String field) {
        if (val == null) return null;
        return requirePrice(val, field);
    }

    private static List<String> parseCapabilities(Object val) {
        if (val == null) return List.of();
        if (!(val instanceof List<?> list) || list.size() > 20) {
            throw new GatewayException(400, "validation_error", "capabilities must be an array up to 20 items");
        }
        List<String> out = new ArrayList<>();
        for (Object o : list) {
            if (!(o instanceof String s)) throw new GatewayException(400, "validation_error", "capability must be string");
            String c = s.trim();
            if (c.isEmpty() || c.length() > 64) throw new GatewayException(400, "validation_error", "capability invalid");
            out.add(c);
        }
        return out;
    }
}
