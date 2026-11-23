package com.jihuayu.aigateway.control;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.config.GatewayConfig;
import com.jihuayu.aigateway.control.ControlAuthService.SessionInfo;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.GatewayTelemetry;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.netty.DataPlaneExecutor;
import com.jihuayu.aigateway.security.SecurityUtils;
import jakarta.servlet.http.HttpServletRequest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Control Plane MCP (Model Context Protocol) Server Registry & Observability Controller (#38, #39).
 */
@RestController
public class McpController {

    private static final Set<String> VALID_TRANSPORTS = Set.of("streamable_http", "sse");

    private final DataSource pool;
    private final ControlAuthService authService;
    private final HotRuntime hotRuntime;
    private final DataPlaneExecutor dataPlaneExecutor;
    private final GatewayConfig config;
    private final ObjectMapper mapper;

    public McpController(
        DataSource pool,
        ControlAuthService authService,
        HotRuntime hotRuntime,
        DataPlaneExecutor dataPlaneExecutor,
        GatewayConfig config,
        ObjectMapper mapper
    ) {
        this.pool = pool;
        this.authService = authService;
        this.hotRuntime = hotRuntime;
        this.dataPlaneExecutor = dataPlaneExecutor;
        this.config = config;
        this.mapper = mapper;
    }

    @GetMapping("/api/mcp-servers")
    public Map<String, Object> listMcpServers(HttpServletRequest req) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer", "viewer"));
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT id::text, workspace_id::text, alias, transport, endpoint_url,
                        allowed_tools::text, timeout_ms, enabled, created_at, updated_at
                   FROM mcp_servers
                  WHERE workspace_id = ?::uuid
                  ORDER BY alias ASC
                 """)) {
            ps.setString(1, auth.workspaceId());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(readMcpRow(rs));
                }
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "查询 MCP Server 列表失败: " + e.getMessage());
        }
        return Map.of("items", items);
    }

    @PostMapping("/api/mcp-servers")
    public ResponseEntity<Map<String, Object>> createMcpServer(
        HttpServletRequest req,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        if (body == null) {
            throw new GatewayException(400, "invalid_request", "请求体不能为空。");
        }
        String alias = body.get("alias") instanceof String s ? s.trim() : "";
        if (alias.isEmpty() || alias.length() > 100) {
            throw new GatewayException(400, "invalid_request", "alias 必须是非空字符串（1-100 字符）。");
        }
        String transport = body.get("transport") instanceof String s ? s.trim().toLowerCase() : "streamable_http";
        if (!VALID_TRANSPORTS.contains(transport)) {
            throw new GatewayException(400, "invalid_request", "transport 仅支持 streamable_http 或 sse。");
        }
        Object urlRaw = body.get("endpointUrl") != null ? body.get("endpointUrl") : body.get("endpoint_url");
        String endpointUrl = urlRaw instanceof String s ? s.trim() : "";
        if (endpointUrl.isEmpty()) {
            throw new GatewayException(400, "invalid_request", "endpointUrl 不能为空。");
        }
        try {
            SecurityUtils.validateProviderUrl(endpointUrl, config.isAllowPrivateUpstream());
        } catch (Exception e) {
            throw new GatewayException(400, "invalid_endpoint_url", "MCP Server 地址未通过安全校验: " + e.getMessage());
        }

        String encryptedSecret = null;
        if (body.get("secret") instanceof String sec && !sec.isBlank()) {
            encryptedSecret = SecurityUtils.encrypt(sec.trim(), config.getEncryptionKey());
        }

        List<String> allowedTools = new ArrayList<>();
        Object toolsRaw = body.get("allowedTools") != null ? body.get("allowedTools") : body.get("allowed_tools");
        if (toolsRaw instanceof List<?> list) {
            for (Object item : list) {
                if (item != null && !String.valueOf(item).isBlank()) {
                    allowedTools.add(String.valueOf(item).trim());
                }
            }
        }
        Object timeoutRaw = body.get("timeoutMs") != null ? body.get("timeoutMs") : body.get("timeout_ms");
        int timeoutMs = timeoutRaw instanceof Number n ? Math.max(500, n.intValue()) : 15_000;
        boolean enabled = !Boolean.FALSE.equals(body.get("enabled"));

        String id = UUID.randomUUID().toString();
        Map<String, Object> created;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 INSERT INTO mcp_servers(
                     id, workspace_id, alias, transport, endpoint_url,
                     encrypted_secret, allowed_tools, timeout_ms, enabled
                 )
                 VALUES(?::uuid, ?::uuid, ?, ?, ?, ?, ?::jsonb, ?, ?)
                 RETURNING id::text, workspace_id::text, alias, transport, endpoint_url,
                           allowed_tools::text, timeout_ms, enabled, created_at, updated_at
                 """)) {
            ps.setString(1, id);
            ps.setString(2, auth.workspaceId());
            ps.setString(3, alias);
            ps.setString(4, transport);
            ps.setString(5, endpointUrl);
            ps.setString(6, encryptedSecret);
            ps.setString(7, mapper.writeValueAsString(allowedTools));
            ps.setInt(8, timeoutMs);
            ps.setBoolean(9, enabled);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                created = readMcpRow(rs);
            }
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("unique")) {
                throw new GatewayException(409, "conflict", "同名 MCP Server 已存在: " + alias);
            }
            throw new GatewayException(500, "internal_error", "创建 MCP Server 失败: " + e.getMessage());
        }

        hotRuntime.publish();
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PatchMapping("/api/mcp-servers/{id}")
    public Map<String, Object> updateMcpServer(
        HttpServletRequest req,
        @PathVariable("id") String id,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        if (body == null || body.isEmpty()) {
            throw new GatewayException(400, "invalid_request", "请求体不能为空。");
        }
        Map<String, Object> updated;
        try (Connection conn = pool.getConnection()) {
            String curUrl;
            String curToolsJson;
            int curTimeout;
            boolean curEnabled;
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT endpoint_url, allowed_tools::text, timeout_ms, enabled
                  FROM mcp_servers
                 WHERE id = ?::uuid AND workspace_id = ?::uuid
                """)) {
                ps.setString(1, id);
                ps.setString(2, auth.workspaceId());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new GatewayException(404, "not_found", "MCP Server 不存在: " + id);
                    }
                    curUrl = rs.getString(1);
                    curToolsJson = rs.getString(2);
                    curTimeout = rs.getInt(3);
                    curEnabled = rs.getBoolean(4);
                }
            }

            Object urlRaw = body.get("endpointUrl") != null ? body.get("endpointUrl") : body.get("endpoint_url");
            if (urlRaw instanceof String u && !u.isBlank()) {
                SecurityUtils.validateProviderUrl(u.trim(), config.isAllowPrivateUpstream());
                curUrl = u.trim();
            }
            Object toolsRaw = body.get("allowedTools") != null ? body.get("allowedTools") : body.get("allowed_tools");
            if (toolsRaw instanceof List<?> list) {
                curToolsJson = mapper.writeValueAsString(list);
            }
            Object timeoutRaw = body.get("timeoutMs") != null ? body.get("timeoutMs") : body.get("timeout_ms");
            if (timeoutRaw instanceof Number n) {
                curTimeout = Math.max(500, n.intValue());
            }
            if (body.get("enabled") instanceof Boolean b) {
                curEnabled = b;
            }

            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE mcp_servers
                   SET endpoint_url = ?,
                       allowed_tools = ?::jsonb,
                       timeout_ms = ?,
                       enabled = ?,
                       updated_at = now()
                 WHERE id = ?::uuid AND workspace_id = ?::uuid
                 RETURNING id::text, workspace_id::text, alias, transport, endpoint_url,
                           allowed_tools::text, timeout_ms, enabled, created_at, updated_at
                """)) {
                ps.setString(1, curUrl);
                ps.setString(2, curToolsJson);
                ps.setInt(3, curTimeout);
                ps.setBoolean(4, curEnabled);
                ps.setString(5, id);
                ps.setString(6, auth.workspaceId());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    updated = readMcpRow(rs);
                }
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "更新 MCP Server 失败: " + e.getMessage());
        }
        hotRuntime.publish();
        return updated;
    }

    @DeleteMapping("/api/mcp-servers/{id}")
    public Map<String, Object> deleteMcpServer(
        HttpServletRequest req,
        @PathVariable("id") String id
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM mcp_servers WHERE id = ?::uuid AND workspace_id = ?::uuid")) {
            ps.setString(1, id);
            ps.setString(2, auth.workspaceId());
            if (ps.executeUpdate() == 0) {
                throw new GatewayException(404, "not_found", "MCP Server 不存在: " + id);
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "删除 MCP Server 失败: " + e.getMessage());
        }
        hotRuntime.publish();
        return Map.of("deleted", true, "id", id);
    }

    /**
     * #39: Prometheus OpenMetrics endpoint on Control Plane.
     */
    @GetMapping(value = "/metrics", produces = "text/plain; version=0.0.4; charset=utf-8")
    public ResponseEntity<String> prometheusMetrics() {
        return ResponseEntity.ok()
            .contentType(MediaType.parseMediaType("text/plain; version=0.0.4; charset=utf-8"))
            .body(dataPlaneExecutor.scrapePrometheusMetrics());
    }

    /**
     * #39: OpenTelemetry gen_ai.* span inspection API.
     */
    @GetMapping("/api/telemetry/spans")
    public Map<String, Object> listRecentSpans(HttpServletRequest req) {
        authService.requireWorkspaceRole(req, List.of("admin", "developer", "viewer"));
        return Map.of("items", GatewayTelemetry.global().recentSpans());
    }

    @GetMapping("/api/telemetry/access-logs")
    public Map<String, Object> listRecentAccessLogs(HttpServletRequest req) {
        authService.requireWorkspaceRole(req, List.of("admin", "developer", "viewer"));
        return Map.of("items", GatewayTelemetry.global().recentAccessLogs());
    }

    @PostMapping("/api/mcp-servers/{id}/probe")
    public Map<String, Object> probeMcpServer(HttpServletRequest req, @PathVariable("id") String id) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        String alias;
        String transport;
        String endpointUrl;
        List<String> allowedTools;
        int timeoutMs;
        boolean enabled;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT alias, transport, endpoint_url, allowed_tools::text, timeout_ms, enabled
                   FROM mcp_servers
                  WHERE id = ?::uuid AND workspace_id = ?::uuid
                 """)) {
            ps.setString(1, id);
            ps.setString(2, auth.workspaceId());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new GatewayException(404, "not_found", "MCP Server 不存在: " + id);
                }
                alias = rs.getString(1);
                transport = rs.getString(2);
                endpointUrl = rs.getString(3);
                String toolsRaw = rs.getString(4);
                allowedTools = (toolsRaw != null && !toolsRaw.isBlank())
                    ? mapper.readValue(toolsRaw, new TypeReference<List<String>>() {})
                    : List.of();
                timeoutMs = rs.getInt(5);
                enabled = rs.getBoolean(6);
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "读取 MCP Server 失败: " + e.getMessage());
        }

        long t0 = System.nanoTime();
        String probeStatus = enabled ? "ok" : "disabled";
        String probeMessage = enabled ? "MCP 端点配置与安全策略校验通过" : "该 MCP Server 当前处于禁用状态";
        try {
            SecurityUtils.validateProviderUrl(endpointUrl, config.isAllowPrivateUpstream());
        } catch (Exception e) {
            probeStatus = "invalid_url";
            probeMessage = e.getMessage();
        }
        long latencyMs = Math.max(1L, (System.nanoTime() - t0) / 1_000_000L);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("alias", alias);
        out.put("transport", transport);
        out.put("endpointUrl", endpointUrl);
        out.put("timeoutMs", timeoutMs);
        out.put("status", probeStatus);
        out.put("message", probeMessage);
        out.put("latencyMs", latencyMs);
        out.put("allowedTools", allowedTools);
        out.put("testedAt", java.time.Instant.now().toString());
        return out;
    }

    @PostMapping("/api/telemetry/circuit-breakers/reset")
    public Map<String, Object> resetCircuitBreakers(HttpServletRequest req) {
        authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        dataPlaneExecutor.resetCircuitBreakers();
        return Map.of(
            "reset", true,
            "openCircuitBreakers", 0,
            "resetAt", java.time.Instant.now().toString()
        );
    }

    @GetMapping("/api/billing/dlq")
    public Map<String, Object> listBillingDlq(HttpServletRequest req) {
        authService.requireWorkspaceRole(req, List.of("admin", "developer", "viewer"));
        var sink = dataPlaneExecutor.pluginPipeline().webhookBillingSinkPlugin();
        return Map.of(
            "items", sink.dlqEvents(),
            "dlqCount", sink.dlqEvents().size(),
            "recentExportedCount", sink.recentExportedEvents().size()
        );
    }

    @PostMapping("/api/billing/dlq/clear")
    public Map<String, Object> clearBillingDlq(HttpServletRequest req) {
        authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        int cleared = dataPlaneExecutor.pluginPipeline().webhookBillingSinkPlugin().clearDlqEvents();
        return Map.of("cleared", cleared);
    }

    @GetMapping("/api/telemetry/runtime")
    public GatewayTelemetry.KernelEngineSnapshot inspectRuntimeTelemetry(HttpServletRequest req) {
        authService.requireWorkspaceRole(req, List.of("admin", "developer", "viewer"));
        return dataPlaneExecutor.captureKernelEngineSnapshot();
    }

    private Map<String, Object> readMcpRow(ResultSet rs) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getString(1));
        row.put("workspaceId", rs.getString(2));
        row.put("alias", rs.getString(3));
        row.put("transport", rs.getString(4));
        row.put("endpointUrl", rs.getString(5));
        String toolsRaw = rs.getString(6);
        row.put("allowedTools", (toolsRaw != null && !toolsRaw.isBlank())
            ? mapper.readValue(toolsRaw, new TypeReference<List<String>>() {})
            : List.of());
        row.put("timeoutMs", rs.getInt(7));
        row.put("enabled", rs.getBoolean(8));
        row.put("createdAt", rs.getTimestamp(9).toInstant().toString());
        row.put("updatedAt", rs.getTimestamp(10).toInstant().toString());
        return row;
    }
}
