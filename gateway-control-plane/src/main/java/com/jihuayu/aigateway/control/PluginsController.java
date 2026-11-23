package com.jihuayu.aigateway.control;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.control.ControlAuthService.SessionInfo;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.netty.DataPlaneExecutor;
import com.jihuayu.aigateway.plugin.ExtProcPlugin;
import com.jihuayu.aigateway.plugin.GatewayPlugin;
import com.jihuayu.aigateway.plugin.JsPluginSandbox;
import com.jihuayu.aigateway.plugin.JsPluginSandbox.JsSandboxSecurityException;
import com.jihuayu.aigateway.plugin.PluginExecutionContext;
import com.jihuayu.aigateway.plugin.PluginPipeline;
import com.jihuayu.aigateway.plugin.PluginResult;
import com.jihuayu.aigateway.plugin.PluginStage;
import com.jihuayu.aigateway.security.SecurityUtils;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.file.Path;
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
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Control Plane Plugin Management API, Scoped Binding Registry, Dynamic JAR Loader & Online Dry-Run Debugger (#36, #41, #42, #37, #51, #52, #53, #54).
 */
@RestController
public class PluginsController {

    private static final Set<String> VALID_PLUGIN_TYPES = Set.of("BUILTIN", "JAVA_SPI", "JS", "WEBHOOK");
    private static final Set<String> VALID_FAIL_MODES = Set.of("FAIL_OPEN", "FAIL_CLOSED");
    private static final Set<String> VALID_SCOPE_TYPES = Set.of("GLOBAL", "WORKSPACE", "PROJECT", "ALIAS", "API_KEY");

    private final DataSource pool;
    private final ControlAuthService authService;
    private final HotRuntime hotRuntime;
    private final DataPlaneExecutor dataPlaneExecutor;
    private final ObjectMapper mapper;

    public PluginsController(
        DataSource pool,
        ControlAuthService authService,
        HotRuntime hotRuntime,
        DataPlaneExecutor dataPlaneExecutor,
        ObjectMapper mapper
    ) {
        this.pool = pool;
        this.authService = authService;
        this.hotRuntime = hotRuntime;
        this.dataPlaneExecutor = dataPlaneExecutor;
        this.mapper = mapper;
    }

    @GetMapping("/api/plugins")
    public Map<String, Object> listPlugins(HttpServletRequest req) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer", "viewer"));
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT id::text, workspace_id::text, name, type, version, priority,
                        stages::text, source_code, code_sha256, jar_path, class_name,
                        config::text, fail_mode, timeout_ms, max_statements, enabled,
                        created_at, updated_at
                   FROM plugins
                  WHERE workspace_id = ?::uuid OR workspace_id IS NULL
                  ORDER BY priority ASC, name ASC
                 """)) {
            ps.setString(1, auth.workspaceId());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(readPluginRow(rs));
                }
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "查询插件列表失败: " + e.getMessage());
        }
        return Map.of("items", items);
    }

    @PostMapping("/api/plugins")
    public ResponseEntity<Map<String, Object>> createPlugin(
        HttpServletRequest req,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        if (body == null) {
            throw new GatewayException(400, "invalid_request", "请求体不能为空。");
        }
        String name = body.get("name") != null ? String.valueOf(body.get("name")).trim() : "";
        if (name.isEmpty()) {
            throw new GatewayException(400, "invalid_request", "插件名称 name 不能为空。");
        }
        String type = body.get("type") != null ? String.valueOf(body.get("type")).trim().toUpperCase() : "JS";
        if (!VALID_PLUGIN_TYPES.contains(type)) {
            throw new GatewayException(400, "invalid_request", "不支持的插件类型: " + type);
        }
        int priority = body.get("priority") instanceof Number n ? n.intValue() : 100;
        String failMode = body.get("failMode") != null ? String.valueOf(body.get("failMode")).trim().toUpperCase() : "FAIL_CLOSED";
        if (!VALID_FAIL_MODES.contains(failMode)) {
            throw new GatewayException(400, "invalid_request", "不支持的 failMode: " + failMode);
        }
        int timeoutMs = body.get("timeoutMs") instanceof Number n ? Math.max(1, n.intValue()) : 50;
        int maxStatements = body.get("maxStatements") instanceof Number n ? Math.max(10, n.intValue()) : 10_000;
        boolean enabled = !Boolean.FALSE.equals(body.get("enabled"));

        List<String> stages = new ArrayList<>();
        if (body.get("stages") instanceof List<?> rawStages && !rawStages.isEmpty()) {
            for (Object s : rawStages) {
                stages.add(PluginStage.fromString(String.valueOf(s)).name());
            }
        } else {
            stages.add(PluginStage.PRE_ADMIT.name());
        }

        String sourceCode = body.get("sourceCode") instanceof String s ? s : null;
        String codeSha256 = null;
        String jarPath = body.get("jarPath") instanceof String s ? s : null;
        String className = body.get("className") instanceof String s ? s : null;
        String id = UUID.randomUUID().toString();

        if ("JS".equals(type)) {
            if (sourceCode == null || sourceCode.isBlank()) {
                throw new GatewayException(400, "invalid_request", "JS 插件必须提供 sourceCode。");
            }
            codeSha256 = SecurityUtils.hashKey(sourceCode);
            try {
                JsPluginSandbox.compile(id, name, codeSha256, sourceCode, priority, maxStatements);
            } catch (JsSandboxSecurityException secEx) {
                throw new GatewayException(400, "js_sandbox_security_violation", secEx.getMessage());
            } catch (Exception ex) {
                throw new GatewayException(400, "invalid_js_syntax", "JS 插件编译校验失败: " + ex.getMessage());
            }
        } else if ("JAVA_SPI".equals(type) && jarPath != null && !jarPath.isBlank()) {
            try {
                dataPlaneExecutor.pluginPipeline().jarLoader().loadJar(Path.of(jarPath));
            } catch (Exception ex) {
                throw new GatewayException(400, "jar_load_failed", ex.getMessage());
            }
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> configMap = body.get("config") instanceof Map<?, ?> m
            ? (Map<String, Object>) m
            : Map.of();

        Map<String, Object> created;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 INSERT INTO plugins(
                     id, workspace_id, name, type, version, priority, stages,
                     source_code, code_sha256, jar_path, class_name, config,
                     fail_mode, timeout_ms, max_statements, enabled
                 )
                 VALUES(?::uuid, ?::uuid, ?, ?, 1, ?, ?::jsonb, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?)
                 RETURNING id::text, workspace_id::text, name, type, version, priority,
                           stages::text, source_code, code_sha256, jar_path, class_name,
                           config::text, fail_mode, timeout_ms, max_statements, enabled,
                           created_at, updated_at
                 """)) {
            ps.setString(1, id);
            ps.setString(2, auth.workspaceId());
            ps.setString(3, name);
            ps.setString(4, type);
            ps.setInt(5, priority);
            ps.setString(6, mapper.writeValueAsString(stages));
            ps.setString(7, sourceCode);
            ps.setString(8, codeSha256);
            ps.setString(9, jarPath);
            ps.setString(10, className);
            ps.setString(11, mapper.writeValueAsString(configMap));
            ps.setString(12, failMode);
            ps.setInt(13, timeoutMs);
            ps.setInt(14, maxStatements);
            ps.setBoolean(15, enabled);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                created = readPluginRow(rs);
            }
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("unique")) {
                throw new GatewayException(409, "conflict", "同名插件已存在: " + name);
            }
            throw new GatewayException(500, "internal_error", "创建插件失败: " + e.getMessage());
        }

        hotRuntime.publish();
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PatchMapping("/api/plugins/{pluginId}")
    public Map<String, Object> updatePlugin(
        HttpServletRequest req,
        @PathVariable("pluginId") String pluginId,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        if (body == null) {
            throw new GatewayException(400, "invalid_request", "请求体不能为空。");
        }

        Map<String, Object> updated;
        try (Connection conn = pool.getConnection()) {
            String curName;
            String curType;
            int curPriority;
            String curStagesJson;
            String curSourceCode;
            String curCodeSha;
            String curConfigJson;
            String curFailMode;
            int curTimeoutMs;
            int curMaxStatements;
            boolean curEnabled;

            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT name, type, priority, stages::text, source_code, code_sha256,
                       config::text, fail_mode, timeout_ms, max_statements, enabled
                  FROM plugins
                 WHERE id = ?::uuid AND (workspace_id = ?::uuid OR workspace_id IS NULL)
                """)) {
                ps.setString(1, pluginId);
                ps.setString(2, auth.workspaceId());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new GatewayException(404, "not_found", "插件不存在: " + pluginId);
                    }
                    curName = rs.getString(1);
                    curType = rs.getString(2);
                    curPriority = rs.getInt(3);
                    curStagesJson = rs.getString(4);
                    curSourceCode = rs.getString(5);
                    curCodeSha = rs.getString(6);
                    curConfigJson = rs.getString(7);
                    curFailMode = rs.getString(8);
                    curTimeoutMs = rs.getInt(9);
                    curMaxStatements = rs.getInt(10);
                    curEnabled = rs.getBoolean(11);
                }
            }

            if (body.get("priority") instanceof Number n) curPriority = n.intValue();
            if (body.get("failMode") instanceof String fm) {
                String up = fm.trim().toUpperCase();
                if (!VALID_FAIL_MODES.contains(up)) {
                    throw new GatewayException(400, "invalid_request", "不支持的 failMode: " + fm);
                }
                curFailMode = up;
            }
            if (body.get("timeoutMs") instanceof Number n) curTimeoutMs = Math.max(1, n.intValue());
            if (body.get("maxStatements") instanceof Number n) curMaxStatements = Math.max(10, n.intValue());
            if (body.get("enabled") instanceof Boolean b) curEnabled = b;
            if (body.get("stages") instanceof List<?> rawStages && !rawStages.isEmpty()) {
                List<String> st = new ArrayList<>();
                for (Object s : rawStages) st.add(PluginStage.fromString(String.valueOf(s)).name());
                curStagesJson = mapper.writeValueAsString(st);
            }
            if (body.get("config") instanceof Map<?, ?> cfgMap) {
                curConfigJson = mapper.writeValueAsString(cfgMap);
            }
            if (body.get("sourceCode") instanceof String newSrc && !newSrc.isBlank()) {
                curSourceCode = newSrc;
                curCodeSha = SecurityUtils.hashKey(newSrc);
                if ("JS".equals(curType)) {
                    try {
                        JsPluginSandbox.compile(pluginId, curName, curCodeSha, curSourceCode, curPriority, curMaxStatements);
                    } catch (JsSandboxSecurityException secEx) {
                        throw new GatewayException(400, "js_sandbox_security_violation", secEx.getMessage());
                    } catch (Exception ex) {
                        throw new GatewayException(400, "invalid_js_syntax", "JS 插件编译校验失败: " + ex.getMessage());
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE plugins
                   SET version = version + 1,
                       priority = ?,
                       stages = ?::jsonb,
                       source_code = ?,
                       code_sha256 = ?,
                       config = ?::jsonb,
                       fail_mode = ?,
                       timeout_ms = ?,
                       max_statements = ?,
                       enabled = ?,
                       updated_at = now()
                 WHERE id = ?::uuid
                 RETURNING id::text, workspace_id::text, name, type, version, priority,
                           stages::text, source_code, code_sha256, jar_path, class_name,
                           config::text, fail_mode, timeout_ms, max_statements, enabled,
                           created_at, updated_at
                """)) {
                ps.setInt(1, curPriority);
                ps.setString(2, curStagesJson);
                ps.setString(3, curSourceCode);
                ps.setString(4, curCodeSha);
                ps.setString(5, curConfigJson);
                ps.setString(6, curFailMode);
                ps.setInt(7, curTimeoutMs);
                ps.setInt(8, curMaxStatements);
                ps.setBoolean(9, curEnabled);
                ps.setString(10, pluginId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    updated = readPluginRow(rs);
                }
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "更新插件失败: " + e.getMessage());
        }

        hotRuntime.publish();
        return updated;
    }

    @DeleteMapping("/api/plugins/{pluginId}")
    public Map<String, Object> deletePlugin(
        HttpServletRequest req,
        @PathVariable("pluginId") String pluginId
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM plugins WHERE id = ?::uuid AND (workspace_id = ?::uuid OR workspace_id IS NULL)")) {
            ps.setString(1, pluginId);
            ps.setString(2, auth.workspaceId());
            int affected = ps.executeUpdate();
            if (affected == 0) {
                throw new GatewayException(404, "not_found", "插件不存在: " + pluginId);
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "删除插件失败: " + e.getMessage());
        }
        hotRuntime.publish();
        return Map.of("deleted", true, "id", pluginId);
    }

    @GetMapping("/api/plugins/bindings")
    public Map<String, Object> listBindings(HttpServletRequest req) {
        authService.requireWorkspaceRole(req, List.of("admin", "developer", "viewer"));
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT id::text, plugin_id::text, scope_type, scope_id, priority,
                        config_override::text, enabled, COALESCE(sample_rate, 1.0) AS sample_rate, created_at
                   FROM plugin_bindings
                  ORDER BY priority ASC, created_at ASC
                 """);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", rs.getString(1));
                row.put("pluginId", rs.getString(2));
                row.put("scopeType", rs.getString(3));
                row.put("scopeId", rs.getString(4));
                row.put("priority", rs.getInt(5));
                String cfg = rs.getString(6);
                row.put("configOverride", (cfg != null && !cfg.isBlank()) ? mapper.readValue(cfg, Map.class) : Map.of());
                row.put("enabled", rs.getBoolean(7));
                row.put("sampleRate", rs.getDouble(8));
                row.put("createdAt", rs.getTimestamp(9).toInstant().toString());
                items.add(row);
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "查询插件绑定失败: " + e.getMessage());
        }
        return Map.of("items", items);
    }

    @PostMapping("/api/plugins/bindings")
    public ResponseEntity<Map<String, Object>> createBinding(
        HttpServletRequest req,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        if (body == null || body.get("pluginId") == null) {
            throw new GatewayException(400, "invalid_request", "pluginId 不能为空。");
        }
        String pluginId = String.valueOf(body.get("pluginId")).trim();
        String scopeType = body.get("scopeType") != null ? String.valueOf(body.get("scopeType")).trim().toUpperCase() : "GLOBAL";
        if (!VALID_SCOPE_TYPES.contains(scopeType)) {
            throw new GatewayException(400, "invalid_request", "不支持的 scopeType: " + scopeType);
        }
        String scopeId = body.get("scopeId") != null ? String.valueOf(body.get("scopeId")).trim() : "*";
        int priority = body.get("priority") instanceof Number n ? n.intValue() : 100;
        boolean enabled = !Boolean.FALSE.equals(body.get("enabled"));
        double sampleRate = body.get("sampleRate") instanceof Number n
            ? Math.max(0.0, Math.min(1.0, n.doubleValue()))
            : 1.0;
        @SuppressWarnings("unchecked")
        Map<String, Object> configOverride = body.get("configOverride") instanceof Map<?, ?> m
            ? (Map<String, Object>) m
            : Map.of();

        String id = UUID.randomUUID().toString();
        Map<String, Object> created = new LinkedHashMap<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 INSERT INTO plugin_bindings(id, plugin_id, scope_type, scope_id, priority, config_override, enabled, sample_rate)
                 VALUES(?::uuid, ?::uuid, ?, ?, ?, ?::jsonb, ?, ?)
                 """)) {
            ps.setString(1, id);
            ps.setString(2, pluginId);
            ps.setString(3, scopeType);
            ps.setString(4, scopeId);
            ps.setInt(5, priority);
            ps.setString(6, mapper.writeValueAsString(configOverride));
            ps.setBoolean(7, enabled);
            ps.setDouble(8, sampleRate);
            ps.executeUpdate();
            created.put("id", id);
            created.put("pluginId", pluginId);
            created.put("scopeType", scopeType);
            created.put("scopeId", scopeId);
            created.put("priority", priority);
            created.put("configOverride", configOverride);
            created.put("enabled", enabled);
            created.put("sampleRate", sampleRate);
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "创建插件绑定失败: " + e.getMessage());
        }

        hotRuntime.publish();
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PatchMapping("/api/plugins/bindings/{bindingId}")
    @SuppressWarnings("unchecked")
    public Map<String, Object> updateBinding(
        HttpServletRequest req,
        @PathVariable("bindingId") String bindingId,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        if (body == null || body.isEmpty()) {
            throw new GatewayException(400, "invalid_request", "请求体不能为空。");
        }
        Map<String, Object> updated = new LinkedHashMap<>();
        try (Connection conn = pool.getConnection()) {
            String pluginId;
            String scopeType;
            String scopeId;
            int priority;
            String cfgJson;
            boolean enabled;
            double sampleRate;
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT plugin_id::text, scope_type, scope_id, priority, config_override::text, enabled, COALESCE(sample_rate, 1.0)
                  FROM plugin_bindings
                 WHERE id = ?::uuid
                """)) {
                ps.setString(1, bindingId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new GatewayException(404, "not_found", "绑定不存在: " + bindingId);
                    }
                    pluginId = rs.getString(1);
                    scopeType = rs.getString(2);
                    scopeId = rs.getString(3);
                    priority = rs.getInt(4);
                    cfgJson = rs.getString(5);
                    enabled = rs.getBoolean(6);
                    sampleRate = rs.getDouble(7);
                }
            }
            if (body.get("scopeType") instanceof String st && !st.isBlank()) {
                String up = st.trim().toUpperCase();
                if (!VALID_SCOPE_TYPES.contains(up)) {
                    throw new GatewayException(400, "invalid_request", "不支持的 scopeType: " + st);
                }
                scopeType = up;
            }
            if (body.get("scopeId") instanceof String sid && !sid.isBlank()) {
                scopeId = sid.trim();
            }
            if (body.get("priority") instanceof Number n) {
                priority = n.intValue();
            }
            if (body.get("sampleRate") instanceof Number n) {
                sampleRate = Math.max(0.0, Math.min(1.0, n.doubleValue()));
            }
            if (body.get("enabled") instanceof Boolean b) {
                enabled = b;
            }
            if (body.get("configOverride") instanceof Map<?, ?> m) {
                cfgJson = mapper.writeValueAsString(m);
            }
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE plugin_bindings
                   SET scope_type = ?, scope_id = ?, priority = ?, config_override = ?::jsonb, enabled = ?, sample_rate = ?
                 WHERE id = ?::uuid
                """)) {
                ps.setString(1, scopeType);
                ps.setString(2, scopeId);
                ps.setInt(3, priority);
                ps.setString(4, cfgJson != null && !cfgJson.isBlank() ? cfgJson : "{}");
                ps.setBoolean(5, enabled);
                ps.setDouble(6, sampleRate);
                ps.setString(7, bindingId);
                ps.executeUpdate();
            }
            updated.put("id", bindingId);
            updated.put("pluginId", pluginId);
            updated.put("scopeType", scopeType);
            updated.put("scopeId", scopeId);
            updated.put("priority", priority);
            updated.put("configOverride", cfgJson != null && !cfgJson.isBlank() ? mapper.readValue(cfgJson, Map.class) : Map.of());
            updated.put("enabled", enabled);
            updated.put("sampleRate", sampleRate);
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "更新插件绑定失败: " + e.getMessage());
        }
        hotRuntime.publish();
        return updated;
    }

    @PostMapping("/api/plugins/resolve-chain")
    public Map<String, Object> resolveEffectivePluginChain(
        HttpServletRequest req,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer", "viewer"));
        String projectId = body != null && body.get("projectId") instanceof String s && !s.isBlank() ? s.trim() : null;
        String aliasName = body != null && body.get("aliasName") instanceof String s && !s.isBlank() ? s.trim() : null;
        String keyId = body != null && body.get("keyId") instanceof String s && !s.isBlank() ? s.trim() : null;

        var snapshot = hotRuntime.latestSnapshot();
        PluginPipeline pipeline = dataPlaneExecutor.pluginPipeline();
        List<Map<String, Object>> stageChains = new ArrayList<>();
        int totalInvocations = 0;

        for (PluginStage stage : PluginStage.values()) {
            List<PluginPipeline.BoundPluginInvocation> bound = pipeline.resolveStagePlugins(
                snapshot, stage, auth.workspaceId(), projectId, aliasName, keyId
            );
            List<Map<String, Object>> pluginsOut = new ArrayList<>();
            for (PluginPipeline.BoundPluginInvocation inv : bound) {
                Map<String, Object> pMap = new LinkedHashMap<>();
                pMap.put("pluginId", inv.metadata() != null ? inv.metadata().id() : inv.plugin().id());
                pMap.put("name", inv.metadata() != null ? inv.metadata().name() : inv.plugin().name());
                pMap.put("type", inv.metadata() != null ? inv.metadata().type() : inv.plugin().type());
                pMap.put("effectivePriority", inv.effectivePriority());
                pMap.put("failMode", inv.failMode());
                pMap.put("sampleRate", inv.sampleRate());
                pMap.put("mergedConfig", inv.mergedConfig());
                pluginsOut.add(pMap);
                totalInvocations++;
            }
            Map<String, Object> stRow = new LinkedHashMap<>();
            stRow.put("stage", stage.name());
            stRow.put("count", pluginsOut.size());
            stRow.put("plugins", pluginsOut);
            stageChains.add(stRow);
        }

        Map<String, Object> res = new LinkedHashMap<>();
        res.put("workspaceId", auth.workspaceId());
        res.put("projectId", projectId != null ? projectId : "*");
        res.put("aliasName", aliasName != null ? aliasName : "*");
        res.put("keyId", keyId != null ? keyId : "*");
        res.put("totalInvocations", totalInvocations);
        res.put("stages", stageChains);
        return res;
    }

    @DeleteMapping("/api/plugins/bindings/{bindingId}")
    public Map<String, Object> deleteBinding(
        HttpServletRequest req,
        @PathVariable("bindingId") String bindingId
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM plugin_bindings WHERE id = ?::uuid")) {
            ps.setString(1, bindingId);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "删除插件绑定失败: " + e.getMessage());
        }
        hotRuntime.publish();
        return Map.of("deleted", true, "id", bindingId);
    }

    /**
     * JVM-mode Dynamic JAR Hot-Loader endpoint (#36).
     */
    @PostMapping("/api/plugins/load-jar")
    public Map<String, Object> loadJarPlugin(
        HttpServletRequest req,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        if (body == null || !(body.get("jarPath") instanceof String jarPath) || jarPath.isBlank()) {
            throw new GatewayException(400, "invalid_request", "jarPath 不能为空。");
        }
        try {
            List<GatewayPlugin> loaded = dataPlaneExecutor.pluginPipeline().jarLoader().loadJar(Path.of(jarPath.trim()));
            List<Map<String, Object>> pluginSummaries = new ArrayList<>();
            for (GatewayPlugin gp : loaded) {
                pluginSummaries.add(Map.of(
                    "id", gp.id(),
                    "name", gp.name(),
                    "type", gp.type(),
                    "priority", gp.priority(),
                    "className", gp.getClass().getName()
                ));
            }
            return Map.of("loadedCount", loaded.size(), "plugins", pluginSummaries);
        } catch (Exception e) {
            throw new GatewayException(400, "jar_load_failed", e.getMessage());
        }
    }

    /**
     * Online Dry-Run Debugger (#42, #52): Executes any inline or registered plugin against a sample request/response
     * without affecting live production traffic or financial ledgers.
     */
    @PostMapping("/api/plugins/dry-run")
    @SuppressWarnings("unchecked")
    public Map<String, Object> dryRunPlugin(
        HttpServletRequest req,
        @RequestBody(required = false) Map<String, Object> body
    ) {
        SessionInfo auth = authService.requireWorkspaceRole(req, List.of("admin", "developer"));
        if (body == null) {
            throw new GatewayException(400, "invalid_request", "请求体不能为空。");
        }

        PluginStage stage = PluginStage.fromString(body.get("stage") instanceof String s ? s : "PRE_ADMIT");
        String failMode = body.get("failMode") instanceof String s ? s : "FAIL_CLOSED";
        int maxStatements = body.get("maxStatements") instanceof Number n ? Math.max(10, n.intValue()) : 10_000;
        Map<String, Object> config = body.get("config") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        Map<String, String> reqHeaders = new LinkedHashMap<>();
        if (body.get("headers") instanceof Map<?, ?> hm) {
            for (Map.Entry<?, ?> e : hm.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    reqHeaders.put(String.valueOf(e.getKey()).toLowerCase(), String.valueOf(e.getValue()));
                }
            }
        }
        Map<String, Object> reqJson = body.get("requestBody") instanceof Map<?, ?> rm
            ? new LinkedHashMap<>((Map<String, Object>) rm)
            : new LinkedHashMap<>();

        String rawQuery = body.get("rawQuery") instanceof String q ? q : "";
        String modelAlias = reqJson.get("model") instanceof String s ? s : "support-chat";
        PluginPipeline pipeline = dataPlaneExecutor.pluginPipeline();
        PluginExecutionContext ctx = new PluginExecutionContext(
            "dryrun-" + UUID.randomUUID(),
            auth.workspaceId(),
            auth.workspaceId(),
            "dryrun-key",
            "/v1/chat/completions",
            rawQuery,
            modelAlias,
            Boolean.TRUE.equals(reqJson.get("stream")),
            reqHeaders,
            reqJson,
            pipeline.createKvStore()
        );
        if (body.get("responseBody") instanceof String respBody) {
            ctx.setResponseBody(respBody);
            ctx.setResponseStatus(200);
        }
        if (body.get("streamChunk") instanceof String chunk) {
            ctx.setStreamChunk(chunk);
        }

        GatewayPlugin targetPlugin;
        String type = body.get("type") instanceof String s ? s.trim().toUpperCase() : "JS";
        if (body.get("sourceCode") instanceof String src && !src.isBlank()) {
            try {
                String sha = SecurityUtils.hashKey(src);
                targetPlugin = JsPluginSandbox.compile("dryrun-js", "dryrun-js", sha, src, 100, maxStatements);
            } catch (JsSandboxSecurityException secEx) {
                throw new GatewayException(400, "js_sandbox_security_violation", secEx.getMessage());
            } catch (Exception ex) {
                throw new GatewayException(400, "invalid_js_syntax", ex.getMessage());
            }
        } else if ("WEBHOOK".equals(type)) {
            targetPlugin = new ExtProcPlugin("dryrun-webhook", "dryrun-webhook", 100, null, mapper);
        } else {
            String nameOrId = body.get("pluginId") instanceof String s ? s : String.valueOf(body.getOrDefault("name", "pii-guardrail"));
            targetPlugin = pipeline.jarLoader().getPlugin(nameOrId);
            if (targetPlugin == null) {
                throw new GatewayException(404, "not_found", "未找到插件: " + nameOrId);
            }
        }

        PluginResult result = pipeline.invokeSingle(targetPlugin, stage, ctx, config, failMode);
        long executedStatements = ctx.traceLog().isEmpty() ? 0L : ctx.traceLog().getLast().executedStatements();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("stage", stage.name());
        out.put("executedStatements", executedStatements);
        out.put("mutatedModelAlias", ctx.modelAlias());
        out.put("boundApiKey", ctx.boundApiKeyPlain());
        out.put("mutatedRequestHeaders", ctx.requestHeaders());
        out.put("mutatedRequestBody", ctx.requestJson());
        out.put("mutatedResponseHeaders", ctx.responseHeaders());
        out.put("mutatedResponseBody", ctx.responseBody());
        out.put("mutatedStreamChunk", ctx.streamChunk());
        out.put("attributionTags", ctx.attributionTags());
        out.put("trace", ctx.traceLog());

        switch (result) {
            case PluginResult.Continue _ -> {
                out.put("action", "CONTINUE");
                out.put("httpStatus", 200);
            }
            case PluginResult.Reject rej -> {
                out.put("action", "REJECT");
                out.put("httpStatus", rej.httpStatus());
                out.put("errorCode", rej.errorCode());
                out.put("message", rej.message());
            }
            case PluginResult.ShortCircuit sc -> {
                out.put("action", "SHORT_CIRCUIT");
                out.put("httpStatus", sc.httpStatus());
                out.put("shortCircuitBody", sc.responseBody());
            }
        }
        return out;
    }

    private Map<String, Object> readPluginRow(ResultSet rs) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getString(1));
        row.put("workspaceId", rs.getString(2));
        row.put("name", rs.getString(3));
        row.put("type", rs.getString(4));
        row.put("version", rs.getInt(5));
        row.put("priority", rs.getInt(6));
        String stagesRaw = rs.getString(7);
        row.put("stages", (stagesRaw != null && !stagesRaw.isBlank())
            ? mapper.readValue(stagesRaw, new TypeReference<List<String>>() {})
            : List.of("PRE_ADMIT"));
        row.put("sourceCode", rs.getString(8));
        row.put("codeSha256", rs.getString(9));
        row.put("jarPath", rs.getString(10));
        row.put("className", rs.getString(11));
        String cfgRaw = rs.getString(12);
        row.put("config", (cfgRaw != null && !cfgRaw.isBlank())
            ? mapper.readValue(cfgRaw, new TypeReference<Map<String, Object>>() {})
            : Map.of());
        row.put("failMode", rs.getString(13));
        row.put("timeoutMs", rs.getInt(14));
        row.put("maxStatements", rs.getInt(15));
        row.put("enabled", rs.getBoolean(16));
        row.put("createdAt", rs.getTimestamp(17).toInstant().toString());
        row.put("updatedAt", rs.getTimestamp(18).toInstant().toString());
        return row;
    }
}
