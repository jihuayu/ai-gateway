package com.jihuayu.aigateway.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.control.ControlAuthService.ProjectAccess;
import com.jihuayu.aigateway.control.ControlAuthService.SessionInfo;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotBilling;
import com.jihuayu.aigateway.hot.HotContract;
import com.jihuayu.aigateway.hot.HotContract.HotKeys;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.netty.DataPlaneExecutor;
import com.jihuayu.aigateway.netty.DataPlaneExecutor.HttpResponsePayload;
import com.jihuayu.aigateway.netty.DataPlaneExecutor.ResponseSender;
import com.jihuayu.aigateway.security.SecurityUtils;
import com.jihuayu.aigateway.worker.EventWorker;
import io.lettuce.core.api.StatefulRedisConnection;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QueriesController {
    private static final Base64.Encoder B64URL_ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64URL_DEC = Base64.getUrlDecoder();

    private final DataSource pool;
    private final ControlAuthService authService;
    private final StatefulRedisConnection<String, String> redisConn;
    private final HotKeys hotKeys;
    private final HotRuntime hotRuntime;
    private final HotBilling hotBilling;
    private final DataPlaneExecutor dataPlane;
    private final ObjectMapper mapper;

    public QueriesController(
        DataSource pool,
        ControlAuthService authService,
        StatefulRedisConnection<String, String> redisConn,
        HotKeys hotKeys,
        HotRuntime hotRuntime,
        HotBilling hotBilling,
        DataPlaneExecutor dataPlane,
        ObjectMapper mapper
    ) {
        this.pool = pool;
        this.authService = authService;
        this.redisConn = redisConn;
        this.hotKeys = hotKeys;
        this.hotRuntime = hotRuntime;
        this.hotBilling = hotBilling;
        this.dataPlane = dataPlane;
        this.mapper = mapper;
        EventWorker.setupEvents(pool);
    }

    private record TimeRange(Instant from, Instant to) {}

    private static TimeRange parseRange(String fromStr, String toStr) {
        Instant to;
        Instant from;
        try {
            to = (toStr != null && !toStr.isBlank()) ? Instant.parse(toStr) : Instant.now();
            from = (fromStr != null && !fromStr.isBlank()) ? Instant.parse(fromStr) : to.minusMillis(86_400_000L);
        } catch (Exception e) {
            throw new GatewayException(400, "invalid_request", "时间范围无效。");
        }
        long diffMs = to.toEpochMilli() - from.toEpochMilli();
        if (diffMs <= 0 || diffMs > 366L * 86_400_000L) {
            throw new GatewayException(400, "invalid_request", "时间范围无效。");
        }
        return new TimeRange(from, to);
    }

    private static String resolveProjectId(String p1, String p2) {
        String id = (p1 != null && !p1.isBlank()) ? p1 : p2;
        if (!ControlAuthService.isUuid(id)) {
            throw new GatewayException(400, "invalid_request", "请选择项目。");
        }
        return id;
    }

    @GetMapping("/api/requests")
    public Map<String, Object> listRequests(
        HttpServletRequest req,
        @RequestParam(value = "project_id", required = false) String p1,
        @RequestParam(value = "projectId", required = false) String p2,
        @RequestParam(value = "from", required = false) String fromStr,
        @RequestParam(value = "to", required = false) String toStr,
        @RequestParam(value = "limit", required = false) Integer limitParam,
        @RequestParam(value = "status", required = false) String status,
        @RequestParam(value = "model", required = false) String model,
        @RequestParam(value = "cursor", required = false) String cursor
    ) {
        String projectId = resolveProjectId(p1, p2);
        authService.requireProject(req, projectId, false);
        TimeRange range = parseRange(fromStr, toStr);
        int limit = limitParam != null ? Math.min(100, Math.max(1, limitParam)) : 50;

        StringBuilder sql = new StringBuilder("""
            SELECT request_id::text, project_id::text, key_id::text, period, reserve_micros,
                   route_version::text, price_snapshot::text, status, alias_name, error_code,
                   input_tokens, output_tokens, actual_micros, created_at, updated_at
              FROM requests
             WHERE project_id = ?::uuid AND created_at >= ? AND created_at < ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(projectId);
        params.add(Timestamp.from(range.from()));
        params.add(Timestamp.from(range.to()));

        if (status != null && !status.isBlank()) {
            sql.append(" AND status = ?");
            params.add(status);
        }
        if (model != null && !model.isBlank()) {
            sql.append(" AND alias_name = ?");
            params.add(model);
        }
        if (cursor != null && !cursor.isBlank()) {
            Instant curAt;
            String curId;
            try {
                byte[] decoded = B64URL_DEC.decode(cursor);
                List<?> curList = mapper.readValue(decoded, List.class);
                if (curList == null || curList.size() != 2) throw new IllegalArgumentException();
                curAt = Instant.parse(String.valueOf(curList.get(0)));
                curId = String.valueOf(curList.get(1));
                if (!ControlAuthService.isUuid(curId)) throw new IllegalArgumentException();
            } catch (Exception e) {
                throw new GatewayException(400, "invalid_request", "无效游标。");
            }
            sql.append(" AND (created_at, request_id) < (?, ?::uuid)");
            params.add(Timestamp.from(curAt));
            params.add(curId);
        }
        sql.append(" ORDER BY created_at DESC, request_id DESC LIMIT ?");
        params.add(limit + 1);

        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                Object p = params.get(i);
                if (p instanceof Timestamp ts) ps.setTimestamp(i + 1, ts);
                else if (p instanceof Integer n) ps.setInt(i + 1, n);
                else ps.setString(i + 1, String.valueOf(p));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(readRequestRow(rs));
                }
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to list requests");
        }

        List<Map<String, Object>> items = rows.size() > limit ? rows.subList(0, limit) : rows;
        String nextCursor = null;
        if (rows.size() > limit && !items.isEmpty()) {
            Map<String, Object> last = items.get(items.size() - 1);
            try {
                String cJson = mapper.writeValueAsString(List.of(last.get("created_at"), last.get("request_id")));
                nextCursor = B64URL_ENC.encodeToString(cJson.getBytes(StandardCharsets.UTF_8));
            } catch (Exception ignored) {}
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", items);
        out.put("next_cursor", nextCursor);
        return out;
    }

    @GetMapping("/api/requests/{id}")
    public Map<String, Object> getRequest(HttpServletRequest req, @PathVariable("id") String id) {
        if (!ControlAuthService.isUuid(id)) {
            throw new GatewayException(400, "invalid_request", "无效请求 ID。");
        }
        authService.authenticateSession(req);
        try (Connection conn = pool.getConnection()) {
            Map<String, Object> row;
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT request_id::text, project_id::text, key_id::text, period, reserve_micros,
                       route_version::text, price_snapshot::text, status, alias_name, error_code,
                       input_tokens, output_tokens, actual_micros, created_at, updated_at
                  FROM requests WHERE request_id = ?::uuid
                """)) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new GatewayException(404, "not_found", "请求不存在。");
                    }
                    row = readRequestRow(rs);
                }
            }
            authService.requireProject(req, String.valueOf(row.get("project_id")), false);
            List<Map<String, Object>> attempts = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT id::text, request_id::text, attempt_no, target_id::text, status, error_code,
                       started_at, finished_at, input_tokens, output_tokens, cost_micros
                  FROM attempts WHERE request_id = ?::uuid ORDER BY attempt_no
                """)) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> a = new LinkedHashMap<>();
                        a.put("id", rs.getString(1));
                        a.put("request_id", rs.getString(2));
                        a.put("attempt_no", rs.getInt(3));
                        a.put("target_id", rs.getString(4));
                        a.put("status", rs.getString(5));
                        a.put("error_code", rs.getString(6));
                        Timestamp sAt = rs.getTimestamp(7);
                        Timestamp fAt = rs.getTimestamp(8);
                        a.put("started_at", sAt != null ? sAt.toInstant().toString() : null);
                        a.put("finished_at", fAt != null ? fAt.toInstant().toString() : null);
                        long inTok = rs.getLong(9); a.put("input_tokens", rs.wasNull() ? null : inTok);
                        long outTok = rs.getLong(10); a.put("output_tokens", rs.wasNull() ? null : outTok);
                        long cost = rs.getLong(11); a.put("cost_micros", rs.wasNull() ? null : cost);
                        attempts.add(a);
                    }
                }
            }
            row.put("attempts", attempts);
            return row;
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to load request details");
        }
    }

    @GetMapping({"/api/usage", "/api/usage/summary"})
    public Map<String, Object> getUsage(
        HttpServletRequest req,
        @RequestParam(value = "project_id", required = false) String p1,
        @RequestParam(value = "projectId", required = false) String p2,
        @RequestParam(value = "from", required = false) String fromStr,
        @RequestParam(value = "to", required = false) String toStr
    ) {
        String projectId = resolveProjectId(p1, p2);
        authService.requireProject(req, projectId, false);
        TimeRange range = parseRange(fromStr, toStr);

        try (Connection conn = pool.getConnection()) {
            Map<String, Object> totals = new LinkedHashMap<>();
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT count(*)::int AS requests,
                       count(*) FILTER(WHERE lower(status) IN('completed','success','succeeded'))::int AS successful_requests,
                       count(*) FILTER(WHERE actual_micros IS NULL AND EXISTS(SELECT 1 FROM reservations rr WHERE rr.request_id = requests.request_id AND rr.status = 'unknown'))::int AS unknown_requests,
                       COALESCE(sum(input_tokens), 0)::text AS input_tokens,
                       COALESCE(sum(output_tokens), 0)::text AS output_tokens,
                       COALESCE(sum(actual_micros), 0)::text AS actual_micros,
                       COALESCE(sum(reserve_micros) FILTER(WHERE actual_micros IS NULL AND EXISTS(SELECT 1 FROM reservations rr WHERE rr.request_id = requests.request_id AND rr.status = 'unknown')), 0)::text AS estimated_micros
                  FROM requests
                 WHERE project_id = ?::uuid AND created_at >= ? AND created_at < ?
                """)) {
                ps.setString(1, projectId);
                ps.setTimestamp(2, Timestamp.from(range.from()));
                ps.setTimestamp(3, Timestamp.from(range.to()));
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    totals.put("requests", rs.getInt(1));
                    totals.put("successful_requests", rs.getInt(2));
                    totals.put("unknown_requests", rs.getInt(3));
                    totals.put("input_tokens", rs.getString(4));
                    totals.put("output_tokens", rs.getString(5));
                    totals.put("actual_micros", rs.getString(6));
                    totals.put("estimated_micros", rs.getString(7));
                }
            }

            List<Map<String, Object>> series = new ArrayList<>();
            String updatedAt = null;
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT project_id::text, bucket, actual_micros, estimated_micros, input_tokens, output_tokens, event_count, updated_at
                  FROM usage_hourly
                 WHERE project_id = ?::uuid AND bucket >= ? AND bucket < ?
                 ORDER BY bucket
                """)) {
                ps.setString(1, projectId);
                ps.setTimestamp(2, Timestamp.from(range.from()));
                ps.setTimestamp(3, Timestamp.from(range.to()));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> s = new LinkedHashMap<>();
                        s.put("project_id", rs.getString(1));
                        s.put("bucket", rs.getTimestamp(2).toInstant().toString());
                        s.put("actual_micros", String.valueOf(rs.getLong(3)));
                        s.put("estimated_micros", String.valueOf(rs.getLong(4)));
                        s.put("input_tokens", String.valueOf(rs.getLong(5)));
                        s.put("output_tokens", String.valueOf(rs.getLong(6)));
                        s.put("event_count", String.valueOf(rs.getLong(7)));
                        String uAt = rs.getTimestamp(8).toInstant().toString();
                        s.put("updated_at", uAt);
                        updatedAt = uAt;
                        series.add(s);
                    }
                }
            }

            Map<String, Object> project = new LinkedHashMap<>();
            String limitMicros = null;
            try (PreparedStatement ps = conn.prepareStatement("SELECT id::text, name, budget_micros::text FROM projects WHERE id = ?::uuid")) {
                ps.setString(1, projectId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        project.put("id", rs.getString(1));
                        project.put("name", rs.getString(2));
                        limitMicros = rs.getString(3);
                        project.put("budget_micros", limitMicros);
                    }
                }
            }

            String outstandingReserved = "0";
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT COALESCE(sum(reserved_micros), 0)::text
                  FROM reservations
                 WHERE project_id = ?::uuid AND actual_micros IS NULL
                   AND status NOT IN('CANCELLED','cancelled','REJECTED','rejected')
                """)) {
                ps.setString(1, projectId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) outstandingReserved = rs.getString(1);
                }
            }

            Timestamp oldestBacklog = null;
            try (PreparedStatement ps = conn.prepareStatement("SELECT min(created_at) FROM outbox WHERE project_id = ?::uuid AND status != 'published'")) {
                ps.setString(1, projectId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) oldestBacklog = rs.getTimestamp(1);
                }
            }

            ZonedDateTime nowUtc = ZonedDateTime.now(ZoneOffset.UTC);
            String period = String.format("%04d-%02d", nowUtc.getYear(), nowUtc.getMonthValue());
            String rawProjectJson = redisConn.sync().get(hotKeys.project(projectId));
            if (rawProjectJson == null || rawProjectJson.isBlank()) {
                throw new GatewayException(503, "service_unavailable", "实时预算暂不可用。");
            }
            Map<?, ?> hotState = mapper.readValue(rawProjectJson, Map.class);
            Object budgetsObj = hotState.get("budgets");
            if (!(budgetsObj instanceof Map<?, ?> budgetsMap)) {
                throw new GatewayException(503, "service_unavailable", "实时预算暂不可用。");
            }
            Object curObj = budgetsMap.get(period);
            long spent = 0L;
            long reserved = 0L;
            if (curObj instanceof Map<?, ?> curMap) {
                Object sObj = curMap.get("spent");
                Object rObj = curMap.get("reserved");
                if (!(sObj instanceof Number sNum) || !(rObj instanceof Number rNum)) {
                    throw new GatewayException(503, "service_unavailable", "实时预算暂不可用。");
                }
                spent = sNum.longValue();
                reserved = rNum.longValue();
                if (spent < 0 || spent > HotContract.MAX_SAFE_INTEGER || reserved < 0 || reserved > HotContract.MAX_SAFE_INTEGER) {
                    throw new GatewayException(503, "service_unavailable", "实时预算暂不可用。");
                }
            }
            Long xlen = redisConn.sync().xlen(hotKeys.journal());
            long pendingEvents = xlen != null ? xlen : 0L;

            Map<String, Object> budget = new LinkedHashMap<>();
            budget.put("period", period);
            budget.put("spent_micros", String.valueOf(spent));
            budget.put("reserved_micros", String.valueOf(reserved));
            budget.put("limit_micros", limitMicros);

            boolean stale = pendingEvents > 0 || (oldestBacklog != null && (System.currentTimeMillis() - oldestBacklog.getTime() > 60_000L));

            Map<String, Object> out = new LinkedHashMap<>(totals);
            out.put("reserved_micros", outstandingReserved);
            out.put("budget", budget);
            out.put("summary", totals);
            out.put("totals", totals);
            out.put("project", project);
            out.put("series", series);
            out.put("items", series);
            out.put("updated_at", updatedAt);
            out.put("ingestion_pending_events", pendingEvents);
            out.put("stale", stale);
            out.put("from", range.from().toString());
            out.put("to", range.to().toString());
            return out;
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to load usage");
        }
    }

    @GetMapping("/api/audit")
    public Map<String, Object> getAudit(HttpServletRequest req) {
        SessionInfo auth = authService.authenticateSession(req);
        if (!"admin".equals(auth.role())) {
            throw new GatewayException(403, "forbidden", "仅管理员可查看审计记录。");
        }
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT id::text, workspace_id::text, project_id::text, actor_user_id::text, action, entity_type, entity_id, changes::text, created_at
                   FROM audit_events
                  WHERE workspace_id = ?::uuid
                  ORDER BY created_at DESC
                  LIMIT 100
                 """)) {
            ps.setString(1, auth.workspaceId());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id", rs.getString(1));
                    row.put("workspace_id", rs.getString(2));
                    row.put("project_id", rs.getString(3));
                    row.put("actor_user_id", rs.getString(4));
                    row.put("action", rs.getString(5));
                    row.put("entity_type", rs.getString(6));
                    row.put("entity_id", rs.getString(7));
                    String chJson = rs.getString(8);
                    row.put("changes", chJson != null ? mapper.readValue(chJson, Object.class) : Map.of());
                    Timestamp cAt = rs.getTimestamp(9);
                    row.put("created_at", cAt != null ? cAt.toInstant().toString() : null);
                    items.add(row);
                }
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to load audit events");
        }
        return Map.of("items", items);
    }

    @PostMapping("/api/test")
    public ResponseEntity<Object> testGatewayCall(HttpServletRequest req, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.authenticateSession(req);
        if (body == null) throw new GatewayException(400, "invalid_request", "请求体无效。");
        String projectId = resolveProjectId(
            body.get("project_id") instanceof String s1 ? s1 : null,
            body.get("projectId") instanceof String s2 ? s2 : null
        );
        authService.requireProject(req, projectId, true);
        if (!(body.get("model") instanceof String model) || model.isBlank()) {
            throw new GatewayException(400, "invalid_request", "请选择模型。");
        }
        String secret = "gw_test_" + SecurityUtils.randomBase64Url(32);
        String keyId = UUID.randomUUID().toString();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 INSERT INTO api_keys(id, project_id, name, key_hash, prefix, created_by, allowed_models, expires_at)
                 VALUES(?::uuid, ?::uuid, 'Console test', ?, 'gw_test', ?::uuid, ?::jsonb, now() + interval '1 minute')
                 """)) {
            ps.setString(1, keyId);
            ps.setString(2, projectId);
            ps.setString(3, SecurityUtils.hashKey(secret));
            ps.setString(4, auth.userId());
            ps.setString(5, mapper.writeValueAsString(List.of(model)));
            ps.executeUpdate();
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create temporary test key");
        }

        try {
            hotRuntime.publish();
            Map<String, Object> chatPayload = new LinkedHashMap<>();
            chatPayload.put("model", model);
            Object msgs = body.get("messages");
            if (msgs instanceof List<?> l && !l.isEmpty()) {
                chatPayload.put("messages", l);
            } else {
                String prompt = body.get("prompt") instanceof String p && !p.isBlank() ? p : "你好";
                chatPayload.put("messages", List.of(Map.of("role", "user", "content", prompt)));
            }
            Object maxTokObj = body.get("max_tokens") != null ? body.get("max_tokens") : body.get("maxTokens");
            int maxTok = maxTokObj instanceof Number n ? n.intValue() : 64;
            chatPayload.put("max_tokens", maxTok);
            chatPayload.put("stream", false);

            String requestId = UUID.randomUUID().toString();
            AtomicInteger statusRef = new AtomicInteger(200);
            AtomicReference<String> jsonRef = new AtomicReference<>("{}");
            AtomicReference<Integer> retryRef = new AtomicReference<>();

            ResponseSender captureSender = new ResponseSender() {
                @Override public boolean isClientClosed() { return false; }
                @Override public void sendJson(int status, String reqId, String jsonBody, Integer retryAfterSec) {
                    statusRef.set(status);
                    jsonRef.set(jsonBody);
                    retryRef.set(retryAfterSec);
                }
                @Override public void startSse(String reqId) {}
                @Override public boolean isSseStarted() { return false; }
                @Override public void writeSseFrame(String frame, long deadlineAt) {}
                @Override public void finishSse() {}
            };

            dataPlane.handleChat("Bearer " + secret, mapper.writeValueAsBytes(chatPayload), requestId, captureSender);

            Map<String, Object> parsed;
            try {
                parsed = new LinkedHashMap<>(mapper.readValue(jsonRef.get(), Map.class));
                parsed.put("request_id", requestId);
            } catch (Exception e) {
                parsed = Map.of("error", Map.of("message", "上游返回格式错误"), "request_id", requestId);
            }
            ResponseEntity.BodyBuilder builder = ResponseEntity.status(statusRef.get())
                .contentType(MediaType.APPLICATION_JSON)
                .header("x-request-id", requestId);
            if (retryRef.get() != null) {
                builder.header("retry-after", String.valueOf(retryRef.get()));
            }
            return builder.body(parsed);
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Test call failed");
        } finally {
            try (Connection conn = pool.getConnection();
                 PreparedStatement ps = conn.prepareStatement("UPDATE api_keys SET revoked_at = now() WHERE id = ?::uuid")) {
                ps.setString(1, keyId);
                ps.executeUpdate();
            } catch (Exception ignored) {}
            try { hotRuntime.publish(); } catch (Exception ignored) {}
        }
    }

    @GetMapping("/health/live")
    public Map<String, Object> healthLive() {
        return Map.of("status", "ok");
    }

    @GetMapping("/health/ready")
    public ResponseEntity<Map<String, Object>> healthReady() {
        if (hotBilling.ready()) {
            return ResponseEntity.ok(Map.of("status", "ready"));
        }
        return ResponseEntity.status(503).body(Map.of("status", "unavailable"));
    }

    @GetMapping("/v1/models")
    public ResponseEntity<String> v1Models(HttpServletRequest req) {
        String requestId = UUID.randomUUID().toString();
        HttpResponsePayload payload = dataPlane.handleModels(req.getHeader("Authorization"), requestId);
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(payload.status())
            .header("Content-Type", "application/json; charset=utf-8")
            .header("x-request-id", requestId);
        if (payload.headers().containsKey("retry-after")) {
            builder.header("retry-after", payload.headers().get("retry-after"));
        }
        return builder.body(payload.jsonBody());
    }

    @PostMapping("/v1/chat/completions")
    public void v1ChatCompletions(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String requestId = UUID.randomUUID().toString();
        byte[] rawBody = req.getInputStream().readNBytes(DataPlaneExecutor.MAX_BODY_BYTES + 1);
        String authHeader = req.getHeader("Authorization");

        AtomicBoolean sseStarted = new AtomicBoolean(false);
        AtomicBoolean closed = new AtomicBoolean(false);

        ResponseSender servletSender = new ResponseSender() {
            @Override
            public boolean isClientClosed() {
                return closed.get();
            }

            @Override
            public void sendJson(int status, String reqId, String jsonBody, Integer retryAfterSec) {
                if (resp.isCommitted()) return;
                resp.setStatus(status);
                resp.setContentType("application/json; charset=utf-8");
                resp.setHeader("x-request-id", reqId);
                if (retryAfterSec != null && retryAfterSec > 0) {
                    resp.setHeader("retry-after", String.valueOf(retryAfterSec));
                }
                try {
                    byte[] bytes = jsonBody.getBytes(StandardCharsets.UTF_8);
                    resp.setContentLength(bytes.length);
                    OutputStream os = resp.getOutputStream();
                    os.write(bytes);
                    os.flush();
                } catch (IOException e) {
                    closed.set(true);
                }
            }

            @Override
            public void startSse(String reqId) {
                if (sseStarted.compareAndSet(false, true)) {
                    resp.setStatus(200);
                    resp.setHeader("Content-Type", "text/event-stream; charset=utf-8");
                    resp.setHeader("Cache-Control", "no-store");
                    resp.setHeader("Connection", "keep-alive");
                    resp.setHeader("X-Accel-Buffering", "no");
                    resp.setHeader("x-request-id", reqId);
                    try {
                        resp.flushBuffer();
                    } catch (IOException e) {
                        closed.set(true);
                    }
                }
            }

            @Override
            public boolean isSseStarted() {
                return sseStarted.get();
            }

            @Override
            public void writeSseFrame(String frame, long deadlineAt) {
                if (closed.get()) {
                    throw new GatewayException(499, "client_closed", "客户端已断开连接。");
                }
                try {
                    OutputStream os = resp.getOutputStream();
                    os.write(frame.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                } catch (IOException e) {
                    closed.set(true);
                    throw new GatewayException(499, "client_closed", "客户端已断开连接。");
                }
            }

            @Override
            public void finishSse() {
                try {
                    resp.getOutputStream().flush();
                } catch (IOException e) {
                    closed.set(true);
                }
            }
        };

        dataPlane.handleChat(authHeader, rawBody, requestId, servletSender);
    }

    private Map<String, Object> readRequestRow(ResultSet rs) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("request_id", rs.getString(1));
        row.put("project_id", rs.getString(2));
        row.put("key_id", rs.getString(3));
        row.put("period", rs.getString(4));
        row.put("reserve_micros", rs.getLong(5));
        String rvJson = rs.getString(6);
        row.put("route_version", rvJson != null ? mapper.readValue(rvJson, Object.class) : null);
        String psJson = rs.getString(7);
        row.put("price_snapshot", psJson != null ? mapper.readValue(psJson, Object.class) : Map.of());
        row.put("status", rs.getString(8));
        row.put("alias_name", rs.getString(9));
        row.put("error_code", rs.getString(10));
        long inTok = rs.getLong(11); row.put("input_tokens", rs.wasNull() ? null : inTok);
        long outTok = rs.getLong(12); row.put("output_tokens", rs.wasNull() ? null : outTok);
        long act = rs.getLong(13); row.put("actual_micros", rs.wasNull() ? null : act);
        Timestamp cAt = rs.getTimestamp(14);
        Timestamp uAt = rs.getTimestamp(15);
        row.put("created_at", cAt != null ? cAt.toInstant().toString() : null);
        row.put("updated_at", uAt != null ? uAt.toInstant().toString() : null);
        return row;
    }
}
