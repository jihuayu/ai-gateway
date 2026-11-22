package com.jihuayu.aigateway.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.security.SecurityUtils;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.springframework.stereotype.Component;

@Component
public class ControlAuthService {
    public static final String SESSION_COOKIE = "gw_session";
    public static final String CSRF_COOKIE = "gw_csrf";
    public static final long SESSION_TTL_MS = 7L * 24 * 60 * 60 * 1000;
    public static final String REQ_AUTH_ATTR = "gw.session.info";
    private static final Pattern UUID_RE = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", Pattern.CASE_INSENSITIVE);

    public record SessionInfo(
        String userId,
        String workspaceId,
        String role,
        List<String> projectIds,
        String csrfHash
    ) {}

    public record ProjectRow(
        String id,
        String workspace_id,
        String name,
        String budget_micros,
        Integer rpm,
        Integer tpm,
        Integer concurrency,
        Integer max_output_tokens,
        Instant created_at,
        Instant updated_at
    ) {
        public ProjectRow(
            String id,
            String workspace_id,
            String name,
            String budget_micros,
            Integer rpm,
            Integer concurrency,
            Integer max_output_tokens,
            Instant created_at,
            Instant updated_at
        ) {
            this(id, workspace_id, name, budget_micros, rpm, null, concurrency, max_output_tokens, created_at, updated_at);
        }
    }

    public record ProjectAccess(SessionInfo auth, ProjectRow project) {}

    public record CreatedSession(String token, String csrf, Instant expiresAt) {}

    private final DataSource pool;
    private final ObjectMapper mapper;

    public ControlAuthService(DataSource pool, ObjectMapper mapper) {
        this.pool = pool;
        this.mapper = mapper;
    }

    public static boolean isUuid(String s) {
        return s != null && UUID_RE.matcher(s).matches();
    }

    public static Map<String, String> parseCookies(HttpServletRequest req) {
        Map<String, String> out = new LinkedHashMap<>();
        String raw = req.getHeader("Cookie");
        if (raw == null || raw.isBlank()) {
            if (req.getCookies() != null) {
                for (Cookie c : req.getCookies()) {
                    out.put(c.getName(), c.getValue());
                }
            }
            return out;
        }
        for (String part : raw.split(";")) {
            int i = part.indexOf('=');
            if (i < 0) continue;
            String k = part.substring(0, i).trim();
            String v = part.substring(i + 1).trim();
            if (!k.isEmpty()) {
                try {
                    out.put(k, URLDecoder.decode(v, StandardCharsets.UTF_8));
                } catch (Exception e) {
                    out.put(k, v);
                }
            }
        }
        return out;
    }

    public CreatedSession createSession(Connection conn, HttpServletResponse resp, String userId, String workspaceId) throws Exception {
        String token = SecurityUtils.randomBase64Url(32);
        String csrf = SecurityUtils.randomBase64Url(32);
        Instant expiresAt = Instant.now().plusMillis(SESSION_TTL_MS);
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO sessions (id, token_hash, csrf_hash, user_id, workspace_id, expires_at)
            VALUES (?::uuid, ?, ?, ?::uuid, ?::uuid, ?)
            """)) {
            ps.setString(1, UUID.randomUUID().toString());
            ps.setString(2, SecurityUtils.hashKey(token));
            ps.setString(3, SecurityUtils.hashKey(csrf));
            ps.setString(4, userId);
            ps.setString(5, workspaceId);
            ps.setTimestamp(6, Timestamp.from(expiresAt));
            ps.executeUpdate();
        }
        setSessionCookies(resp, token, csrf, (int) (SESSION_TTL_MS / 1000));
        return new CreatedSession(token, csrf, expiresAt);
    }

    public CreatedSession createSession(HttpServletResponse resp, String userId, String workspaceId) {
        try (Connection conn = pool.getConnection()) {
            return createSession(conn, resp, userId, workspaceId);
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create session");
        }
    }

    public static void setSessionCookies(HttpServletResponse resp, String token, String csrf, int maxAge) {
        String encToken = URLEncoder.encode(token, StandardCharsets.UTF_8);
        String encCsrf = URLEncoder.encode(csrf, StandardCharsets.UTF_8);
        resp.addHeader("Set-Cookie", SESSION_COOKIE + "=" + encToken + "; Path=/; HttpOnly; SameSite=Lax; Max-Age=" + maxAge);
        resp.addHeader("Set-Cookie", CSRF_COOKIE + "=" + encCsrf + "; Path=/; SameSite=Lax; Max-Age=" + maxAge);
    }

    public static void clearSessionCookies(HttpServletResponse resp) {
        resp.addHeader("Set-Cookie", SESSION_COOKIE + "=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0");
        resp.addHeader("Set-Cookie", CSRF_COOKIE + "=; Path=/; SameSite=Lax; Max-Age=0");
    }

    public SessionInfo authenticateSession(HttpServletRequest req) {
        Object cached = req.getAttribute(REQ_AUTH_ATTR);
        if (cached instanceof SessionInfo info) return info;

        String token = parseCookies(req).get(SESSION_COOKIE);
        if (token == null || token.isBlank()) {
            throw new GatewayException(401, "unauthenticated", "Session required");
        }
        try (Connection conn = pool.getConnection()) {
            String csrfHash;
            String userId;
            String workspaceId;
            Timestamp expiresAt;
            Timestamp revokedAt;
            String role;
            Timestamp disabledAt;

            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT s.csrf_hash, s.user_id::text, s.workspace_id::text, s.expires_at, s.revoked_at, u.role, u.disabled_at
                  FROM sessions s JOIN users u ON u.id = s.user_id
                 WHERE s.token_hash = ?
                """)) {
                ps.setString(1, SecurityUtils.hashKey(token));
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new GatewayException(401, "unauthenticated", "Session required");
                    }
                    csrfHash = rs.getString(1);
                    userId = rs.getString(2);
                    workspaceId = rs.getString(3);
                    expiresAt = rs.getTimestamp(4);
                    revokedAt = rs.getTimestamp(5);
                    role = rs.getString(6);
                    disabledAt = rs.getTimestamp(7);
                }
            }
            if (revokedAt != null || disabledAt != null || expiresAt == null || !expiresAt.toInstant().isAfter(Instant.now())) {
                throw new GatewayException(401, "unauthenticated", "Session required");
            }
            List<String> projectIds = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement("SELECT project_id::text FROM project_members WHERE user_id = ?::uuid")) {
                ps.setString(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        projectIds.add(rs.getString(1));
                    }
                }
            }
            SessionInfo info = new SessionInfo(userId, workspaceId, role, projectIds, csrfHash);
            req.setAttribute(REQ_AUTH_ATTR, info);
            return info;
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Authentication check failed");
        }
    }

    public void requireCsrf(HttpServletRequest req) {
        SessionInfo auth = authenticateSession(req);
        String token = req.getHeader("X-CSRF-Token");
        if (token == null || token.isBlank()) {
            throw new GatewayException(403, "csrf_failed", "CSRF token required");
        }
        String providedHash = SecurityUtils.hashKey(token);
        if (!SecurityUtils.constantTimeEquals(providedHash, auth.csrfHash())) {
            throw new GatewayException(403, "csrf_failed", "CSRF token invalid");
        }
    }

    public SessionInfo requireAdmin(HttpServletRequest req) {
        SessionInfo auth = authenticateSession(req);
        if (!"admin".equals(auth.role())) {
            throw new GatewayException(403, "forbidden", "Administrator role required");
        }
        return auth;
    }

    public SessionInfo requireWorkspaceRole(HttpServletRequest req, List<String> roles) {
        SessionInfo auth = authenticateSession(req);
        if (!roles.contains(auth.role())) {
            throw new GatewayException(403, "forbidden", "Insufficient role");
        }
        return auth;
    }

    public ProjectAccess requireProject(HttpServletRequest req, String projectId, boolean write) {
        if (!isUuid(projectId)) {
            throw new GatewayException(404, "project_not_found", "Project not found");
        }
        SessionInfo auth = authenticateSession(req);
        if (write && !List.of("admin", "developer").contains(auth.role())) {
            throw new GatewayException(403, "forbidden", "Write access required");
        }
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT p.id::text, p.workspace_id::text, p.name, p.budget_micros::text, p.rpm, p.tpm, p.concurrency, p.max_output_tokens, p.created_at, p.updated_at
                   FROM projects p
                  WHERE p.id = ?::uuid AND p.workspace_id = ?::uuid
                    AND (? = 'admin' OR EXISTS (SELECT 1 FROM project_members m WHERE m.project_id = p.id AND m.user_id = ?::uuid))
                 """)) {
            ps.setString(1, projectId);
            ps.setString(2, auth.workspaceId());
            ps.setString(3, auth.role());
            ps.setString(4, auth.userId());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new GatewayException(404, "project_not_found", "Project not found");
                }
                int rpm = rs.getInt(5);
                Integer rpmVal = rs.wasNull() ? null : rpm;
                int tpm = rs.getInt(6);
                Integer tpmVal = rs.wasNull() ? null : tpm;
                int conc = rs.getInt(7);
                Integer concVal = rs.wasNull() ? null : conc;
                int maxOut = rs.getInt(8);
                Integer maxOutVal = rs.wasNull() ? null : maxOut;
                Timestamp cAt = rs.getTimestamp(9);
                Timestamp uAt = rs.getTimestamp(10);
                ProjectRow row = new ProjectRow(
                    rs.getString(1),
                    rs.getString(2),
                    rs.getString(3),
                    rs.getString(4),
                    rpmVal,
                    tpmVal,
                    concVal,
                    maxOutVal,
                    cAt != null ? cAt.toInstant() : Instant.now(),
                    uAt != null ? uAt.toInstant() : Instant.now()
                );
                return new ProjectAccess(auth, row);
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Project access check failed");
        }
    }

    public Map<String, Object> sessionPayload(SessionInfo auth, String csrf) {
        try (Connection conn = pool.getConnection()) {
            Map<String, Object> userMap = new LinkedHashMap<>();
            try (PreparedStatement ps = conn.prepareStatement("SELECT id::text, email, role, workspace_id::text FROM users WHERE id = ?::uuid")) {
                ps.setString(1, auth.userId());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        userMap.put("id", rs.getString(1));
                        userMap.put("email", rs.getString(2));
                        userMap.put("role", rs.getString(3));
                        userMap.put("workspaceId", rs.getString(4));
                    }
                }
            }
            List<Map<String, Object>> projects = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT id::text, name
                  FROM projects
                 WHERE workspace_id = ?::uuid
                   AND (? = 'admin' OR id = ANY(?::uuid[]))
                 ORDER BY name
                """)) {
                ps.setString(1, auth.workspaceId());
                ps.setString(2, auth.role());
                ps.setArray(3, conn.createArrayOf("uuid", auth.projectIds().toArray()));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        projects.add(Map.of("id", rs.getString(1), "name", rs.getString(2)));
                    }
                }
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("user", userMap);
            out.put("projects", projects);
            out.put("csrf", csrf);
            return out;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to load session payload");
        }
    }

    public void audit(Connection conn, SessionInfo auth, String projectId, String action, String entityType, String entityId, Object changes) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO audit_events (id, workspace_id, project_id, actor_user_id, action, entity_type, entity_id, changes)
            VALUES (?::uuid, ?::uuid, ?::uuid, ?::uuid, ?, ?, ?, ?::jsonb)
            """)) {
            ps.setString(1, UUID.randomUUID().toString());
            ps.setString(2, auth.workspaceId());
            ps.setString(3, projectId);
            ps.setString(4, auth.userId());
            ps.setString(5, action);
            ps.setString(6, entityType);
            ps.setString(7, entityId);
            ps.setString(8, mapper.writeValueAsString(changes != null ? changes : Map.of()));
            ps.executeUpdate();
        }
    }

    public void audit(SessionInfo auth, String projectId, String action, String entityType, String entityId, Object changes) {
        try (Connection conn = pool.getConnection()) {
            audit(conn, auth, projectId, action, entityType, entityId, changes);
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Audit write failed");
        }
    }
}
