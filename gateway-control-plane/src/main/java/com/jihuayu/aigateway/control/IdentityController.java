package com.jihuayu.aigateway.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.control.ControlAuthService.CreatedSession;
import com.jihuayu.aigateway.control.ControlAuthService.ProjectAccess;
import com.jihuayu.aigateway.control.ControlAuthService.SessionInfo;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotContract;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.security.SecurityUtils;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IdentityController {
    private static final Pattern EMAIL_RE = Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
    private static final Set<String> VALID_ROLES = Set.of("admin", "developer", "viewer");

    private final DataSource pool;
    private final ControlAuthService authService;
    private final HotRuntime hotRuntime;
    private final ObjectMapper mapper;

    public IdentityController(DataSource pool, ControlAuthService authService, HotRuntime hotRuntime, ObjectMapper mapper) {
        this.pool = pool;
        this.authService = authService;
        this.hotRuntime = hotRuntime;
        this.mapper = mapper;
    }

    @PostMapping("/api/bootstrap")
    public Map<String, Object> bootstrap(@RequestBody(required = false) Map<String, Object> body, HttpServletResponse resp) {
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        String email = requireEmail(body.get("email"));
        String password = requirePassword(body.get("password"));
        String projectName = requireNonBlank(body.get("projectName"), "projectName", 100);
        String passwordHash = SecurityUtils.hashPassword(password);

        Map<String, Object> response;
        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement lockPs = conn.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
                    lockPs.setLong(1, 0x626f6f74L);
                    lockPs.execute();
                }
                try (PreparedStatement countPs = conn.prepareStatement("SELECT count(*)::int FROM users");
                     ResultSet rs = countPs.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) {
                        throw new GatewayException(409, "bootstrap_closed", "Bootstrap is only available on an empty installation");
                    }
                }
                String workspaceId = UUID.randomUUID().toString();
                String userId = UUID.randomUUID().toString();
                String projectId = UUID.randomUUID().toString();

                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO workspaces (id, name) VALUES (?::uuid, ?)")) {
                    ps.setString(1, workspaceId);
                    ps.setString(2, "Default workspace");
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO users (id, workspace_id, email, password_hash, role) VALUES (?::uuid, ?::uuid, ?, ?, 'admin')")) {
                    ps.setString(1, userId);
                    ps.setString(2, workspaceId);
                    ps.setString(3, email);
                    ps.setString(4, passwordHash);
                    ps.executeUpdate();
                }
                Map<String, Object> projMap = new LinkedHashMap<>();
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO projects (id, workspace_id, name)
                    VALUES (?::uuid, ?::uuid, ?)
                    RETURNING id::text, name, budget_micros::text, rpm, tpm, concurrency, max_output_tokens
                    """)) {
                    ps.setString(1, projectId);
                    ps.setString(2, workspaceId);
                    ps.setString(3, projectName);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            projMap.put("id", rs.getString(1));
                            projMap.put("name", rs.getString(2));
                            projMap.put("budget_micros", rs.getString(3));
                            int rpm = rs.getInt(4); projMap.put("rpm", rs.wasNull() ? null : rpm);
                            int tpm = rs.getInt(5); projMap.put("tpm", rs.wasNull() ? null : tpm);
                            int conc = rs.getInt(6); projMap.put("concurrency", rs.wasNull() ? null : conc);
                            int maxOut = rs.getInt(7); projMap.put("max_output_tokens", rs.wasNull() ? null : maxOut);
                        }
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO project_members (project_id, user_id) VALUES (?::uuid, ?::uuid)")) {
                    ps.setString(1, projectId);
                    ps.setString(2, userId);
                    ps.executeUpdate();
                }
                SessionInfo actor = new SessionInfo(userId, workspaceId, "admin", List.of(projectId), "");
                authService.audit(conn, actor, projectId, "bootstrap", "workspace", workspaceId, Map.of("projectName", projectName));
                CreatedSession session = authService.createSession(conn, resp, userId, workspaceId);
                conn.commit();

                Map<String, Object> sessionObj = new LinkedHashMap<>();
                sessionObj.put("user", Map.of("id", userId, "email", email, "role", "admin", "workspaceId", workspaceId));
                sessionObj.put("projects", List.of(Map.of("id", projectId, "name", projectName)));
                sessionObj.put("csrf", session.csrf());

                response = new LinkedHashMap<>();
                response.put("session", sessionObj);
                response.put("csrf", session.csrf());
                response.put("project", projMap);
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Bootstrap failed");
        }
        hotRuntime.publish();
        return response;
    }

    @PostMapping("/api/login")
    public Map<String, Object> login(@RequestBody(required = false) Map<String, Object> body, HttpServletResponse resp) {
        if (body == null) throw new GatewayException(400, "validation_error", "Credentials required");
        String email = requireEmail(body.get("email"));
        String password = requirePassword(body.get("password"));

        String userId;
        String workspaceId;
        String passwordHash;
        String role;
        Timestamp disabledAt;

        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT id::text, workspace_id::text, password_hash, role, disabled_at FROM users WHERE lower(email) = lower(?)")) {
            ps.setString(1, email);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new GatewayException(401, "invalid_credentials", "Email or password is incorrect");
                }
                userId = rs.getString(1);
                workspaceId = rs.getString(2);
                passwordHash = rs.getString(3);
                role = rs.getString(4);
                disabledAt = rs.getTimestamp(5);
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Login query failed");
        }

        if (disabledAt != null || !SecurityUtils.verifyPassword(password, passwordHash)) {
            throw new GatewayException(401, "invalid_credentials", "Email or password is incorrect");
        }

        CreatedSession session = authService.createSession(resp, userId, workspaceId);
        List<String> projectIds = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT project_id::text FROM project_members WHERE user_id = ?::uuid")) {
            ps.setString(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) projectIds.add(rs.getString(1));
            }
        } catch (Exception ignored) {}

        SessionInfo auth = new SessionInfo(userId, workspaceId, role, projectIds, SecurityUtils.hashKey(session.csrf()));
        Map<String, Object> payload = authService.sessionPayload(auth, session.csrf());
        hotRuntime.publish();
        return Map.of("session", payload, "csrf", session.csrf());
    }

    @PostMapping("/api/logout")
    public Map<String, Object> logout(HttpServletRequest req, HttpServletResponse resp) {
        String token = ControlAuthService.parseCookies(req).get(ControlAuthService.SESSION_COOKIE);
        if (token != null && !token.isBlank()) {
            try (Connection conn = pool.getConnection();
                 PreparedStatement ps = conn.prepareStatement("UPDATE sessions SET revoked_at = now() WHERE token_hash = ? AND revoked_at IS NULL")) {
                ps.setString(1, SecurityUtils.hashKey(token));
                ps.executeUpdate();
            } catch (Exception ignored) {}
        }
        ControlAuthService.clearSessionCookies(resp);
        hotRuntime.publish();
        return Map.of("ok", true);
    }

    @GetMapping("/api/session")
    public Map<String, Object> getSession(HttpServletRequest req) {
        SessionInfo auth = authService.authenticateSession(req);
        String csrf = ControlAuthService.parseCookies(req).get(ControlAuthService.CSRF_COOKIE);
        if (csrf == null || csrf.isBlank()) {
            throw new GatewayException(401, "unauthenticated", "Session required");
        }
        return authService.sessionPayload(auth, csrf);
    }

    @GetMapping("/api/projects")
    public Map<String, Object> listProjects(HttpServletRequest req) {
        SessionInfo auth = authService.authenticateSession(req);
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT id::text, workspace_id::text, name, budget_micros::text, rpm, tpm, concurrency, max_output_tokens, created_at, updated_at
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
                    items.add(readProjectRow(rs));
                }
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to list projects");
        }
        return Map.of("items", items);
    }

    @PostMapping("/api/projects")
    public Map<String, Object> createProject(HttpServletRequest req, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.requireAdmin(req);
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        String name = requireNonBlank(body.get("name"), "name", 100);
        Long budgetMicros = parseOptionalLong(body, "budget_micros", "budgetMicros", 0L, HotContract.MAX_SAFE_INTEGER);
        Integer rpm = parseOptionalInt(body, "rpm", null, 1, 1_000_000);
        Integer tpm = parseOptionalInt(body, "tpm", null, 1, 1_000_000_000);
        Integer concurrency = parseOptionalInt(body, "concurrency", null, 1, 10_000);
        Integer maxOutput = parseOptionalInt(body, "max_output_tokens", "maxOutputTokens", 1, 1_000_000);
        String id = UUID.randomUUID().toString();

        Map<String, Object> created;
        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO projects (id, workspace_id, name, budget_micros, rpm, tpm, concurrency, max_output_tokens)
                    VALUES (?::uuid, ?::uuid, ?, ?, ?, ?, ?, ?)
                    RETURNING id::text, workspace_id::text, name, budget_micros::text, rpm, tpm, concurrency, max_output_tokens, created_at, updated_at
                    """)) {
                    ps.setString(1, id);
                    ps.setString(2, auth.workspaceId());
                    ps.setString(3, name);
                    setNullableLong(ps, 4, budgetMicros);
                    setNullableInt(ps, 5, rpm);
                    setNullableInt(ps, 6, tpm);
                    setNullableInt(ps, 7, concurrency);
                    setNullableInt(ps, 8, maxOutput);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        created = readProjectRow(rs);
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO project_members (project_id, user_id) VALUES (?::uuid, ?::uuid) ON CONFLICT DO NOTHING")) {
                    ps.setString(1, id);
                    ps.setString(2, auth.userId());
                    ps.executeUpdate();
                }
                Map<String, Object> auditChanges = new LinkedHashMap<>();
                auditChanges.put("name", name);
                auditChanges.put("budget_micros", budgetMicros);
                auditChanges.put("rpm", rpm);
                auditChanges.put("tpm", tpm);
                auditChanges.put("concurrency", concurrency);
                auditChanges.put("max_output_tokens", maxOutput);
                authService.audit(conn, auth, id, "project.create", "project", id, auditChanges);
                conn.commit();
            } catch (SQLException sqle) {
                conn.rollback();
                if ("23505".equals(sqle.getSQLState())) {
                    throw new GatewayException(409, "project_exists", "Project name already exists");
                }
                throw sqle;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create project");
        }
        hotRuntime.publish();
        return created;
    }

    @GetMapping("/api/projects/{id}")
    public Map<String, Object> getProject(HttpServletRequest req, @PathVariable("id") String id) {
        ProjectAccess access = authService.requireProject(req, id, false);
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", access.project().id());
        map.put("workspace_id", access.project().workspace_id());
        map.put("name", access.project().name());
        map.put("budget_micros", access.project().budget_micros());
        map.put("rpm", access.project().rpm());
        map.put("tpm", access.project().tpm());
        map.put("concurrency", access.project().concurrency());
        map.put("max_output_tokens", access.project().max_output_tokens());
        map.put("created_at", access.project().created_at().toString());
        map.put("updated_at", access.project().updated_at().toString());
        return map;
    }

    @PatchMapping("/api/projects/{id}")
    public Map<String, Object> updateProject(HttpServletRequest req, @PathVariable("id") String id, @RequestBody(required = false) Map<String, Object> body) {
        ProjectAccess access = authService.requireProject(req, id, true);
        if (!"admin".equals(access.auth().role())) {
            throw new GatewayException(403, "forbidden", "Only administrators may change project settings");
        }
        if (body == null || body.isEmpty()) {
            throw new GatewayException(400, "validation_error", "No fields to update");
        }
        List<String> sets = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        Map<String, Object> changes = new LinkedHashMap<>();

        if (body.containsKey("name")) {
            String name = requireNonBlank(body.get("name"), "name", 100);
            sets.add("name = ?");
            params.add(name);
            changes.put("name", name);
        }
        if (body.containsKey("budget_micros") || body.containsKey("budgetMicros")) {
            Long b = parseOptionalLong(body, "budget_micros", "budgetMicros", 0L, HotContract.MAX_SAFE_INTEGER);
            sets.add("budget_micros = ?");
            params.add(b);
            changes.put("budget_micros", b);
        }
        if (body.containsKey("rpm")) {
            Integer rpm = parseOptionalInt(body, "rpm", null, 1, 1_000_000);
            sets.add("rpm = ?");
            params.add(rpm);
            changes.put("rpm", rpm);
        }
        if (body.containsKey("tpm")) {
            Integer tpm = parseOptionalInt(body, "tpm", null, 1, 1_000_000_000);
            sets.add("tpm = ?");
            params.add(tpm);
            changes.put("tpm", tpm);
        }
        if (body.containsKey("concurrency")) {
            Integer conc = parseOptionalInt(body, "concurrency", null, 1, 10_000);
            sets.add("concurrency = ?");
            params.add(conc);
            changes.put("concurrency", conc);
        }
        if (body.containsKey("max_output_tokens") || body.containsKey("maxOutputTokens")) {
            Integer maxOut = parseOptionalInt(body, "max_output_tokens", "maxOutputTokens", 1, 1_000_000);
            sets.add("max_output_tokens = ?");
            params.add(maxOut);
            changes.put("max_output_tokens", maxOut);
        }
        if (sets.isEmpty()) {
            throw new GatewayException(400, "validation_error", "No fields to update");
        }

        Map<String, Object> updated;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE projects SET " + String.join(", ", sets) + ", updated_at = now() WHERE id = ?::uuid " +
                 "RETURNING id::text, workspace_id::text, name, budget_micros::text, rpm, tpm, concurrency, max_output_tokens, created_at, updated_at")) {
            for (int i = 0; i < params.size(); i++) {
                Object val = params.get(i);
                if (val == null) ps.setNull(i + 1, Types.NULL);
                else if (val instanceof Long l) ps.setLong(i + 1, l);
                else if (val instanceof Integer n) ps.setInt(i + 1, n);
                else ps.setString(i + 1, String.valueOf(val));
            }
            ps.setString(params.size() + 1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                updated = readProjectRow(rs);
            }
            authService.audit(conn, access.auth(), id, "project.update", "project", id, changes);
        } catch (SQLException sqle) {
            if ("23505".equals(sqle.getSQLState())) {
                throw new GatewayException(409, "project_exists", "Project name already exists");
            }
            throw new GatewayException(500, "internal_error", "Failed to update project");
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to update project");
        }
        hotRuntime.publish();
        return updated;
    }

    @GetMapping("/api/members")
    public Map<String, Object> listMembers(HttpServletRequest req) {
        SessionInfo auth = authService.requireAdmin(req);
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT u.id::text, u.email, u.role, u.disabled_at, u.created_at,
                        COALESCE(json_agg(p.id::text) FILTER (WHERE p.id IS NOT NULL), '[]'::json)::text AS project_ids
                   FROM users u
                   LEFT JOIN project_members pm ON pm.user_id = u.id
                   LEFT JOIN projects p ON p.id = pm.project_id AND p.workspace_id = u.workspace_id
                  WHERE u.workspace_id = ?::uuid
                  GROUP BY u.id
                  ORDER BY u.created_at, u.email
                 """)) {
            ps.setString(1, auth.workspaceId());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(readMemberRow(rs));
                }
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to list members");
        }
        return Map.of("items", items);
    }

    @PatchMapping("/api/members/{id}")
    public Map<String, Object> updateMember(HttpServletRequest req, @PathVariable("id") String userId, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.requireAdmin(req);
        if (!ControlAuthService.isUuid(userId)) {
            throw new GatewayException(404, "not_found", "Resource not found");
        }
        if (body == null || body.isEmpty()) {
            throw new GatewayException(400, "validation_error", "No fields to update");
        }
        String newRoleParam = null;
        if (body.containsKey("role") && body.get("role") != null) {
            newRoleParam = String.valueOf(body.get("role"));
            if (!VALID_ROLES.contains(newRoleParam)) {
                throw new GatewayException(400, "validation_error", "Invalid role");
            }
        }
        Boolean disabledParam = null;
        if (body.containsKey("disabled") && body.get("disabled") != null) {
            if (!(body.get("disabled") instanceof Boolean b)) {
                throw new GatewayException(400, "validation_error", "disabled must be boolean");
            }
            disabledParam = b;
        }
        if (userId.equals(auth.userId()) && Boolean.TRUE.equals(disabledParam)) {
            throw new GatewayException(400, "self_disable", "Administrators cannot disable their own account");
        }
        List<String> projectIdsParam = null;
        if (body.containsKey("project_ids") && body.get("project_ids") != null) {
            if (!(body.get("project_ids") instanceof List<?> list)) {
                throw new GatewayException(400, "validation_error", "project_ids must be an array");
            }
            Set<String> unique = new LinkedHashSet<>();
            for (Object o : list) {
                String pid = String.valueOf(o);
                if (!ControlAuthService.isUuid(pid)) {
                    throw new GatewayException(400, "validation_error", "Invalid project UUID");
                }
                unique.add(pid);
            }
            projectIdsParam = new ArrayList<>(unique);
        }

        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                List<String> activeAdmins = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement("SELECT id::text FROM users WHERE workspace_id = ?::uuid AND role = 'admin' AND disabled_at IS NULL FOR UPDATE")) {
                    ps.setString(1, auth.workspaceId());
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) activeAdmins.add(rs.getString(1));
                    }
                }
                String curRole;
                boolean curDisabled;
                try (PreparedStatement ps = conn.prepareStatement("SELECT role, disabled_at FROM users WHERE id = ?::uuid AND workspace_id = ?::uuid FOR UPDATE")) {
                    ps.setString(1, userId);
                    ps.setString(2, auth.workspaceId());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            throw new GatewayException(404, "member_not_found", "Workspace member not found");
                        }
                        curRole = rs.getString(1);
                        curDisabled = rs.getTimestamp(2) != null;
                    }
                }
                String effRole = newRoleParam != null ? newRoleParam : curRole;
                boolean effDisabled = disabledParam != null ? disabledParam : curDisabled;
                if ("admin".equals(curRole) && !curDisabled && (!"admin".equals(effRole) || effDisabled)) {
                    long otherAdmins = activeAdmins.stream().filter(id -> !id.equals(userId)).count();
                    if (otherAdmins == 0) {
                        throw new GatewayException(409, "last_admin", "At least one active administrator must remain");
                    }
                }
                if (newRoleParam != null) {
                    try (PreparedStatement ps = conn.prepareStatement("UPDATE users SET role = ? WHERE id = ?::uuid")) {
                        ps.setString(1, newRoleParam);
                        ps.setString(2, userId);
                        ps.executeUpdate();
                    }
                }
                if (disabledParam != null) {
                    try (PreparedStatement ps = conn.prepareStatement("UPDATE users SET disabled_at = ? WHERE id = ?::uuid")) {
                        if (disabledParam) ps.setTimestamp(1, Timestamp.from(Instant.now()));
                        else ps.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                        ps.setString(2, userId);
                        ps.executeUpdate();
                    }
                }
                if (projectIdsParam != null) {
                    validateProjectIds(conn, auth.workspaceId(), projectIdsParam);
                    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM project_members WHERE user_id = ?::uuid")) {
                        ps.setString(1, userId);
                        ps.executeUpdate();
                    }
                    for (String pid : projectIdsParam) {
                        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO project_members (project_id, user_id) VALUES (?::uuid, ?::uuid) ON CONFLICT DO NOTHING")) {
                            ps.setString(1, pid);
                            ps.setString(2, userId);
                            ps.executeUpdate();
                        }
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement("UPDATE sessions SET revoked_at = now() WHERE user_id = ?::uuid AND revoked_at IS NULL")) {
                    ps.setString(1, userId);
                    ps.executeUpdate();
                }
                Map<String, Object> auditChanges = new LinkedHashMap<>();
                auditChanges.put("role", newRoleParam);
                auditChanges.put("disabled", disabledParam);
                auditChanges.put("project_ids", projectIdsParam);
                authService.audit(conn, auth, null, "member.update", "user", userId, auditChanges);
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }

            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT u.id::text, u.email, u.role, u.disabled_at, u.created_at,
                       COALESCE(json_agg(p.id::text) FILTER (WHERE p.id IS NOT NULL), '[]'::json)::text AS project_ids
                  FROM users u
                  LEFT JOIN project_members pm ON pm.user_id = u.id
                  LEFT JOIN projects p ON p.id = pm.project_id AND p.workspace_id = u.workspace_id
                 WHERE u.id = ?::uuid AND u.workspace_id = ?::uuid
                 GROUP BY u.id
                """)) {
                ps.setString(1, userId);
                ps.setString(2, auth.workspaceId());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Map<String, Object> view = readMemberRow(rs);
                    hotRuntime.publish();
                    return view;
                }
            }
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to update member");
        }
    }

    @PostMapping("/api/invitations")
    public Map<String, Object> createInvitation(HttpServletRequest req, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.requireAdmin(req);
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        String email = requireEmail(body.get("email"));
        String role = String.valueOf(body.get("role"));
        if (!VALID_ROLES.contains(role)) {
            throw new GatewayException(400, "validation_error", "Invalid role");
        }
        List<String> projectIds = new ArrayList<>();
        if (body.get("project_ids") instanceof List<?> list) {
            Set<String> set = new LinkedHashSet<>();
            for (Object o : list) {
                String pid = String.valueOf(o);
                if (!ControlAuthService.isUuid(pid)) throw new GatewayException(400, "validation_error", "Invalid project UUID");
                set.add(pid);
            }
            projectIds.addAll(set);
        }

        String token = SecurityUtils.randomBase64Url(32);
        Instant expiresAt = Instant.now().plusSeconds(7L * 24 * 3600);
        String id = UUID.randomUUID().toString();

        try (Connection conn = pool.getConnection()) {
            validateProjectIds(conn, auth.workspaceId(), projectIds);
            try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM users WHERE lower(email) = ?")) {
                ps.setString(1, email);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        throw new GatewayException(409, "email_registered", "A user with this email already exists");
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO invitations (id, workspace_id, email, role, token_hash, project_ids, created_by, expires_at)
                VALUES (?::uuid, ?::uuid, ?, ?, ?, ?::jsonb, ?::uuid, ?)
                """)) {
                ps.setString(1, id);
                ps.setString(2, auth.workspaceId());
                ps.setString(3, email);
                ps.setString(4, role);
                ps.setString(5, SecurityUtils.hashKey(token));
                ps.setString(6, mapper.writeValueAsString(projectIds));
                ps.setString(7, auth.userId());
                ps.setTimestamp(8, Timestamp.from(expiresAt));
                ps.executeUpdate();
            }
            authService.audit(conn, auth, null, "invitation.create", "invitation", id, Map.of(
                "email", email, "role", role, "project_ids", projectIds, "expires_at", expiresAt.toString()
            ));
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create invitation");
        }
        hotRuntime.publish();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("email", email);
        out.put("role", role);
        out.put("project_ids", projectIds);
        out.put("expires_at", expiresAt.toString());
        out.put("token", token);
        return out;
    }

    @PostMapping("/api/invitations/accept")
    @SuppressWarnings("unchecked")
    public Map<String, Object> acceptInvitation(@RequestBody(required = false) Map<String, Object> body, HttpServletResponse resp) {
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        String token = body.get("token") instanceof String s ? s.trim() : "";
        if (token.length() < 20 || token.length() > 300) {
            throw new GatewayException(400, "validation_error", "Invalid token");
        }
        String password = requirePassword(body.get("password"));
        String passwordHash = SecurityUtils.hashPassword(password);

        String userId = UUID.randomUUID().toString();
        String email;
        String role;
        String workspaceId;
        List<String> projectIds = new ArrayList<>();

        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                String invId;
                Timestamp acceptedAt;
                Timestamp expiresAt;
                List<String> reqProjIds;
                try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT id::text, workspace_id::text, email, role, project_ids::text, expires_at, accepted_at
                      FROM invitations
                     WHERE token_hash = ?
                     FOR UPDATE
                    """)) {
                    ps.setString(1, SecurityUtils.hashKey(token));
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            throw new GatewayException(400, "invalid_invitation", "Invitation is invalid or has expired");
                        }
                        invId = rs.getString(1);
                        workspaceId = rs.getString(2);
                        email = rs.getString(3);
                        role = rs.getString(4);
                        String pJson = rs.getString(5);
                        reqProjIds = pJson != null ? mapper.readValue(pJson, List.class) : List.of();
                        expiresAt = rs.getTimestamp(6);
                        acceptedAt = rs.getTimestamp(7);
                    }
                }
                if (acceptedAt != null || expiresAt == null || !expiresAt.toInstant().isAfter(Instant.now())) {
                    throw new GatewayException(400, "invalid_invitation", "Invitation is invalid or has expired");
                }
                try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM users WHERE lower(email) = lower(?)")) {
                    ps.setString(1, email);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            throw new GatewayException(409, "email_registered", "A user with this email already exists");
                        }
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO users (id, workspace_id, email, password_hash, role)
                    VALUES (?::uuid, ?::uuid, ?, ?, ?)
                    """)) {
                    ps.setString(1, userId);
                    ps.setString(2, workspaceId);
                    ps.setString(3, email);
                    ps.setString(4, passwordHash);
                    ps.setString(5, role);
                    ps.executeUpdate();
                }
                if (!reqProjIds.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement("SELECT id::text FROM projects WHERE workspace_id = ?::uuid AND id = ANY(?::uuid[])")) {
                        ps.setString(1, workspaceId);
                        ps.setArray(2, conn.createArrayOf("uuid", reqProjIds.toArray()));
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) projectIds.add(rs.getString(1));
                        }
                    }
                    for (String pid : projectIds) {
                        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO project_members (project_id, user_id) VALUES (?::uuid, ?::uuid) ON CONFLICT DO NOTHING")) {
                            ps.setString(1, pid);
                            ps.setString(2, userId);
                            ps.executeUpdate();
                        }
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement("UPDATE invitations SET accepted_at = now() WHERE id = ?::uuid")) {
                    ps.setString(1, invId);
                    ps.executeUpdate();
                }
                SessionInfo actor = new SessionInfo(userId, workspaceId, role, projectIds, "");
                authService.audit(conn, actor, null, "invitation.accept", "user", userId, Map.of(
                    "invitation_id", invId, "role", role, "project_ids", projectIds
                ));
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }

            CreatedSession session = authService.createSession(conn, resp, userId, workspaceId);
            List<Map<String, Object>> projects = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement("SELECT id::text, name FROM projects WHERE workspace_id = ?::uuid AND id = ANY(?::uuid[]) ORDER BY name")) {
                ps.setString(1, workspaceId);
                ps.setArray(2, conn.createArrayOf("uuid", projectIds.toArray()));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        projects.add(Map.of("id", rs.getString(1), "name", rs.getString(2)));
                    }
                }
            }
            hotRuntime.publish();
            Map<String, Object> sessionObj = new LinkedHashMap<>();
            sessionObj.put("user", Map.of("id", userId, "email", email, "role", role, "workspaceId", workspaceId));
            sessionObj.put("projects", projects);
            sessionObj.put("csrf", session.csrf());
            return Map.of("session", sessionObj, "csrf", session.csrf());
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to accept invitation");
        }
    }

    @GetMapping("/api/settings")
    public Map<String, Object> getSettings(HttpServletRequest req) {
        SessionInfo auth = authService.requireAdmin(req);
        return loadSettings(auth.workspaceId());
    }

    @PatchMapping("/api/settings")
    public Map<String, Object> updateSettings(HttpServletRequest req, @RequestBody(required = false) Map<String, Object> body) {
        SessionInfo auth = authService.requireAdmin(req);
        if (body == null || !body.containsKey("request_retention_days")) {
            throw new GatewayException(400, "validation_error", "request_retention_days is required");
        }
        Integer days = parseOptionalInt(body, "request_retention_days", null, 1, 3650);
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 INSERT INTO settings (workspace_id, key, value)
                 VALUES (?::uuid, 'request_retention_days', ?::jsonb)
                 ON CONFLICT (workspace_id, key) DO UPDATE
                    SET value = EXCLUDED.value, updated_at = now()
                 """)) {
            ps.setString(1, auth.workspaceId());
            ps.setString(2, days == null ? "null" : String.valueOf(days));
            ps.executeUpdate();
            Map<String, Object> auditBody = new LinkedHashMap<>();
            auditBody.put("request_retention_days", days);
            authService.audit(conn, auth, null, "settings.update", "workspace_settings", auth.workspaceId(), auditBody);
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to update settings");
        }
        hotRuntime.publish();
        return loadSettings(auth.workspaceId());
    }

    private Map<String, Object> loadSettings(String workspaceId) {
        Integer retention = null;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT value::text FROM settings WHERE workspace_id = ?::uuid AND key = 'request_retention_days'")) {
            ps.setString(1, workspaceId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String raw = rs.getString(1);
                    if (raw != null && !raw.equals("null")) {
                        retention = Integer.parseInt(raw.trim());
                    }
                }
            }
        } catch (Exception ignored) {}
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("request_retention_days", retention);
        return map;
    }

    private void validateProjectIds(Connection conn, String workspaceId, List<String> ids) throws Exception {
        if (ids.isEmpty()) return;
        try (PreparedStatement ps = conn.prepareStatement("SELECT count(*)::int FROM projects WHERE workspace_id = ?::uuid AND id = ANY(?::uuid[])")) {
            ps.setString(1, workspaceId);
            ps.setArray(2, conn.createArrayOf("uuid", ids.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getInt(1) != ids.size()) {
                    throw new GatewayException(400, "invalid_project_ids", "One or more projects do not belong to this workspace");
                }
            }
        }
    }

    private Map<String, Object> readProjectRow(ResultSet rs) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getString(1));
        row.put("workspace_id", rs.getString(2));
        row.put("name", rs.getString(3));
        row.put("budget_micros", rs.getString(4));
        int rpm = rs.getInt(5); row.put("rpm", rs.wasNull() ? null : rpm);
        int tpm = rs.getInt(6); row.put("tpm", rs.wasNull() ? null : tpm);
        int conc = rs.getInt(7); row.put("concurrency", rs.wasNull() ? null : conc);
        int maxOut = rs.getInt(8); row.put("max_output_tokens", rs.wasNull() ? null : maxOut);
        Timestamp cAt = rs.getTimestamp(9);
        Timestamp uAt = rs.getTimestamp(10);
        if (cAt != null) row.put("created_at", cAt.toInstant().toString());
        if (uAt != null) row.put("updated_at", uAt.toInstant().toString());
        return row;
    }

    private Map<String, Object> readMemberRow(ResultSet rs) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getString(1));
        row.put("email", rs.getString(2));
        row.put("role", rs.getString(3));
        Timestamp dAt = rs.getTimestamp(4);
        row.put("disabled_at", dAt != null ? dAt.toInstant().toString() : null);
        Timestamp cAt = rs.getTimestamp(5);
        row.put("created_at", cAt != null ? cAt.toInstant().toString() : null);
        String pJson = rs.getString(6);
        row.put("project_ids", pJson != null ? mapper.readValue(pJson, List.class) : List.of());
        return row;
    }

    private static String requireEmail(Object val) {
        if (!(val instanceof String s)) throw new GatewayException(400, "validation_error", "Invalid email");
        String clean = s.trim().toLowerCase();
        if (clean.isEmpty() || clean.length() > 320 || !EMAIL_RE.matcher(clean).matches()) {
            throw new GatewayException(400, "validation_error", "Invalid email");
        }
        return clean;
    }

    private static String requirePassword(Object val) {
        if (!(val instanceof String s) || s.length() < 10 || s.length() > 200) {
            throw new GatewayException(400, "validation_error", "Password must be 10-200 characters");
        }
        return s;
    }

    private static String requireNonBlank(Object val, String field, int maxLen) {
        if (!(val instanceof String s)) throw new GatewayException(400, "validation_error", field + " is required");
        String clean = s.trim();
        if (clean.isEmpty() || clean.length() > maxLen) {
            throw new GatewayException(400, "validation_error", field + " must be 1-" + maxLen + " characters");
        }
        return clean;
    }

    private static Long parseOptionalLong(Map<String, Object> body, String key1, String key2, long min, long max) {
        Object val = body.containsKey(key1) ? body.get(key1) : (key2 != null ? body.get(key2) : null);
        if (val == null) return null;
        if (!(val instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue())) {
            throw new GatewayException(400, "validation_error", key1 + " must be an integer");
        }
        long l = n.longValue();
        if (l < min || l > max) {
            throw new GatewayException(400, "validation_error", key1 + " out of range");
        }
        return l;
    }

    private static Integer parseOptionalInt(Map<String, Object> body, String key1, String key2, int min, int max) {
        Object val = body.containsKey(key1) ? body.get(key1) : (key2 != null ? body.get(key2) : null);
        if (val == null) return null;
        if (!(val instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue())) {
            throw new GatewayException(400, "validation_error", key1 + " must be an integer");
        }
        int i = n.intValue();
        if (i < min || i > max) {
            throw new GatewayException(400, "validation_error", key1 + " out of range");
        }
        return i;
    }

    private static void setNullableLong(PreparedStatement ps, int idx, Long val) throws Exception {
        if (val == null) ps.setNull(idx, Types.BIGINT);
        else ps.setLong(idx, val);
    }

    private static void setNullableInt(PreparedStatement ps, int idx, Integer val) throws Exception {
        if (val == null) ps.setNull(idx, Types.INTEGER);
        else ps.setInt(idx, val);
    }
}
