package com.jihuayu.aigateway.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.control.ControlAuthService.ProjectAccess;
import com.jihuayu.aigateway.control.ControlAuthService.SessionInfo;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotContract;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.security.SecurityUtils;
import jakarta.servlet.http.HttpServletRequest;
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
import javax.sql.DataSource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CatalogController {
    private static final String ALIAS_QUERY = """
        SELECT a.id::text, a.project_id::text, a.name, a.current_version_id::text,
               cv.version AS current_version, cv.targets::text AS current_targets, cv.capabilities::text AS current_capabilities,
               cv.price_snapshot::text AS current_price_snapshot, cv.published_at,
               dv.id::text AS draft_id, dv.version AS draft_version, dv.targets::text AS draft_targets,
               dv.capabilities::text AS draft_capabilities, dv.price_snapshot::text AS draft_price_snapshot, dv.created_at AS draft_created_at
          FROM aliases a
          LEFT JOIN route_versions cv ON cv.id = a.current_version_id
          LEFT JOIN LATERAL (
            SELECT * FROM route_versions WHERE alias_id = a.id AND status = 'draft'
            ORDER BY version DESC LIMIT 1
          ) dv ON true
        """;

    private record TargetDetail(
        String id,
        String provider_id,
        String upstream_model,
        long input_price,
        long output_price,
        Long cached_input_price,
        Long cache_write_price,
        Long reasoning_price,
        List<String> capabilities,
        boolean enabled,
        String provider_name
    ) {}

    private record ParsedTargets(List<String> ids, Map<String, Integer> weights) {}

    private final DataSource pool;
    private final ControlAuthService authService;
    private final HotRuntime hotRuntime;
    private final ObjectMapper mapper;

    public CatalogController(DataSource pool, ControlAuthService authService, HotRuntime hotRuntime, ObjectMapper mapper) {
        this.pool = pool;
        this.authService = authService;
        this.hotRuntime = hotRuntime;
        this.mapper = mapper;
    }

    @GetMapping({"/api/aliases", "/api/model-aliases"})
    public Map<String, Object> listAliasesQuery(HttpServletRequest req, @RequestParam(value = "project_id", required = false) String projectId) {
        return doListAliases(req, requireQueryProjectId(projectId));
    }

    @GetMapping("/api/projects/{id}/aliases")
    public Map<String, Object> listAliasesPath(HttpServletRequest req, @PathVariable("id") String id) {
        requireUuidOr404(id);
        return doListAliases(req, id);
    }

    @PostMapping({"/api/aliases", "/api/model-aliases"})
    public Map<String, Object> createAliasBody(HttpServletRequest req, @RequestBody(required = false) Map<String, Object> body) {
        if (body == null || !(body.get("project_id") instanceof String pid) || !ControlAuthService.isUuid(pid)) {
            throw new GatewayException(400, "validation_error", "project_id is required");
        }
        return doCreateAlias(req, pid, body);
    }

    @PostMapping("/api/projects/{id}/aliases")
    public Map<String, Object> createAliasPath(HttpServletRequest req, @PathVariable("id") String id, @RequestBody(required = false) Map<String, Object> body) {
        requireUuidOr404(id);
        return doCreateAlias(req, id, body);
    }

    @PostMapping("/api/aliases/{id}/versions")
    public Map<String, Object> createAliasVersion(HttpServletRequest req, @PathVariable("id") String aliasId, @RequestBody(required = false) Map<String, Object> body) {
        requireUuidOr404(aliasId);
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        ParsedTargets parsedTargets = parseTargetSpecs(body.get("targets"), body.get("weights"));
        List<String> targetIds = parsedTargets.ids();
        List<String> capabilities = parseCapabilities(body.get("capabilities"));

        try (Connection conn = pool.getConnection()) {
            AliasAccess acc = aliasForAccess(req, conn, aliasId, true);
            List<TargetDetail> targets = loadTargets(conn, acc.workspaceId(), targetIds);
            requireCoverage(targets, capabilities);
            Map<String, Object> snapshot = buildPriceSnapshot(targets, parsedTargets.weights());
            String versionId = UUID.randomUUID().toString();
            int version;

            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM aliases WHERE id = ?::uuid FOR UPDATE")) {
                    ps.setString(1, aliasId);
                    ps.executeQuery();
                }
                try (PreparedStatement ps = conn.prepareStatement("SELECT COALESCE(MAX(version), 0) + 1 FROM route_versions WHERE alias_id = ?::uuid")) {
                    ps.setString(1, aliasId);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        version = rs.getInt(1);
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO route_versions (id, alias_id, version, targets, capabilities, price_snapshot, status, created_by)
                    VALUES (?::uuid, ?::uuid, ?, ?::jsonb, ?::jsonb, ?::jsonb, 'draft', ?::uuid)
                    """)) {
                    ps.setString(1, versionId);
                    ps.setString(2, aliasId);
                    ps.setInt(3, version);
                    ps.setString(4, mapper.writeValueAsString(targetIds));
                    ps.setString(5, mapper.writeValueAsString(capabilities));
                    ps.setString(6, mapper.writeValueAsString(snapshot));
                    ps.setString(7, acc.auth().userId());
                    ps.executeUpdate();
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
            authService.audit(conn, acc.auth(), acc.projectId(), "alias.version.create", "route_version", versionId, Map.of(
                "alias_id", aliasId, "version", version, "targets", targetIds
            ));
            hotRuntime.publish();
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", versionId);
            out.put("alias_id", aliasId);
            out.put("version", version);
            out.put("status", "draft");
            out.put("targets", targetIds);
            out.put("weights", parsedTargets.weights());
            out.put("capabilities", capabilities);
            out.put("price_snapshot", snapshot);
            out.put("target_details", targets);
            return out;
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create route version");
        }
    }

    @PostMapping("/api/aliases/{id}/publish")
    @SuppressWarnings("unchecked")
    public Map<String, Object> publishAlias(HttpServletRequest req, @PathVariable("id") String aliasId, @RequestBody(required = false) Map<String, Object> body) {
        requireUuidOr404(aliasId);
        if (body == null || (!body.containsKey("expectedVersion") && !body.containsKey("expected_version"))) {
            throw new GatewayException(400, "validation_error", "expectedVersion is required");
        }
        Object expRaw = body.containsKey("expectedVersion") ? body.get("expectedVersion") : body.get("expected_version");
        Integer expectedVersion = null;
        if (expRaw != null) {
            if (!(expRaw instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue()) || n.intValue() < 0) {
                throw new GatewayException(400, "validation_error", "expectedVersion must be a non-negative integer");
            }
            expectedVersion = n.intValue();
        }
        boolean hasCurrentVerId = body.containsKey("current_version_id");
        String reqCurrentVerId = null;
        if (hasCurrentVerId && body.get("current_version_id") != null) {
            reqCurrentVerId = String.valueOf(body.get("current_version_id"));
            if (!ControlAuthService.isUuid(reqCurrentVerId)) {
                throw new GatewayException(400, "validation_error", "current_version_id must be a UUID");
            }
        }

        try (Connection conn = pool.getConnection()) {
            AliasAccess acc = aliasForAccess(req, conn, aliasId, true);
            String draftId;
            int draftVersion;
            List<String> draftTargets;
            List<String> draftCaps;
            Map<String, Object> draftSnapshot;
            List<TargetDetail> targets;

            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                String currentVerId;
                try (PreparedStatement ps = conn.prepareStatement("SELECT current_version_id::text FROM aliases WHERE id = ?::uuid FOR UPDATE")) {
                    ps.setString(1, aliasId);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        currentVerId = rs.getString(1);
                    }
                }
                int latest;
                try (PreparedStatement ps = conn.prepareStatement("SELECT COALESCE(MAX(version), 0) FROM route_versions WHERE alias_id = ?::uuid")) {
                    ps.setString(1, aliasId);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        latest = rs.getInt(1);
                    }
                }
                if (hasCurrentVerId) {
                    if ((reqCurrentVerId == null && currentVerId != null) || (reqCurrentVerId != null && !reqCurrentVerId.equals(currentVerId))) {
                        throw new GatewayException(409, "version_conflict", "Alias was modified; reload before publishing");
                    }
                }
                if (expectedVersion == null) {
                    if (currentVerId != null) {
                        throw new GatewayException(409, "version_conflict", "Alias already has a published version");
                    }
                } else if (expectedVersion != latest) {
                    throw new GatewayException(409, "version_conflict", "Alias was modified; reload before publishing");
                }

                try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT id::text, version, targets::text, capabilities::text, price_snapshot::text
                      FROM route_versions
                     WHERE alias_id = ?::uuid AND status = 'draft'
                     ORDER BY version DESC LIMIT 1
                    """)) {
                    ps.setString(1, aliasId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            throw new GatewayException(409, "no_draft", "There is no draft version to publish");
                        }
                        draftId = rs.getString(1);
                        draftVersion = rs.getInt(2);
                        draftTargets = parseJsonTargetIds(rs.getString(3));
                        draftCaps = mapper.readValue(rs.getString(4), List.class);
                        draftSnapshot = mapper.readValue(rs.getString(5), Map.class);
                    }
                }
                if (draftTargets.isEmpty() || draftTargets.size() > 8) {
                    throw new GatewayException(409, "invalid_route", "A route requires 1 to 8 targets");
                }
                targets = loadTargets(conn, acc.workspaceId(), draftTargets);
                requireCoverage(targets, draftCaps);

                try (PreparedStatement ps = conn.prepareStatement("UPDATE route_versions SET status = 'published', published_at = now() WHERE id = ?::uuid AND status = 'draft'")) {
                    ps.setString(1, draftId);
                    if (ps.executeUpdate() == 0) {
                        throw new GatewayException(409, "version_conflict", "Draft was already published");
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement("UPDATE aliases SET current_version_id = ?::uuid, updated_at = now() WHERE id = ?::uuid")) {
                    ps.setString(1, draftId);
                    ps.setString(2, aliasId);
                    ps.executeUpdate();
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }

            authService.audit(conn, acc.auth(), acc.projectId(), "alias.publish", "alias", aliasId, Map.of(
                "version_id", draftId, "version", draftVersion
            ));
            hotRuntime.publish();
            Map<String, TargetDetail> detailMap = new LinkedHashMap<>();
            for (TargetDetail t : targets) detailMap.put(t.id(), t);
            return buildAliasPublic(
                aliasId, acc.projectId(), acc.name(), draftId, draftVersion, draftTargets, draftCaps, draftSnapshot,
                null, null, List.of(), List.of(), Map.of(), detailMap
            );
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to publish alias");
        }
    }

    @PostMapping("/api/aliases/{id}/rollback")
    public Map<String, Object> rollbackAlias(HttpServletRequest req, @PathVariable("id") String aliasId, @RequestBody(required = false) Map<String, Object> body) {
        requireUuidOr404(aliasId);
        Object targetVerObj = body != null ? (body.get("target_version") != null ? body.get("target_version") : body.get("targetVersion")) : null;
        if (!(targetVerObj instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue()) || n.intValue() < 1) {
            throw new GatewayException(400, "validation_error", "target_version is required and must be a positive integer");
        }
        int targetVersionNo = n.intValue();
        try (Connection conn = pool.getConnection()) {
            AliasAccess acc = aliasForAccess(req, conn, aliasId, true);
            List<String> oldTargets;
            List<String> oldCaps;
            Map<String, Integer> oldWeights = new LinkedHashMap<>();
            int newVersion;
            String newVersionId = UUID.randomUUID().toString();
            List<TargetDetail> targets;
            Map<String, Object> snapshot;

            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM aliases WHERE id = ?::uuid FOR UPDATE")) {
                    ps.setString(1, aliasId);
                    ps.executeQuery();
                }
                try (PreparedStatement ps = conn.prepareStatement("SELECT targets::text, capabilities::text, price_snapshot::text FROM route_versions WHERE alias_id = ?::uuid AND version = ? AND status = 'published'")) {
                    ps.setString(1, aliasId);
                    ps.setInt(2, targetVersionNo);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            throw new GatewayException(404, "version_not_found", "Published target version not found");
                        }
                        oldTargets = parseJsonTargetIds(rs.getString(1));
                        oldCaps = parseJsonList(rs.getString(2));
                        Map<String, Object> oldSnap = parseJsonMap(rs.getString(3));
                        if (oldSnap.get("weights") instanceof Map<?, ?> wm) {
                            for (Map.Entry<?, ?> e : wm.entrySet()) {
                                if (e.getKey() != null && e.getValue() instanceof Number wn) {
                                    oldWeights.put(String.valueOf(e.getKey()), wn.intValue());
                                }
                            }
                        }
                    }
                }
                targets = loadTargets(conn, acc.workspaceId(), oldTargets);
                requireCoverage(targets, oldCaps);
                snapshot = buildPriceSnapshot(targets, oldWeights);
                try (PreparedStatement ps = conn.prepareStatement("SELECT COALESCE(MAX(version), 0) + 1 FROM route_versions WHERE alias_id = ?::uuid")) {
                    ps.setString(1, aliasId);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        newVersion = rs.getInt(1);
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO route_versions (id, alias_id, version, targets, capabilities, price_snapshot, status, created_by, published_at) " +
                    "VALUES (?::uuid, ?::uuid, ?, ?::jsonb, ?::jsonb, ?::jsonb, 'published', ?::uuid, now())")) {
                    ps.setString(1, newVersionId);
                    ps.setString(2, aliasId);
                    ps.setInt(3, newVersion);
                    ps.setString(4, mapper.writeValueAsString(oldTargets));
                    ps.setString(5, mapper.writeValueAsString(oldCaps));
                    ps.setString(6, mapper.writeValueAsString(snapshot));
                    ps.setString(7, acc.auth().userId());
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement("UPDATE aliases SET current_version_id = ?::uuid, updated_at = now() WHERE id = ?::uuid")) {
                    ps.setString(1, newVersionId);
                    ps.setString(2, aliasId);
                    ps.executeUpdate();
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
            authService.audit(conn, acc.auth(), acc.projectId(), "alias.rollback", "alias", aliasId, Map.of(
                "from_version", targetVersionNo, "new_version", newVersion, "version_id", newVersionId
            ));
            hotRuntime.publish();
            Map<String, TargetDetail> detailMap = new LinkedHashMap<>();
            for (TargetDetail t : targets) detailMap.put(t.id(), t);
            return buildAliasPublic(
                aliasId, acc.projectId(), acc.name(), newVersionId, newVersion, oldTargets, oldCaps, snapshot,
                null, null, List.of(), List.of(), Map.of(), detailMap
            );
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to rollback alias");
        }
    }

    @GetMapping("/api/keys")
    public Map<String, Object> listKeysQuery(HttpServletRequest req, @RequestParam(value = "project_id", required = false) String projectId) {
        return doListKeys(req, requireQueryProjectId(projectId));
    }

    @GetMapping("/api/projects/{id}/keys")
    public Map<String, Object> listKeysPath(HttpServletRequest req, @PathVariable("id") String id) {
        requireUuidOr404(id);
        return doListKeys(req, id);
    }

    @PostMapping("/api/keys")
    public Map<String, Object> createKeyBody(HttpServletRequest req, @RequestBody(required = false) Map<String, Object> body) {
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        Object pidObj = body.get("project_id") != null ? body.get("project_id") : body.get("projectId");
        if (!(pidObj instanceof String pid) || !ControlAuthService.isUuid(pid)) {
            throw new GatewayException(400, "validation_error", "project_id is required");
        }
        return doCreateKey(req, pid, body);
    }

    @PostMapping("/api/projects/{id}/keys")
    public Map<String, Object> createKeyPath(HttpServletRequest req, @PathVariable("id") String id, @RequestBody(required = false) Map<String, Object> body) {
        requireUuidOr404(id);
        return doCreateKey(req, id, body);
    }

    @PostMapping("/api/keys/{id}/revoke")
    public Map<String, Object> revokeKey(HttpServletRequest req, @PathVariable("id") String keyId) {
        requireUuidOr404(keyId);
        try (Connection conn = pool.getConnection()) {
            String projectId;
            String createdBy;
            String prefix;
            try (PreparedStatement ps = conn.prepareStatement("SELECT project_id::text, created_by::text, prefix FROM api_keys WHERE id = ?::uuid")) {
                ps.setString(1, keyId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new GatewayException(404, "key_not_found", "API key not found");
                    }
                    projectId = rs.getString(1);
                    createdBy = rs.getString(2);
                    prefix = rs.getString(3);
                }
            }
            ProjectAccess acc = authService.requireProject(req, projectId, true);
            if ("developer".equals(acc.auth().role()) && !acc.auth().userId().equals(createdBy)) {
                throw new GatewayException(403, "forbidden", "Developers may only revoke their own keys");
            }
            Map<String, Object> out = new LinkedHashMap<>();
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE api_keys
                   SET revoked_at = COALESCE(revoked_at, now())
                 WHERE id = ?::uuid
                RETURNING id::text, project_id::text, prefix, revoked_at
                """)) {
                ps.setString(1, keyId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    out.put("id", rs.getString(1));
                    out.put("project_id", rs.getString(2));
                    out.put("prefix", rs.getString(3));
                    Timestamp rAt = rs.getTimestamp(4);
                    out.put("revoked_at", rAt != null ? rAt.toInstant().toString() : null);
                }
            }
            authService.audit(conn, acc.auth(), projectId, "key.revoke", "api_key", keyId, Map.of("prefix", prefix));
            hotRuntime.publish();
            return out;
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to revoke key");
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doListAliases(HttpServletRequest req, String projectId) {
        authService.requireProject(req, projectId, false);
        try (Connection conn = pool.getConnection()) {
            List<Map<String, Object>> rawRows = new ArrayList<>();
            Set<String> allTargetIds = new LinkedHashSet<>();
            try (PreparedStatement ps = conn.prepareStatement(ALIAS_QUERY + " WHERE a.project_id = ?::uuid ORDER BY a.name")) {
                ps.setString(1, projectId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> r = new LinkedHashMap<>();
                        r.put("id", rs.getString("id"));
                        r.put("project_id", rs.getString("project_id"));
                        r.put("name", rs.getString("name"));
                        r.put("current_version_id", rs.getString("current_version_id"));
                        int cv = rs.getInt("current_version");
                        r.put("current_version", rs.wasNull() ? null : cv);
                        List<String> ct = parseJsonTargetIds(rs.getString("current_targets"));
                        r.put("current_targets", ct);
                        allTargetIds.addAll(ct);
                        r.put("current_capabilities", parseJsonList(rs.getString("current_capabilities")));
                        r.put("current_price_snapshot", parseJsonMap(rs.getString("current_price_snapshot")));
                        r.put("draft_id", rs.getString("draft_id"));
                        int dv = rs.getInt("draft_version");
                        r.put("draft_version", rs.wasNull() ? null : dv);
                        List<String> dt = parseJsonTargetIds(rs.getString("draft_targets"));
                        r.put("draft_targets", dt);
                        allTargetIds.addAll(dt);
                        r.put("draft_capabilities", parseJsonList(rs.getString("draft_capabilities")));
                        r.put("draft_price_snapshot", parseJsonMap(rs.getString("draft_price_snapshot")));
                        rawRows.add(r);
                    }
                }
            }
            Map<String, TargetDetail> detailMap = loadTargetDetailMap(conn, allTargetIds);
            List<Map<String, Object>> items = new ArrayList<>();
            for (Map<String, Object> r : rawRows) {
                items.add(buildAliasPublic(
                    (String) r.get("id"),
                    (String) r.get("project_id"),
                    (String) r.get("name"),
                    (String) r.get("current_version_id"),
                    (Integer) r.get("current_version"),
                    (List<String>) r.get("current_targets"),
                    (List<String>) r.get("current_capabilities"),
                    (Map<String, Object>) r.get("current_price_snapshot"),
                    (String) r.get("draft_id"),
                    (Integer) r.get("draft_version"),
                    (List<String>) r.get("draft_targets"),
                    (List<String>) r.get("draft_capabilities"),
                    (Map<String, Object>) r.get("draft_price_snapshot"),
                    detailMap
                ));
            }
            return Map.of("items", items);
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to list aliases");
        }
    }

    private Map<String, Object> doCreateAlias(HttpServletRequest req, String projectId, Map<String, Object> body) {
        ProjectAccess acc = authService.requireProject(req, projectId, true);
        if (!"admin".equals(acc.auth().role())) {
            throw new GatewayException(403, "forbidden", "Only admins may change model routes");
        }
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        String name = body.get("name") instanceof String s ? s.trim() : "";
        if (name.isEmpty() || name.length() > 100) {
            throw new GatewayException(400, "validation_error", "name is required");
        }
        ParsedTargets parsedTargets = parseTargetSpecs(body.get("targets"), body.get("weights"));
        List<String> targetIds = parsedTargets.ids();
        List<String> capabilities = parseCapabilities(body.get("capabilities"));

        try (Connection conn = pool.getConnection()) {
            List<TargetDetail> targets = loadTargets(conn, acc.auth().workspaceId(), targetIds);
            requireCoverage(targets, capabilities);
            Map<String, Object> snapshot = buildPriceSnapshot(targets, parsedTargets.weights());
            String aliasId = UUID.randomUUID().toString();
            String versionId = UUID.randomUUID().toString();

            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO aliases (id, project_id, name) VALUES (?::uuid, ?::uuid, ?)")) {
                    ps.setString(1, aliasId);
                    ps.setString(2, projectId);
                    ps.setString(3, name);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO route_versions (id, alias_id, version, targets, capabilities, price_snapshot, status, created_by)
                    VALUES (?::uuid, ?::uuid, 1, ?::jsonb, ?::jsonb, ?::jsonb, 'draft', ?::uuid)
                    """)) {
                    ps.setString(1, versionId);
                    ps.setString(2, aliasId);
                    ps.setString(3, mapper.writeValueAsString(targetIds));
                    ps.setString(4, mapper.writeValueAsString(capabilities));
                    ps.setString(5, mapper.writeValueAsString(snapshot));
                    ps.setString(6, acc.auth().userId());
                    ps.executeUpdate();
                }
                conn.commit();
            } catch (SQLException sqle) {
                conn.rollback();
                if ("23505".equals(sqle.getSQLState())) {
                    throw new GatewayException(409, "alias_exists", "Alias name already exists in this project");
                }
                throw sqle;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }

            authService.audit(conn, acc.auth(), projectId, "alias.create", "alias", aliasId, Map.of(
                "name", name, "targets", targetIds, "weights", parsedTargets.weights(), "capabilities", capabilities
            ));
            hotRuntime.publish();
            Map<String, TargetDetail> detailMap = new LinkedHashMap<>();
            for (TargetDetail t : targets) detailMap.put(t.id(), t);
            return buildAliasPublic(
                aliasId, projectId, name, null, null, List.of(), List.of(), Map.of(),
                versionId, 1, targetIds, capabilities, snapshot, detailMap
            );
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create alias");
        }
    }

    private Map<String, Object> doListKeys(HttpServletRequest req, String projectId) {
        authService.requireProject(req, projectId, false);
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 SELECT id::text, project_id::text, name, prefix, created_by::text, allowed_models::text,
                        expires_at, revoked_at, key_rpm, key_tpm, budget_micros::text, created_at, last_used_at
                   FROM api_keys
                  WHERE project_id = ?::uuid
                  ORDER BY created_at DESC
                 """)) {
            ps.setString(1, projectId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id", rs.getString(1));
                    row.put("project_id", rs.getString(2));
                    row.put("name", rs.getString(3));
                    row.put("prefix", rs.getString(4));
                    row.put("created_by", rs.getString(5));
                    row.put("allowed_models", parseJsonList(rs.getString(6)));
                    Timestamp eAt = rs.getTimestamp(7);
                    row.put("expires_at", eAt != null ? eAt.toInstant().toString() : null);
                    Timestamp rAt = rs.getTimestamp(8);
                    row.put("revoked_at", rAt != null ? rAt.toInstant().toString() : null);
                    int krpm = rs.getInt(9);
                    row.put("key_rpm", rs.wasNull() ? null : krpm);
                    int ktpm = rs.getInt(10);
                    row.put("key_tpm", rs.wasNull() ? null : ktpm);
                    row.put("budget_micros", rs.getString(11));
                    Timestamp cAt = rs.getTimestamp(12);
                    row.put("created_at", cAt != null ? cAt.toInstant().toString() : null);
                    Timestamp lAt = rs.getTimestamp(13);
                    row.put("last_used_at", lAt != null ? lAt.toInstant().toString() : null);
                    items.add(row);
                }
            }
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to list keys");
        }
        return Map.of("items", items);
    }

    private Map<String, Object> doCreateKey(HttpServletRequest req, String projectId, Map<String, Object> body) {
        ProjectAccess acc = authService.requireProject(req, projectId, true);
        if (body == null) throw new GatewayException(400, "validation_error", "Request body required");
        String name = body.get("name") instanceof String s ? s.trim() : "";
        if (name.isEmpty() || name.length() > 100) {
            throw new GatewayException(400, "validation_error", "name is required");
        }
        Set<String> allowedSet = new LinkedHashSet<>();
        Object allowedRaw = body.get("allowed_models") != null ? body.get("allowed_models") : body.get("allowedModels");
        if (allowedRaw != null) {
            if (!(allowedRaw instanceof List<?> list) || list.size() > 100) {
                throw new GatewayException(400, "validation_error", "allowed_models invalid");
            }
            for (Object o : list) {
                if (!(o instanceof String s) || s.trim().isEmpty() || s.trim().length() > 100) {
                    throw new GatewayException(400, "validation_error", "allowed_models item invalid");
                }
                allowedSet.add(s.trim());
            }
        }
        List<String> allowedModels = new ArrayList<>(allowedSet);

        Instant expiresAt;
        if (body.get("expires_at") instanceof String expStr && !expStr.isBlank()) {
            try {
                expiresAt = Instant.parse(expStr);
            } catch (Exception e) {
                throw new GatewayException(400, "invalid_expiry", "expires_at must be valid ISO datetime");
            }
        } else {
            expiresAt = Instant.now().plusSeconds(30L * 24 * 3600);
        }
        if (!expiresAt.isAfter(Instant.now())) {
            throw new GatewayException(400, "invalid_expiry", "expires_at must be in the future");
        }

        Integer keyRpm = null;
        if (body.containsKey("key_rpm") && body.get("key_rpm") != null) {
            if (!(body.get("key_rpm") instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue()) || n.intValue() < 1 || n.intValue() > 1_000_000) {
                throw new GatewayException(400, "validation_error", "key_rpm invalid");
            }
            keyRpm = n.intValue();
        }
        Integer keyTpm = null;
        if (body.containsKey("key_tpm") && body.get("key_tpm") != null) {
            if (!(body.get("key_tpm") instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue()) || n.intValue() < 1 || n.intValue() > 1_000_000_000) {
                throw new GatewayException(400, "validation_error", "key_tpm invalid");
            }
            keyTpm = n.intValue();
        }
        Long keyBudgetMicros = null;
        Object kbRaw = body.containsKey("budget_micros") ? body.get("budget_micros") : body.get("key_budget_micros");
        if (kbRaw != null) {
            if (!(kbRaw instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue()) || n.longValue() < 0 || n.longValue() > HotContract.MAX_SAFE_INTEGER) {
                throw new GatewayException(400, "validation_error", "budget_micros invalid");
            }
            keyBudgetMicros = n.longValue();
        }

        String plaintext = "sk-" + SecurityUtils.randomBase64Url(24);
        String prefix = plaintext.substring(0, 12);
        String id = UUID.randomUUID().toString();

        try (Connection conn = pool.getConnection()) {
            if (!allowedModels.isEmpty()) {
                Set<String> foundNames = new LinkedHashSet<>();
                try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM aliases WHERE project_id = ?::uuid AND name = ANY(?::text[])")) {
                    ps.setString(1, projectId);
                    ps.setArray(2, conn.createArrayOf("text", allowedModels.toArray()));
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) foundNames.add(rs.getString(1));
                    }
                }
                List<String> missing = allowedModels.stream().filter(m -> !foundNames.contains(m)).toList();
                if (!missing.isEmpty()) {
                    throw new GatewayException(400, "invalid_allowed_models", "Unknown model aliases: " + String.join(", ", missing));
                }
            }

            Map<String, Object> created = new LinkedHashMap<>();
            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO api_keys (id, project_id, name, key_hash, prefix, created_by, allowed_models, expires_at, key_rpm, key_tpm, budget_micros)
                VALUES (?::uuid, ?::uuid, ?, ?, ?, ?::uuid, ?::jsonb, ?, ?, ?, ?)
                RETURNING id::text, project_id::text, name, prefix, created_by::text, allowed_models::text, expires_at, key_rpm, key_tpm, budget_micros::text, created_at
                """)) {
                ps.setString(1, id);
                ps.setString(2, projectId);
                ps.setString(3, name);
                ps.setString(4, SecurityUtils.hashKey(plaintext));
                ps.setString(5, prefix);
                ps.setString(6, acc.auth().userId());
                ps.setString(7, mapper.writeValueAsString(allowedModels));
                ps.setTimestamp(8, Timestamp.from(expiresAt));
                if (keyRpm == null) ps.setNull(9, Types.INTEGER);
                else ps.setInt(9, keyRpm);
                if (keyTpm == null) ps.setNull(10, Types.INTEGER);
                else ps.setInt(10, keyTpm);
                if (keyBudgetMicros == null) ps.setNull(11, Types.BIGINT);
                else ps.setLong(11, keyBudgetMicros);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    created.put("id", rs.getString(1));
                    created.put("project_id", rs.getString(2));
                    created.put("name", rs.getString(3));
                    created.put("prefix", rs.getString(4));
                    created.put("created_by", rs.getString(5));
                    created.put("allowed_models", parseJsonList(rs.getString(6)));
                    Timestamp eAt = rs.getTimestamp(7);
                    created.put("expires_at", eAt != null ? eAt.toInstant().toString() : null);
                    int krpm = rs.getInt(8);
                    created.put("key_rpm", rs.wasNull() ? null : krpm);
                    int ktpm = rs.getInt(9);
                    created.put("key_tpm", rs.wasNull() ? null : ktpm);
                    created.put("budget_micros", rs.getString(10));
                    Timestamp cAt = rs.getTimestamp(11);
                    created.put("created_at", cAt != null ? cAt.toInstant().toString() : null);
                }
            }
            created.put("key", plaintext);
            authService.audit(conn, acc.auth(), projectId, "key.create", "api_key", id, Map.of(
                "name", name, "allowed_models", allowedModels, "expires_at", expiresAt.toString()
            ));
            hotRuntime.publish();
            return created;
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            throw new GatewayException(500, "internal_error", "Failed to create API key");
        }
    }

    private record AliasAccess(String aliasId, String projectId, String workspaceId, String name, SessionInfo auth) {}

    private AliasAccess aliasForAccess(HttpServletRequest req, Connection conn, String aliasId, boolean write) throws Exception {
        String projectId;
        String workspaceId;
        String name;
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT a.project_id::text, p.workspace_id::text, a.name
              FROM aliases a JOIN projects p ON p.id = a.project_id
             WHERE a.id = ?::uuid
            """)) {
            ps.setString(1, aliasId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new GatewayException(404, "alias_not_found", "Alias not found");
                }
                projectId = rs.getString(1);
                workspaceId = rs.getString(2);
                name = rs.getString(3);
            }
        }
        ProjectAccess acc = authService.requireProject(req, projectId, write);
        if (write && !"admin".equals(acc.auth().role())) {
            throw new GatewayException(403, "forbidden", "Only admins may change model routes");
        }
        return new AliasAccess(aliasId, projectId, workspaceId, name, acc.auth());
    }

    private List<TargetDetail> loadTargets(Connection conn, String workspaceId, List<String> ids) throws Exception {
        Set<String> unique = new LinkedHashSet<>(ids);
        if (unique.size() != ids.size()) {
            throw new GatewayException(400, "duplicate_target", "Target IDs must be unique");
        }
        Map<String, TargetDetail> byId = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT mt.id::text, mt.provider_id::text, mt.upstream_model, mt.input_price, mt.output_price,
                   mt.cached_input_price, mt.cache_write_price, mt.reasoning_price,
                   mt.capabilities::text, mt.enabled, p.name AS provider_name
              FROM model_targets mt JOIN providers p ON p.id = mt.provider_id
             WHERE mt.id = ANY(?::uuid[]) AND p.workspace_id = ?::uuid
            """)) {
            ps.setArray(1, conn.createArrayOf("uuid", unique.toArray()));
            ps.setString(2, workspaceId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long cIn = rs.getLong(6); Long cInObj = rs.wasNull() ? null : cIn;
                    long cWr = rs.getLong(7); Long cWrObj = rs.wasNull() ? null : cWr;
                    long rPr = rs.getLong(8); Long rPrObj = rs.wasNull() ? null : rPr;
                    TargetDetail td = new TargetDetail(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getLong(4),
                        rs.getLong(5),
                        cInObj,
                        cWrObj,
                        rPrObj,
                        parseJsonList(rs.getString(9)),
                        rs.getBoolean(10),
                        rs.getString(11)
                    );
                    byId.put(td.id(), td);
                }
            }
        }
        if (byId.size() != unique.size()) {
            throw new GatewayException(400, "target_not_found", "One or more targets do not belong to this workspace");
        }
        List<TargetDetail> ordered = new ArrayList<>();
        for (String id : ids) ordered.add(byId.get(id));
        return ordered;
    }

    private Map<String, TargetDetail> loadTargetDetailMap(Connection conn, Set<String> ids) throws Exception {
        if (ids.isEmpty()) return Map.of();
        Map<String, TargetDetail> byId = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT mt.id::text, mt.provider_id::text, mt.upstream_model, mt.input_price, mt.output_price,
                   mt.cached_input_price, mt.cache_write_price, mt.reasoning_price,
                   mt.capabilities::text, mt.enabled, p.name AS provider_name
              FROM model_targets mt JOIN providers p ON p.id = mt.provider_id
             WHERE mt.id = ANY(?::uuid[])
            """)) {
            ps.setArray(1, conn.createArrayOf("uuid", ids.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long cIn = rs.getLong(6); Long cInObj = rs.wasNull() ? null : cIn;
                    long cWr = rs.getLong(7); Long cWrObj = rs.wasNull() ? null : cWr;
                    long rPr = rs.getLong(8); Long rPrObj = rs.wasNull() ? null : rPr;
                    TargetDetail td = new TargetDetail(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getLong(4),
                        rs.getLong(5),
                        cInObj,
                        cWrObj,
                        rPrObj,
                        parseJsonList(rs.getString(9)),
                        rs.getBoolean(10),
                        rs.getString(11)
                    );
                    byId.put(td.id(), td);
                }
            }
        }
        return byId;
    }

    private static void requireCoverage(List<TargetDetail> targets, List<String> required) {
        for (TargetDetail t : targets) {
            List<String> missing = required.stream().filter(c -> !t.capabilities().contains(c)).toList();
            if (!missing.isEmpty()) {
                throw new GatewayException(400, "capability_not_covered", "Target " + t.upstream_model() + " lacks capabilities: " + String.join(", ", missing));
            }
        }
    }

    private static Map<String, Object> buildPriceSnapshot(List<TargetDetail> targets, Map<String, Integer> weights) {
        Map<String, Object> prices = new LinkedHashMap<>();
        for (TargetDetail t : targets) {
            Map<String, Object> tMap = new LinkedHashMap<>();
            tMap.put("upstream_model", t.upstream_model());
            tMap.put("input_micros_per_million", String.valueOf(t.input_price()));
            tMap.put("output_micros_per_million", String.valueOf(t.output_price()));
            if (t.cached_input_price() != null) tMap.put("cached_input_micros_per_million", String.valueOf(t.cached_input_price()));
            if (t.cache_write_price() != null) tMap.put("cache_write_micros_per_million", String.valueOf(t.cache_write_price()));
            if (t.reasoning_price() != null) tMap.put("reasoning_micros_per_million", String.valueOf(t.reasoning_price()));
            tMap.put("weight", weights.getOrDefault(t.id(), 100));
            prices.put(t.id(), tMap);
        }
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("carrier", "USD");
        snap.put("unit", "micros_per_million_tokens");
        snap.put("targets", prices);
        snap.put("weights", weights);
        return snap;
    }

    private Map<String, Object> buildAliasPublic(
        String id, String projectId, String name,
        String currentVerId, Integer currentVer, List<String> currentTargets, List<String> currentCaps, Map<String, Object> currentPriceSnap,
        String draftId, Integer draftVer, List<String> draftTargets, List<String> draftCaps, Map<String, Object> draftPriceSnap,
        Map<String, TargetDetail> details
    ) {
        boolean hasDraft = draftId != null;
        List<String> targets = hasDraft ? draftTargets : currentTargets;
        Integer version = hasDraft ? draftVer : currentVer;
        String status = hasDraft ? "draft" : (currentVer != null ? "published" : "empty");
        List<String> caps = hasDraft ? draftCaps : currentCaps;
        Map<String, Object> snapshot = hasDraft ? draftPriceSnap : currentPriceSnap;

        Map<String, Object> currentVerObj = null;
        if (currentVerId != null) {
            currentVerObj = new LinkedHashMap<>();
            currentVerObj.put("id", currentVerId);
            currentVerObj.put("version", currentVer);
            currentVerObj.put("status", "published");
            currentVerObj.put("targets", currentTargets);
            currentVerObj.put("capabilities", currentCaps);
            currentVerObj.put("price_snapshot", currentPriceSnap);
        }
        Map<String, Object> draftVerObj = null;
        if (draftId != null) {
            draftVerObj = new LinkedHashMap<>();
            draftVerObj.put("id", draftId);
            draftVerObj.put("version", draftVer);
            draftVerObj.put("status", "draft");
            draftVerObj.put("targets", draftTargets);
            draftVerObj.put("capabilities", draftCaps);
            draftVerObj.put("price_snapshot", draftPriceSnap);
        }

        List<TargetDetail> targetDetails = new ArrayList<>();
        for (String tid : targets) {
            TargetDetail td = details.get(tid);
            if (td != null) targetDetails.add(td);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("project_id", projectId);
        out.put("name", name);
        out.put("current_version_id", currentVerId);
        out.put("version", version);
        out.put("status", status);
        out.put("targets", targets);
        out.put("capabilities", caps);
        out.put("price_snapshot", snapshot);
        out.put("target_details", targetDetails);
        out.put("current_version", currentVerObj);
        out.put("draft_version", draftVerObj);
        return out;
    }

    private List<String> parseJsonList(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            List<?> raw = mapper.readValue(json, List.class);
            List<String> out = new ArrayList<>();
            for (Object o : raw) {
                if (o instanceof String s) out.add(s);
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    private List<String> parseJsonTargetIds(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            List<?> raw = mapper.readValue(json, List.class);
            List<String> out = new ArrayList<>();
            for (Object o : raw) {
                if (o instanceof String s) out.add(s);
                else if (o instanceof Map<?, ?> m && m.get("target_id") instanceof String ts) out.add(ts);
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    private Map<String, Object> parseJsonMap(String json) {
        if (json == null || json.isBlank()) return Map.of();
        try {
            return mapper.readValue(json, Map.class);
        } catch (Exception e) {
            return Map.of();
        }
    }

    private static ParsedTargets parseTargetSpecs(Object rawTargets, Object rawWeights) {
        if (!(rawTargets instanceof List<?> list) || list.isEmpty() || list.size() > 8) {
            throw new GatewayException(400, "validation_error", "targets must contain 1 to 8 target entries");
        }
        Map<String, Integer> weights = new LinkedHashMap<>();
        if (rawWeights instanceof Map<?, ?> wm) {
            for (Map.Entry<?, ?> e : wm.entrySet()) {
                if (e.getKey() instanceof String k && e.getValue() instanceof Number n) {
                    int w = n.intValue();
                    if (w < 0 || w > 10_000) throw new GatewayException(400, "validation_error", "weight must be between 0 and 10000");
                    weights.put(k, w);
                }
            }
        }
        List<String> ids = new ArrayList<>();
        for (Object o : list) {
            if (o instanceof String s && ControlAuthService.isUuid(s)) {
                ids.add(s);
                weights.putIfAbsent(s, 100);
            } else if (o instanceof Map<?, ?> m) {
                Object tidObj = m.get("target_id") != null ? m.get("target_id") : m.get("id");
                if (!(tidObj instanceof String tid) || !ControlAuthService.isUuid(tid)) {
                    throw new GatewayException(400, "validation_error", "target_id must be a valid UUID");
                }
                int w = 100;
                if (m.get("weight") != null) {
                    if (!(m.get("weight") instanceof Number wn) || wn.intValue() < 0 || wn.intValue() > 10_000) {
                        throw new GatewayException(400, "validation_error", "weight must be between 0 and 10000");
                    }
                    w = wn.intValue();
                }
                ids.add(tid);
                weights.put(tid, w);
            } else {
                throw new GatewayException(400, "validation_error", "target must be a valid UUID or {target_id, weight} object");
            }
        }
        return new ParsedTargets(ids, weights);
    }

    private static List<String> parseCapabilities(Object val) {
        if (val == null) return List.of();
        if (!(val instanceof List<?> list) || list.size() > 20) {
            throw new GatewayException(400, "validation_error", "capabilities must be an array up to 20 items");
        }
        List<String> out = new ArrayList<>();
        for (Object o : list) {
            if (!(o instanceof String s) || s.trim().isEmpty() || s.trim().length() > 64) {
                throw new GatewayException(400, "validation_error", "invalid capability");
            }
            out.add(s.trim());
        }
        return out;
    }

    private static String requireQueryProjectId(String projectId) {
        if (!ControlAuthService.isUuid(projectId)) {
            throw new GatewayException(400, "project_id_required", "A valid project_id query parameter is required");
        }
        return projectId;
    }

    private static void requireUuidOr404(String id) {
        if (!ControlAuthService.isUuid(id)) {
            throw new GatewayException(404, "not_found", "Resource not found");
        }
    }
}
