package com.jihuayu.aigateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jihuayu.aigateway.config.GatewayConfig;
import com.jihuayu.aigateway.control.ControlAuthService;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotBilling;
import com.jihuayu.aigateway.hot.HotContract.AdmitInput;
import com.jihuayu.aigateway.hot.HotContract.AttemptRecord;
import com.jihuayu.aigateway.hot.HotContract.HotKeys;
import com.jihuayu.aigateway.hot.HotContract.HotRequest;
import com.jihuayu.aigateway.hot.HotContract.SettleInput;
import com.jihuayu.aigateway.hot.HotProjector;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.hot.HotStream;
import com.jihuayu.aigateway.netty.DataPlaneExecutor;
import com.jihuayu.aigateway.security.SecurityUtils;
import com.jihuayu.aigateway.security.SecurityUtils.ProviderUrlException;
import com.jihuayu.aigateway.worker.EventWorker;
import com.jihuayu.aigateway.worker.RetentionService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariDataSource;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "gateway.netty-port=0",
        "gateway.allow-private-upstream=true",
        "gateway.background-workers=false",
        "gateway.data-plane-namespace=java_test_ns"
    }
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AiGatewayIntegrationTest {

    @LocalServerPort
    private int adminPort;

    @Autowired
    private com.jihuayu.aigateway.netty.NettyGatewayServer nettyServer;

    @Autowired
    private HikariDataSource primaryPool;

    @Autowired
    @Qualifier("backgroundDataSource")
    private HikariDataSource backgroundPool;

    @Autowired
    private StatefulRedisConnection<String, String> redisConn;

    @Autowired
    private HotKeys hotKeys;

    @Autowired
    private HotRuntime hotRuntime;

    @Autowired
    private HotBilling hotBilling;

    @Autowired
    private HotProjector hotProjector;

    @Autowired
    private EventWorker eventWorker;

    @Autowired
    private ObjectMapper mapper;

    private HttpServer mockUpstream;
    private int upstreamPort;
    private final AtomicInteger primaryStatus = new AtomicInteger(200);
    private final AtomicInteger primaryCalls = new AtomicInteger(0);
    private final AtomicInteger fallbackCalls = new AtomicInteger(0);

    private HttpClient httpClient;
    private String sessionCookie;
    private String csrfToken;
    private String projectId;
    private String providerId;
    private String primaryTargetId;
    private String fallbackTargetId;
    private String aliasId;
    private String apiKeyPlaintext;
    private String apiKeyId;

    @BeforeAll
    void setupCleanStateAndMockUpstream() throws Exception {
        // Ensure clean DB and Redis before suite
        try (Connection conn = primaryPool.getConnection(); Statement st = conn.createStatement()) {
            st.execute("""
                TRUNCATE TABLE workspaces, users, projects, project_members, sessions, invitations,
                               providers, model_targets, aliases, route_versions, api_keys,
                               requests, attempts, reservations, ledger, outbox, consumer_inbox,
                               audit_events, settings, usage_hourly, dead_letters, billing_state,
                               hot_projection_state, hot_runtime_manifests CASCADE
                """);
        }
        redisConn.sync().flushall();
        hotRuntime.clearLocalCache();
        hotRuntime.initialize();
        hotBilling.setEpoch(hotRuntime.getEpoch());
        hotProjector.setExpectedEpoch(hotRuntime.getEpoch());
        hotBilling.initialize();

        httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();

        mockUpstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockUpstream.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        upstreamPort = mockUpstream.getAddress().getPort();

        mockUpstream.createContext("/v1/models", exchange -> {
            byte[] resp = "{\"object\":\"list\",\"data\":[{\"id\":\"gpt-4.1-mini\"}]}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });

        mockUpstream.createContext("/v1/chat/completions", (HttpExchange exchange) -> {
            byte[] reqBytes = exchange.getRequestBody().readAllBytes();
            Map<?, ?> reqJson = mapper.readValue(reqBytes, Map.class);
            String model = String.valueOf(reqJson.get("model"));
            boolean stream = Boolean.TRUE.equals(reqJson.get("stream"));

            if ("gpt-primary".equals(model)) {
                primaryCalls.incrementAndGet();
                int st = primaryStatus.get();
                if (st != 200) {
                    byte[] err = "{\"error\":{\"message\":\"rate limited\"}}".getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.getResponseHeaders().set("Retry-After", "2");
                    exchange.sendResponseHeaders(st, err.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(err);
                    }
                    return;
                }
            } else {
                fallbackCalls.incrementAndGet();
            }

            if (stream) {
                exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
                exchange.sendResponseHeaders(200, 0);
                try (OutputStream os = exchange.getResponseBody()) {
                    String chunk1 = "data: {\"id\":\"up_1\",\"object\":\"chat.completion.chunk\",\"model\":\"" + model + "\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello \"}}]}\n\n";
                    String chunk2 = "data: {\"id\":\"up_1\",\"object\":\"chat.completion.chunk\",\"model\":\"" + model + "\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Java 25!\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":20,\"total_tokens\":30}}\n\n";
                    String done = "data: [DONE]\n\n";
                    os.write(chunk1.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    os.write(chunk2.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    os.write(done.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
            } else {
                String json = """
                    {
                      "id": "up_123",
                      "object": "chat.completion",
                      "created": 1700000000,
                      "model": "%s",
                      "choices": [
                        {
                          "index": 0,
                          "message": { "role": "assistant", "content": "Hello from %s" },
                          "finish_reason": "stop"
                        }
                      ],
                      "usage": { "prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150 }
                    }
                    """.formatted(model, model);
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        });
        mockUpstream.start();
    }

    @AfterAll
    void tearDown() {
        if (mockUpstream != null) {
            mockUpstream.stop(0);
        }
    }

    @Test
    @Order(1)
    void testSecurityCryptoPasswordAndSsrfGuards() {
        String hexKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        String encrypted = SecurityUtils.encrypt("sk-upstream-secret-123", hexKey);
        assertTrue(encrypted.startsWith("v1."));
        assertEquals("sk-upstream-secret-123", SecurityUtils.decrypt(encrypted, hexKey));

        String pwHash = SecurityUtils.hashPassword("super-secure-password");
        assertTrue(pwHash.startsWith("scrypt:"));
        assertTrue(SecurityUtils.verifyPassword("super-secure-password", pwHash));
        assertFalse(SecurityUtils.verifyPassword("wrong-password", pwHash));

        // SSRF checks
        assertTrue(SecurityUtils.isBlockedAddress("127.0.0.1"));
        assertTrue(SecurityUtils.isBlockedAddress("10.0.0.1"));
        assertTrue(SecurityUtils.isBlockedAddress("169.254.169.254"));
        assertTrue(SecurityUtils.isBlockedAddress("192.168.1.1"));
        assertTrue(SecurityUtils.isBlockedAddress("::1"));
        assertTrue(SecurityUtils.isBlockedAddress("::ffff:127.0.0.1"));
        assertFalse(SecurityUtils.isBlockedAddress("8.8.8.8"));

        assertThrows(ProviderUrlException.class, () -> SecurityUtils.validateProviderUrl("http://127.0.0.1:8080/v1", false));
        assertThrows(ProviderUrlException.class, () -> SecurityUtils.validateProviderUrl("https://169.254.169.254/v1", false));
        assertThrows(ProviderUrlException.class, () -> SecurityUtils.validateProviderUrl("https://user:pass@8.8.8.8/v1", false));
        assertDoesNotThrow(() -> SecurityUtils.validateProviderUrl("http://127.0.0.1:" + upstreamPort + "/v1", true));
    }

    @Test
    @Order(2)
    void testControlPlaneBootstrapProvidersAliasesAndKeys() throws Exception {
        // 1. Static Console HTML served on /
        HttpResponse<String> htmlResp = sendAdmin("GET", "/", null, false);
        assertEquals(200, htmlResp.statusCode());
        assertTrue(htmlResp.body().contains("AI Gateway 控制台"));

        // 2. Bootstrap first admin and project
        HttpResponse<String> bootResp = sendAdmin("POST", "/api/bootstrap", Map.of(
            "email", "admin@example.com",
            "password", "admin-password-123",
            "projectName", "核心网关项目"
        ), false);
        assertEquals(200, bootResp.statusCode(), bootResp.body());
        extractCookies(bootResp);
        assertNotNull(sessionCookie);
        assertNotNull(csrfToken);

        Map<?, ?> bootJson = mapper.readValue(bootResp.body(), Map.class);
        Map<?, ?> projMap = (Map<?, ?>) bootJson.get("project");
        projectId = String.valueOf(projMap.get("id"));
        assertTrue(ControlAuthService.isUuid(projectId));

        // Second bootstrap must fail with 409
        HttpResponse<String> bootAgain = sendAdmin("POST", "/api/bootstrap", Map.of(
            "email", "other@example.com",
            "password", "admin-password-123",
            "projectName", "第二个项目"
        ), false);
        assertEquals(409, bootAgain.statusCode());

        // 3. CSRF enforcement: mutating request without X-CSRF-Token must return 403
        HttpResponse<String> noCsrfResp = sendAdmin("PATCH", "/api/projects/" + projectId, Map.of("budget_micros", 1_000_000L), false);
        assertEquals(403, noCsrfResp.statusCode());

        // With valid CSRF token: set project budget to 500,000 micro-USD ($0.50)
        HttpResponse<String> patchProj = sendAdmin("PATCH", "/api/projects/" + projectId, Map.of(
            "budget_micros", 500_000L,
            "rpm", 600,
            "concurrency", 16,
            "max_output_tokens", 1000
        ), true);
        assertEquals(200, patchProj.statusCode(), patchProj.body());

        // 4. Create Provider & test upstream connection
        HttpResponse<String> provResp = sendAdmin("POST", "/api/providers", Map.of(
            "name", "LocalMockOpenAI",
            "protocol", "openai",
            "base_url", "http://127.0.0.1:" + upstreamPort + "/v1",
            "secret", "sk-upstream-mock-key",
            "enabled", true
        ), true);
        assertEquals(200, provResp.statusCode(), provResp.body());
        Map<?, ?> provJson = mapper.readValue(provResp.body(), Map.class);
        providerId = String.valueOf(provJson.get("id"));
        assertFalse(provJson.containsKey("encrypted_secret"));
        assertFalse(provJson.containsKey("secret"));

        HttpResponse<String> testConnResp = sendAdmin("POST", "/api/providers/" + providerId + "/test", Map.of(), true);
        assertEquals(200, testConnResp.statusCode(), testConnResp.body());
        Map<?, ?> testConnJson = mapper.readValue(testConnResp.body(), Map.class);
        assertEquals("ok", testConnJson.get("status"));

        // 5. Create primary and fallback model targets
        // Primary: input 1,000,000 micros/1M ($1/1M = 1 micro/token), output 2,000,000 micros/1M (2 micros/token)
        HttpResponse<String> t1Resp = sendAdmin("POST", "/api/providers/" + providerId + "/targets", Map.of(
            "upstream_model", "gpt-primary",
            "input_price", 1_000_000L,
            "output_price", 2_000_000L,
            "capabilities", List.of("chat", "stream", "stream_usage"),
            "enabled", true
        ), true);
        assertEquals(200, t1Resp.statusCode(), t1Resp.body());
        primaryTargetId = String.valueOf(mapper.readValue(t1Resp.body(), Map.class).get("id"));

        HttpResponse<String> t2Resp = sendAdmin("POST", "/api/providers/" + providerId + "/targets", Map.of(
            "upstream_model", "gpt-fallback",
            "input_price", 1_000_000L,
            "output_price", 2_000_000L,
            "capabilities", List.of("chat", "stream", "stream_usage"),
            "enabled", true
        ), true);
        assertEquals(200, t2Resp.statusCode(), t2Resp.body());
        fallbackTargetId = String.valueOf(mapper.readValue(t2Resp.body(), Map.class).get("id"));

        // 6. Create Model Alias draft & publish with optimistic version check
        HttpResponse<String> aliasResp = sendAdmin("POST", "/api/aliases", Map.of(
            "project_id", projectId,
            "name", "support-chat",
            "targets", List.of(primaryTargetId, fallbackTargetId),
            "capabilities", List.of("chat", "stream")
        ), true);
        assertEquals(200, aliasResp.statusCode(), aliasResp.body());
        aliasId = String.valueOf(mapper.readValue(aliasResp.body(), Map.class).get("id"));

        // Stale expectedVersion must return 409
        HttpResponse<String> stalePub = sendAdmin("POST", "/api/aliases/" + aliasId + "/publish", Map.of(
            "expectedVersion", 99
        ), true);
        assertEquals(409, stalePub.statusCode());

        // Publish version 1
        HttpResponse<String> pubResp = sendAdmin("POST", "/api/aliases/" + aliasId + "/publish", Map.of(
            "expectedVersion", 1
        ), true);
        assertEquals(200, pubResp.statusCode(), pubResp.body());
        assertEquals("published", mapper.readValue(pubResp.body(), Map.class).get("status"));

        // 7. Create API Key
        HttpResponse<String> keyResp = sendAdmin("POST", "/api/keys", Map.of(
            "project_id", projectId,
            "name", "prod-service-key",
            "allowed_models", List.of("support-chat")
        ), true);
        assertEquals(200, keyResp.statusCode(), keyResp.body());
        Map<?, ?> keyJson = mapper.readValue(keyResp.body(), Map.class);
        apiKeyId = String.valueOf(keyJson.get("id"));
        apiKeyPlaintext = String.valueOf(keyJson.get("key"));
        assertTrue(apiKeyPlaintext.startsWith("sk-"));
    }

    @Test
    @Order(3)
    void testDirectNettyGatewayZeroSqlBufferedAndSseAndFallback() throws Exception {
        int nettyPort = nettyServer.getBoundPort();
        assertTrue(nettyPort > 0);

        // 1. GET /v1/models on direct Netty port
        HttpRequest modelsReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + nettyPort + "/v1/models"))
            .header("Authorization", "Bearer " + apiKeyPlaintext)
            .GET()
            .build();
        HttpResponse<String> modelsResp = httpClient.send(modelsReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, modelsResp.statusCode(), modelsResp.body());
        assertTrue(modelsResp.headers().firstValue("x-request-id").isPresent());
        Map<?, ?> modelsJson = mapper.readValue(modelsResp.body(), Map.class);
        List<?> modelData = (List<?>) modelsJson.get("data");
        assertEquals(1, modelData.size());
        assertEquals("support-chat", ((Map<?, ?>) modelData.get(0)).get("id"));

        // 2. Reject unknown parameter (no silent drop)
        HttpRequest badParamReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + nettyPort + "/v1/chat/completions"))
            .header("Authorization", "Bearer " + apiKeyPlaintext)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"model\":\"support-chat\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"top_p\":0.9}"))
            .build();
        HttpResponse<String> badParamResp = httpClient.send(badParamReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, badParamResp.statusCode());
        assertTrue(badParamResp.body().contains("unsupported_parameter"));

        // 3. Buffered POST /v1/chat/completions on Netty port (100 prompt + 50 completion = 100*1 + 50*2 = 200 micro-USD)
        HttpRequest chatReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + nettyPort + "/v1/chat/completions"))
            .header("Authorization", "Bearer " + apiKeyPlaintext)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"model\":\"support-chat\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello Netty\"}],\"max_tokens\":64}"))
            .build();
        HttpResponse<String> chatResp = httpClient.send(chatReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, chatResp.statusCode(), chatResp.body());
        Map<?, ?> chatJson = mapper.readValue(chatResp.body(), Map.class);
        assertEquals("support-chat", chatJson.get("model"));
        Map<?, ?> usage = (Map<?, ?>) chatJson.get("usage");
        assertEquals(100, ((Number) usage.get("prompt_tokens")).intValue());
        assertEquals(50, ((Number) usage.get("completion_tokens")).intValue());

        // 4. SSE Stream POST /v1/chat/completions on Netty port (10 prompt + 20 completion = 10*1 + 20*2 = 50 micro-USD)
        HttpRequest streamReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + nettyPort + "/v1/chat/completions"))
            .header("Authorization", "Bearer " + apiKeyPlaintext)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"model\":\"support-chat\",\"messages\":[{\"role\":\"user\",\"content\":\"Stream Netty\"}],\"stream\":true,\"max_tokens\":64}"))
            .build();
        HttpResponse<String> streamResp = httpClient.send(streamReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, streamResp.statusCode());
        assertTrue(streamResp.headers().firstValue("content-type").orElse("").contains("text/event-stream"));
        assertTrue(streamResp.body().contains("\"model\":\"support-chat\""));
        assertTrue(streamResp.body().contains("Java 25!"));
        assertTrue(streamResp.body().contains("data: [DONE]"));

        // 5. Fallback when primary returns 429 (100 prompt + 50 completion on fallback = 200 micro-USD)
        primaryStatus.set(429);
        try {
            HttpResponse<String> fbResp = httpClient.send(chatReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, fbResp.statusCode(), fbResp.body());
            assertTrue(fbResp.body().contains("Hello from gpt-fallback"));
        } finally {
            primaryStatus.set(200);
        }

        // Verify PG still has 0 rows before HotProjector drains (proving zero-SQL hot path!)
        try (Connection conn = primaryPool.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT count(*)::int FROM requests");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            assertEquals(0, rs.getInt(1), "Hot path must execute zero SQL on PostgreSQL during request handling");
        }

        // Now drain Redis Stream journal to PostgreSQL via HotProjector + EventWorker
        hotProjector.drain(5_000L);
        eventWorker.tick();

        // Verify PostgreSQL ledger & requests match exact micro-USD (200 + 50 + 200 = 450 micro-USD)
        try (Connection conn = primaryPool.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT count(*)::int, COALESCE(sum(actual_micros),0)::bigint FROM requests WHERE project_id = ?::uuid")) {
                ps.setString(1, projectId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(3, rs.getInt(1));
                    assertEquals(450L, rs.getLong(2));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT count(*)::int, COALESCE(sum(amount_micros),0)::bigint FROM ledger WHERE project_id = ?::uuid")) {
                ps.setString(1, projectId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(3, rs.getInt(1));
                    assertEquals(450L, rs.getLong(2));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT COALESCE(sum(actual_micros),0)::bigint, COALESCE(sum(event_count),0)::bigint FROM usage_hourly WHERE project_id = ?::uuid")) {
                ps.setString(1, projectId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(450L, rs.getLong(1));
                    assertEquals(3L, rs.getLong(2));
                }
            }
        }

        // 6. Verify /api/usage and /api/requests on Control Plane
        HttpResponse<String> usageResp = sendAdmin("GET", "/api/usage?project_id=" + projectId, null, false);
        assertEquals(200, usageResp.statusCode(), usageResp.body());
        Map<?, ?> usageJson = mapper.readValue(usageResp.body(), Map.class);
        assertEquals("450", String.valueOf(usageJson.get("actual_micros")));
        Map<?, ?> budgetObj = (Map<?, ?>) usageJson.get("budget");
        assertEquals("450", String.valueOf(budgetObj.get("spent_micros")));
        assertEquals("0", String.valueOf(budgetObj.get("reserved_micros")));

        HttpResponse<String> reqsResp = sendAdmin("GET", "/api/requests?project_id=" + projectId, null, false);
        assertEquals(200, reqsResp.statusCode(), reqsResp.body());
        List<?> items = (List<?>) mapper.readValue(reqsResp.body(), Map.class).get("items");
        assertEquals(3, items.size());

        // 7. Immediate key revocation enforcement
        HttpResponse<String> revResp = sendAdmin("POST", "/api/keys/" + apiKeyId + "/revoke", Map.of(), true);
        assertEquals(200, revResp.statusCode());

        HttpResponse<String> afterRevoke = httpClient.send(modelsReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(401, afterRevoke.statusCode());
    }

    @Test
    @Order(4)
    void testUnknownSettlementAdjustmentNearMaxSafeIntegerAndRetention() throws Exception {
        // Create a fresh active API key since apiKeyId was revoked at the end of Order(3)
        HttpResponse<String> keyResp = sendAdmin("POST", "/api/keys", Map.of(
            "project_id", projectId,
            "name", "active-billing-key",
            "allowed_models", List.of("support-chat")
        ), true);
        assertEquals(200, keyResp.statusCode(), keyResp.body());
        Map<?, ?> keyJson = mapper.readValue(keyResp.body(), Map.class);
        String activeKeyId = String.valueOf(keyJson.get("id"));
        String activeKeyPlain = String.valueOf(keyJson.get("key"));

        String reqId = UUID.randomUUID().toString();
        String period = DataPlaneExecutor.utcPeriod();
        hotBilling.admit(new AdmitInput(
            reqId,
            projectId,
            activeKeyId,
            period,
            1000L,
            600,
            600,
            16,
            500_000L,
            new LinkedHashMap<String, Object>() {{
                put("alias_id", aliasId);
                put("alias_name", "support-chat");
                put("runtime_revision", redisConn.sync().hget(hotKeys.config(), "revision"));
                put("runtime_key_hash", SecurityUtils.hashKey(activeKeyPlain));
                put("runtime_expires_at", null);
            }},
            Map.of("version", 1)
        ));
        hotBilling.dispatch(reqId);

        // Settle with unknown usage (actualMicros = null -> charges reserveMicros = 1000)
        AttemptRecord unknownAttempt = new AttemptRecord(
            1, primaryTargetId, "gpt-primary", "ok", 200, null,
            true, false, false, null, null, null, 1000L, false, 15L
        );
        hotBilling.settle(reqId, new SettleInput(null, null, null, "completed", List.of(unknownAttempt)));
        hotProjector.drain(5_000L);

        // Now deliver actual usage correction (actualMicros = 350 -> net delta = -650)
        AttemptRecord knownAttempt = new AttemptRecord(
            1, primaryTargetId, "gpt-primary", "ok", 200, null,
            true, false, true, 150L, 100L, 350L, 1000L, false, 15L
        );
        hotBilling.settle(reqId, new SettleInput(350L, 150L, 100L, "completed", List.of(knownAttempt)));

        // Conflicting second actual amount must throw 409 settlement_conflict
        GatewayException conflict = assertThrows(GatewayException.class, () ->
            hotBilling.settle(reqId, new SettleInput(400L, 150L, 125L, "completed", List.of(knownAttempt)))
        );
        assertEquals(409, conflict.getStatusCode());
        assertEquals("settlement_conflict", conflict.getCode());

        hotProjector.drain(5_000L);
        eventWorker.tick();

        // Total spent should now be 450 + 350 = 800 micro-USD across 5 ledger entries (3 settle + 1 estimate + 1 adjust)
        try (Connection conn = primaryPool.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT count(*)::int, COALESCE(sum(amount_micros),0)::bigint FROM ledger WHERE project_id = ?::uuid")) {
            ps.setString(1, projectId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(5, rs.getInt(1));
                assertEquals(800L, rs.getLong(2));
            }
        }

        // Test RetentionService preserves ledger and hot_projection_state tombstones
        RetentionService.cleanExpiredData(primaryPool, 30);
        try (Connection conn = primaryPool.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT count(*)::int FROM hot_projection_state WHERE request_id = ?::uuid")) {
            ps.setString(1, reqId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    private HttpResponse<String> sendAdmin(String method, String path, Object body, boolean includeCsrf) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + adminPort + path));
        if (sessionCookie != null && csrfToken != null) {
            b.header("Cookie", "gw_session=" + sessionCookie + "; gw_csrf=" + csrfToken);
        }
        if (includeCsrf && csrfToken != null) {
            b.header("X-CSRF-Token", csrfToken);
        }
        if (body != null) {
            b.header("Content-Type", "application/json");
            b.method(method, HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)));
        } else {
            b.method(method, HttpRequest.BodyPublishers.noBody());
        }
        return httpClient.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private void extractCookies(HttpResponse<?> resp) {
        for (String h : resp.headers().allValues("set-cookie")) {
            String[] parts = h.split(";")[0].split("=", 2);
            if (parts.length == 2) {
                if ("gw_session".equals(parts[0].trim())) sessionCookie = parts[1].trim();
                if ("gw_csrf".equals(parts[0].trim())) csrfToken = parts[1].trim();
            }
        }
    }
}
