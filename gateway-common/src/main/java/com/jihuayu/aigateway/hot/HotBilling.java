package com.jihuayu.aigateway.hot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jihuayu.aigateway.hot.HotContract.AdmitInput;
import com.jihuayu.aigateway.hot.HotContract.AttemptRecord;
import com.jihuayu.aigateway.hot.HotContract.HotKeys;
import com.jihuayu.aigateway.hot.HotContract.HotRequest;
import com.jihuayu.aigateway.hot.HotContract.SettleInput;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class HotBilling {
    private static final Pattern UUID_RE = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PERIOD_RE = Pattern.compile("^\\d{4}-(0[1-9]|1[0-2])$");
    private static final Pattern STATUS_RE = Pattern.compile("^[a-z][a-z0-9_]{0,31}$");

    private static final String READY_LUA = """
        local r=string.match(redis.call('INFO','server'),'run_id:([%w]+)')
        if redis.call('HGET',KEYS[1],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[1],'run_id')~=r or redis.call('HEXISTS',KEYS[2],'revision')~=1 or redis.call('TYPE',KEYS[4]).ok~='stream' then return 0 end
        local slots=tonumber(redis.call('GET',KEYS[3]))
        if not slots or redis.call('HGET',KEYS[1],'projection_paused')=='1' or redis.call('XLEN',KEYS[4])+slots+7>tonumber(ARGV[2]) then return 0 end
        local info=redis.call('XINFO','STREAM',KEYS[4])
        for i=1,#info,2 do if info[i]=='last-generated-id' and info[i+1]==redis.call('HGET',KEYS[1],'journal_tail') then return 1 end end
        return 0
        """;

    private record CodeEntry(int status, String code) {}

    private static final Map<Integer, CodeEntry> CODES = Map.ofEntries(
        Map.entry(1, new CodeEntry(503, "billing_generation_mismatch")),
        Map.entry(2, new CodeEntry(503, "configuration_changed")),
        Map.entry(3, new CodeEntry(503, "billing_state_lost")),
        Map.entry(4, new CodeEntry(429, "rate_limit_exceeded")),
        Map.entry(5, new CodeEntry(429, "concurrency_limit_exceeded")),
        Map.entry(6, new CodeEntry(429, "budget_exceeded")),
        Map.entry(7, new CodeEntry(409, "request_already_final")),
        Map.entry(8, new CodeEntry(400, "invalid_request")),
        Map.entry(9, new CodeEntry(404, "request_not_found")),
        Map.entry(10, new CodeEntry(503, "event_backlog")),
        Map.entry(11, new CodeEntry(409, "settlement_conflict")),
        Map.entry(12, new CodeEntry(409, "not_admitted")),
        Map.entry(13, new CodeEntry(410, "lease_lost")),
        Map.entry(14, new CodeEntry(409, "already_dispatched")),
        Map.entry(15, new CodeEntry(401, "invalid_api_key"))
    );

    public record RequestScope(String projectId, String keyId, String owner) {}

    private final StatefulRedisConnection<String, String> redisConn;
    private final HotKeys keys;
    private volatile String epoch;
    private final int maxBacklog;
    private final long leaseMs;
    private final ObjectMapper mapper;
    private final Cache<String, RequestScope> scopeCache = Caffeine.newBuilder()
        .maximumSize(65_536)
        .expireAfterAccess(Duration.ofMinutes(15))
        .build();
    private final TokioMpscBatchChannel batchChannel;
    private final DistributedQuotaAndHaEngine haEngine = new DistributedQuotaAndHaEngine();

    private volatile String lua = "";
    private volatile String sha = "";
    private volatile boolean initialized = false;

    public HotBilling(
        StatefulRedisConnection<String, String> redisConn,
        HotKeys keys,
        String epoch,
        int maxBacklog,
        long leaseMs,
        ObjectMapper mapper
    ) {
        this.redisConn = redisConn;
        this.keys = keys;
        this.epoch = epoch;
        this.maxBacklog = maxBacklog;
        this.leaseMs = leaseMs;
        this.mapper = mapper;
        this.batchChannel = new TokioMpscBatchChannel(redisConn);
    }

    public TokioMpscBatchChannel batchChannel() {
        return batchChannel;
    }

    public DistributedQuotaAndHaEngine haEngine() {
        return haEngine;
    }

    public void initialize() {
        try (InputStream in = HotBilling.class.getResourceAsStream("/lua/hot.lua")) {
            if (in == null) throw new IllegalStateException("Missing /lua/hot.lua resource");
            this.lua = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load hot.lua", e);
        }
        this.sha = redisConn.sync().scriptLoad(this.lua);
        this.initialized = true;
    }

    private RequestScope meta(String id) {
        RequestScope cached = scopeCache.getIfPresent(id);
        if (cached != null) return cached;
        String json = redisConn.sync().get(keys.request(id));
        if (json == null || json.isBlank()) {
            throw new GatewayException(404, "request_not_found", "Request not found");
        }
        try {
            HotRequest req = mapper.readValue(json, HotRequest.class);
            RequestScope scope = new RequestScope(req.projectId(), req.keyId(), req.owner());
            remember(id, scope);
            return scope;
        } catch (Exception e) {
            throw new GatewayException(503, "billing_unavailable", "Failed to decode request state");
        }
    }

    private void remember(String id, RequestScope scope) {
        scopeCache.put(id, scope);
    }

    @SuppressWarnings("unchecked")
    private List<Object> operation(String op, String id, Map<String, Object> input, RequestScope explicitScope) {
        if (!initialized) {
            throw new GatewayException(503, "billing_not_initialized", "Billing not initialized");
        }
        RequestScope scope = explicitScope != null ? explicitScope : meta(id);
        String[] scriptKeys = new String[]{
            keys.runtime(),
            keys.config(),
            keys.journal(),
            keys.slots(),
            keys.active(),
            keys.project(scope.projectId()),
            keys.request(id),
            keys.projectRate(scope.projectId()),
            keys.keyRate(scope.keyId())
        };
        Map<String, Object> payloadMap = new LinkedHashMap<>(input);
        payloadMap.put("owner", scope.owner());
        payloadMap.put("at", Instant.now().toString());
        String payload;
        try {
            payload = mapper.writeValueAsString(payloadMap);
        } catch (Exception e) {
            throw new GatewayException(400, "invalid_request", "Invalid metadata payload");
        }
        if (payload.getBytes(StandardCharsets.UTF_8).length > 65536) {
            throw new GatewayException(400, "invalid_request", "Metadata too large");
        }
        String[] argv = new String[]{
            op,
            this.epoch,
            id,
            payload,
            String.valueOf(this.maxBacklog),
            String.valueOf(this.leaseMs)
        };
        List<Object> result;
        try {
            result = batchChannel.submitAndAwait(this.sha, this.lua, scriptKeys, argv, 5_000L);
            haEngine.notifyRedisHealthy();
        } catch (GatewayException ge) {
            throw ge;
        } catch (Exception e) {
            haEngine.notifyRedisFailure();
            throw new GatewayException(503, "billing_unavailable", "Redis billing unavailable");
        }
        int code = ((Number) result.get(0)).intValue();
        if (code != 0) {
            CodeEntry entry = CODES.getOrDefault(code, new CodeEntry(503, "billing_unavailable"));
            Map<String, Object> details = null;
            if (code == 4 && result.size() > 1 && result.get(1) instanceof Number n) {
                details = Map.of("retryAfterMs", n.longValue());
            } else if (code == 6 && result.size() > 1 && result.get(1) instanceof Number n) {
                details = Map.of("availableMicros", n.longValue());
            }
            throw new GatewayException(entry.status(), entry.code(), entry.code(), details);
        }
        return result;
    }

    public void admit(AdmitInput input) {
        admitInternal("admit", input);
    }

    /**
     * P0 Single-RTT combined {@code admit + dispatch} atomic operation via Tokio MPSC Group-Commit channel.
     */
    public void admitAndDispatch(AdmitInput input) {
        admitInternal("admit_and_dispatch", input);
    }

    private void admitInternal(String opName, AdmitInput input) {
        requireUuid(input.requestId());
        requireUuid(input.projectId());
        requireUuid(input.keyId());
        if (input.period() == null || !PERIOD_RE.matcher(input.period()).matches()) {
            throw new GatewayException(400, "invalid_period", "Invalid period");
        }
        requireSafeInt(input.reserveMicros(), 0, HotContract.MAX_SAFE_INTEGER);
        requireSafeInt(input.budgetMicros(), -1, HotContract.MAX_SAFE_INTEGER);
        requireSafeInt(input.keyBudgetMicros(), -1, HotContract.MAX_SAFE_INTEGER);
        requireSafeInt(input.rpm(), 1, 1_000_000);
        requireSafeInt(input.keyRpm(), 1, 1_000_000);
        if (input.projectTpm() != null) requireSafeInt(input.projectTpm(), 1, 1_000_000_000);
        if (input.keyTpm() != null) requireSafeInt(input.keyTpm(), 1, 1_000_000_000);
        requireSafeInt(input.reserveTokens(), 0, HotContract.MAX_SAFE_INTEGER);
        requireSafeInt(input.concurrency(), 1, 1_000_000);

        if (haEngine.isSimulatedRedisOutage()) {
            haEngine.admitDegradedLocal(input);
            remember(input.requestId(), new RequestScope(input.projectId(), input.keyId(), java.util.UUID.randomUUID().toString()));
            return;
        }

        RequestScope scope = new RequestScope(input.projectId(), input.keyId(), UUID.randomUUID().toString());
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("requestId", input.requestId());
        map.put("projectId", input.projectId());
        map.put("keyId", input.keyId());
        map.put("period", input.period());
        map.put("reserveMicros", input.reserveMicros());
        map.put("rpm", input.rpm());
        map.put("keyRpm", input.keyRpm());
        map.put("projectTpm", input.projectTpm());
        map.put("keyTpm", input.keyTpm());
        map.put("reserveTokens", input.reserveTokens());
        map.put("concurrency", input.concurrency());
        map.put("budgetMicros", input.budgetMicros());
        map.put("keyBudgetMicros", input.keyBudgetMicros());
        map.put("routeVersion", input.routeVersion());
        map.put("priceSnapshot", input.priceSnapshot());

        List<Object> values = operation(opName, input.requestId(), map, scope);
        String actualOwner = values.size() > 1 && values.get(1) instanceof String s ? s : scope.owner();
        remember(input.requestId(), new RequestScope(input.projectId(), input.keyId(), actualOwner));
    }

    public void dispatch(String id) {
        String reqId = requireUuid(id);
        if (haEngine.isDegradedRequest(reqId)) {
            return;
        }
        operation("dispatch", reqId, Map.of(), null);
    }

    public void heartbeat(String id) {
        String reqId = requireUuid(id);
        if (haEngine.isDegradedRequest(reqId)) {
            return;
        }
        operation("heartbeat", reqId, Map.of(), null);
    }

    public void cancelBeforeDispatch(String id) {
        String reqId = requireUuid(id);
        if (haEngine.isDegradedRequest(reqId)) {
            haEngine.settleDegradedLocal(reqId, this.epoch, new SettleInput(null, null, null, "cancelled", List.of()));
            return;
        }
        operation("cancel", reqId, Map.of(), null);
    }

    public void settle(String id, SettleInput input) {
        settle(id, null, input);
    }

    public void settle(String id, String aliasName, SettleInput input) {
        if (input.status() == null || !STATUS_RE.matcher(input.status()).matches()) {
            throw new GatewayException(400, "invalid_request", "Invalid status");
        }
        if (input.actualMicros() != null) requireSafeInt(input.actualMicros(), 0, HotContract.MAX_SAFE_INTEGER);
        if (input.inputTokens() != null) requireSafeInt(input.inputTokens(), 0, HotContract.MAX_SAFE_INTEGER);
        if (input.outputTokens() != null) requireSafeInt(input.outputTokens(), 0, HotContract.MAX_SAFE_INTEGER);
        if (input.cachedInputTokens() != null) requireSafeInt(input.cachedInputTokens(), 0, HotContract.MAX_SAFE_INTEGER);
        if (input.reasoningTokens() != null) requireSafeInt(input.reasoningTokens(), 0, HotContract.MAX_SAFE_INTEGER);
        if (input.ttftMs() != null) requireSafeInt(input.ttftMs(), 0, HotContract.MAX_SAFE_INTEGER);
        List<AttemptRecord> rows = validateAttempts(input.attempts());
        String reqId = requireUuid(id);
        if (haEngine.isDegradedRequest(reqId)) {
            haEngine.settleDegradedLocal(reqId, this.epoch, input);
            return;
        }
        String errorCode = deriveErrorCode(input.status(), rows);
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("actualMicros", input.actualMicros());
        map.put("inputTokens", input.inputTokens());
        map.put("outputTokens", input.outputTokens());
        map.put("cachedInputTokens", input.cachedInputTokens());
        map.put("reasoningTokens", input.reasoningTokens());
        map.put("ttftMs", input.ttftMs());
        map.put("status", input.status());
        map.put("attempts", rows);
        map.put("errorCode", errorCode);
        if (aliasName != null && !aliasName.isBlank()) {
            map.put("aliasName", aliasName);
        }
        operation("settle", reqId, map, null);
    }

    public void persistMetadata(String id, String aliasName, String status, List<AttemptRecord> records) {
        String reqId = requireUuid(id);
        if (haEngine.isDegradedRequest(reqId)) {
            return;
        }
        List<AttemptRecord> rows = validateAttempts(records);
        String errorCode = deriveErrorCode(status, rows);
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("aliasName", aliasName);
        map.put("errorCode", errorCode);
        map.put("attempts", rows);
        operation("metadata", reqId, map, null);
    }

    public int reap() {
        List<String> expired = redisConn.sync().zrangebyscore(
            keys.active(),
            Range.create(Double.NEGATIVE_INFINITY, (double) System.currentTimeMillis()),
            Limit.create(0, 50)
        );
        for (String id : expired) {
            operation("expire", id, Map.of(), null);
        }
        return expired.size();
    }

    public boolean ready() {
        if (haEngine.isSimulatedRedisOutage()) {
            return true; // HA Degraded mode keeps data plane alive
        }
        try {
            Long res = redisConn.sync().eval(
                READY_LUA,
                ScriptOutputType.INTEGER,
                new String[]{keys.runtime(), keys.config(), keys.slots(), keys.journal()},
                this.epoch,
                String.valueOf(this.maxBacklog)
            );
            return res != null && res == 1L;
        } catch (Exception e) {
            return false;
        }
    }

    public void setEpoch(String epoch) {
        this.epoch = epoch;
    }

    public void close() {
        this.initialized = false;
        this.batchChannel.close();
    }

    private static String deriveErrorCode(String status, List<AttemptRecord> rows) {
        if ("completed".equals(status)) return null;
        for (int i = rows.size() - 1; i >= 0; i--) {
            AttemptRecord a = rows.get(i);
            if (!"ok".equals(a.status())) {
                if (a.error_code() != null) return a.error_code();
            }
        }
        return "cancelled".equals(status) ? "client_cancelled" : null;
    }

    private static List<AttemptRecord> validateAttempts(List<AttemptRecord> input) {
        if (input == null || input.size() > 8) {
            throw new GatewayException(400, "invalid_request", "Invalid attempts");
        }
        List<AttemptRecord> out = new ArrayList<>(input.size());
        for (AttemptRecord raw : input) {
            requireSafeInt(raw.attempt_no(), 1, 8);
            requireUuid(raw.target_id());
            if (raw.http_status() != null) requireSafeInt(raw.http_status(), 100, 599);
            if (raw.input_tokens() != null) requireSafeInt(raw.input_tokens(), 0, HotContract.MAX_SAFE_INTEGER);
            if (raw.output_tokens() != null) requireSafeInt(raw.output_tokens(), 0, HotContract.MAX_SAFE_INTEGER);
            if (raw.cached_input_tokens() != null) requireSafeInt(raw.cached_input_tokens(), 0, HotContract.MAX_SAFE_INTEGER);
            if (raw.reasoning_tokens() != null) requireSafeInt(raw.reasoning_tokens(), 0, HotContract.MAX_SAFE_INTEGER);
            if (raw.cost_micros() != null) requireSafeInt(raw.cost_micros(), 0, HotContract.MAX_SAFE_INTEGER);
            requireSafeInt(raw.upper_bound_micros(), 0, HotContract.MAX_SAFE_INTEGER);
            String model = raw.upstream_model() != null ? raw.upstream_model().substring(0, Math.min(256, raw.upstream_model().length())) : "";
            String err = raw.error_code() != null ? raw.error_code().substring(0, Math.min(100, raw.error_code().length())) : null;
            out.add(new AttemptRecord(
                raw.attempt_no(),
                raw.target_id(),
                model,
                "ok".equals(raw.status()) ? "ok" : "error",
                raw.http_status(),
                err,
                raw.request_sent(),
                raw.execution_unknown(),
                raw.usage_known(),
                raw.input_tokens(),
                raw.output_tokens(),
                raw.cost_micros(),
                raw.upper_bound_micros(),
                raw.retry_safe(),
                Math.max(0L, raw.latency_ms()),
                raw.cached_input_tokens(),
                raw.reasoning_tokens()
            ));
        }
        return out;
    }

    public static String requireUuid(String s) {
        if (s == null || !UUID_RE.matcher(s).matches()) {
            throw new GatewayException(400, "invalid_request", "Invalid UUID");
        }
        return s;
    }

    public static long requireSafeInt(long val, long min, long max) {
        if (val < min || val > max) {
            throw new GatewayException(400, "invalid_request", "Invalid integer");
        }
        return val;
    }
}
