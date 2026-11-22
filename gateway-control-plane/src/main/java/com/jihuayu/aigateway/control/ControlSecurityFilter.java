package com.jihuayu.aigateway.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.security.SecurityUtils;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ControlSecurityFilter extends OncePerRequestFilter {
    private static final Set<String> SAFE_METHODS = Set.of("GET", "HEAD", "OPTIONS");
    private static final Set<String> AUTH_RATE_PATHS = Set.of("/api/login", "/api/bootstrap", "/api/invitations/accept");
    private static final String AUTH_RATE_LUA = """
        local n = redis.call('INCR', KEYS[1])
        if n == 1 then redis.call('EXPIRE', KEYS[1], 60) end
        return n
        """;

    private final ControlAuthService authService;
    private final StatefulRedisConnection<String, String> redisConn;
    private final ObjectMapper mapper;

    public ControlSecurityFilter(
        ControlAuthService authService,
        StatefulRedisConnection<String, String> redisConn,
        ObjectMapper mapper
    ) {
        this.authService = authService;
        this.redisConn = redisConn;
        this.mapper = mapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
        throws ServletException, IOException {
        resp.setHeader("X-Content-Type-Options", "nosniff");
        resp.setHeader("Referrer-Policy", "same-origin");
        resp.setHeader("X-Frame-Options", "DENY");
        resp.setHeader("Cache-Control", "no-store");
        resp.setHeader("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'");

        String method = req.getMethod().toUpperCase();
        String path = req.getRequestURI();

        try {
            if ("POST".equals(method) && AUTH_RATE_PATHS.contains(path)) {
                String clientIp = req.getRemoteAddr() != null ? req.getRemoteAddr() : "127.0.0.1";
                String rateKey = "auth:" + SecurityUtils.hashKey(clientIp) + ":" + path;
                Long count = redisConn.sync().eval(AUTH_RATE_LUA, ScriptOutputType.INTEGER, new String[]{rateKey});
                if (count != null && count > 20L) {
                    resp.setHeader("Retry-After", "60");
                    throw new GatewayException(429, "auth_rate_limit", "登录尝试过多，请稍后重试。");
                }
            }

            if (!SAFE_METHODS.contains(method) && path.startsWith("/api")) {
                if (!"/api/invitations/accept".equals(path)
                    && !"/api/bootstrap".equals(path)
                    && !"/api/login".equals(path)) {
                    authService.authenticateSession(req);
                    authService.requireCsrf(req);
                }
            }

            chain.doFilter(req, resp);
        } catch (GatewayException ge) {
            writeError(resp, ge.getStatusCode(), ge.getCode(), ge.getMessage());
        } catch (Exception e) {
            if (e.getCause() instanceof GatewayException ge) {
                writeError(resp, ge.getStatusCode(), ge.getCode(), ge.getMessage());
            } else {
                writeError(resp, 500, "internal_error", "服务暂时不可用，请稍后重试。");
            }
        }
    }

    private void writeError(HttpServletResponse resp, int status, String code, String message) throws IOException {
        if (resp.isCommitted()) return;
        resp.setStatus(status);
        resp.setContentType("application/json; charset=utf-8");
        String safeMsg = status >= 500 ? "服务暂时不可用，请稍后重试。" : message;
        String safeCode = (code != null && !code.isBlank()) ? code : (status >= 500 ? "internal_error" : "invalid_request");
        mapper.writeValue(resp.getWriter(), Map.of("error", Map.of("code", safeCode, "message", safeMsg)));
    }

    @RestControllerAdvice
    public static class GlobalErrorHandler {
        @ExceptionHandler(GatewayException.class)
        public ResponseEntity<Map<String, Object>> handleGatewayException(GatewayException ge) {
            int status = ge.getStatusCode();
            String msg = status >= 500 ? "服务暂时不可用，请稍后重试。" : ge.getMessage();
            String code = (ge.getCode() != null && !ge.getCode().isBlank()) ? ge.getCode() : (status >= 500 ? "internal_error" : "invalid_request");
            ResponseEntity.BodyBuilder builder = ResponseEntity.status(status);
            if (ge.getDetails() != null && ge.getDetails().get("retryAfterMs") instanceof Number n) {
                int sec = (int) Math.max(1L, (n.longValue() + 999L) / 1000L);
                builder.header("Retry-After", String.valueOf(sec));
            }
            return builder.body(Map.of("error", Map.of("code", code, "message", msg)));
        }

        @ExceptionHandler(Exception.class)
        public ResponseEntity<Map<String, Object>> handleGenericException(Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                "error", Map.of("code", "internal_error", "message", "服务暂时不可用，请稍后重试。")
            ));
        }
    }
}
