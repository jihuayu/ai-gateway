package com.jihuayu.aigateway.plugin;

import java.util.Map;

/**
 * Algebraic result returned by any plugin stage execution.
 */
public sealed interface PluginResult
    permits PluginResult.Continue,
            PluginResult.Reject,
            PluginResult.ShortCircuit {

    record Continue() implements PluginResult {
        public static final Continue INSTANCE = new Continue();
    }

    record Reject(
        int httpStatus,
        String errorCode,
        String message,
        Map<String, String> headers
    ) implements PluginResult {
        public Reject(int httpStatus, String errorCode, String message) {
            this(httpStatus, errorCode, message, Map.of());
        }
    }

    record ShortCircuit(
        int httpStatus,
        String contentType,
        String responseBody,
        Map<String, String> headers
    ) implements PluginResult {
        public ShortCircuit(int httpStatus, String responseBody, Map<String, String> headers) {
            this(httpStatus, "application/json", responseBody, headers != null ? headers : Map.of());
        }
    }

    static Continue continueExecution() {
        return Continue.INSTANCE;
    }

    static Reject reject(int httpStatus, String errorCode, String message) {
        return new Reject(httpStatus, errorCode, message, Map.of());
    }

    static Reject reject(int httpStatus, String errorCode, String message, Map<String, String> headers) {
        return new Reject(httpStatus, errorCode, message, headers != null ? headers : Map.of());
    }

    static ShortCircuit shortCircuit(int httpStatus, String responseBody, Map<String, String> headers) {
        return new ShortCircuit(httpStatus, "application/json", responseBody, headers != null ? headers : Map.of());
    }
}
