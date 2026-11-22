package com.jihuayu.aigateway.plugin;

/**
 * 8-Stage Microkernel Plugin Lifecycle Stages aligned with the 3-Phase Redis Lua Financial Ledger.
 */
public enum PluginStage {
    AUTHENTICATE,
    REQUEST_RECEIVED,
    RATE_LIMIT,
    PRE_ADMIT,
    POST_ADMIT_PRE_DISPATCH,
    ROUTE_SELECT,
    PRE_UPSTREAM,
    STREAM_CHUNK,
    RESPONSE_CONTENT,
    POST_SETTLE;

    public static PluginStage fromString(String value) {
        if (value == null || value.isBlank()) {
            return PRE_ADMIT;
        }
        String normalized = value.trim().toUpperCase().replace('-', '_');
        return switch (normalized) {
            case "AUTHENTICATE", "AUTH", "ONAUTHENTICATE", "ON_AUTHENTICATE", "ON_AUTH" -> AUTHENTICATE;
            case "REQUEST_RECEIVED", "ONREQUESTRECEIVED", "ON_REQUEST_RECEIVED" -> REQUEST_RECEIVED;
            case "RATE_LIMIT", "RATELIMIT", "ONRATELIMIT", "ON_RATE_LIMIT" -> RATE_LIMIT;
            case "PRE_ADMIT", "ONPREADMIT", "ON_PRE_ADMIT" -> PRE_ADMIT;
            case "POST_ADMIT_PRE_DISPATCH", "ONPOSTADMITPREDISPATCH", "ON_POST_ADMIT_PRE_DISPATCH", "CACHE", "ON_POST_ADMIT" -> POST_ADMIT_PRE_DISPATCH;
            case "ROUTE_SELECT", "ROUTING", "ONROUTESELECT", "ON_ROUTE_SELECT" -> ROUTE_SELECT;
            case "PRE_UPSTREAM", "ONPREUPSTREAM", "ON_PRE_UPSTREAM" -> PRE_UPSTREAM;
            case "STREAM_CHUNK", "ONSTREAMCHUNK", "ON_STREAM_CHUNK" -> STREAM_CHUNK;
            case "RESPONSE_CONTENT", "ONRESPONSECONTENT", "ON_RESPONSE_CONTENT", "POST_RESPONSE" -> RESPONSE_CONTENT;
            case "POST_SETTLE", "ONPOSTSETTLE", "ON_POST_SETTLE", "BILLING_SINK" -> POST_SETTLE;
            default -> throw new IllegalArgumentException("Unknown plugin stage: " + value);
        };
    }
}
