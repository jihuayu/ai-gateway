package com.jihuayu.aigateway.plugin;

public enum PluginStage {
    REQUEST_RECEIVED,
    PRE_ADMIT,
    POST_ADMIT_PRE_DISPATCH,
    PRE_UPSTREAM,
    RESPONSE_CONTENT,
    POST_SETTLE;

    public static PluginStage fromString(String value) {
        if (value == null || value.isBlank()) {
            return PRE_ADMIT;
        }
        String normalized = value.trim().toUpperCase().replace('-', '_');
        return switch (normalized) {
            case "REQUEST_RECEIVED", "ONREQUESTRECEIVED", "ON_REQUEST_RECEIVED" -> REQUEST_RECEIVED;
            case "PRE_ADMIT", "ONPREADMIT", "ON_PRE_ADMIT" -> PRE_ADMIT;
            case "POST_ADMIT_PRE_DISPATCH", "ONPOSTADMITPREDISPATCH", "ON_POST_ADMIT_PRE_DISPATCH", "CACHE" -> POST_ADMIT_PRE_DISPATCH;
            case "PRE_UPSTREAM", "ONPREUPSTREAM", "ON_PRE_UPSTREAM" -> PRE_UPSTREAM;
            case "RESPONSE_CONTENT", "ONRESPONSECONTENT", "ON_RESPONSE_CONTENT", "POST_RESPONSE" -> RESPONSE_CONTENT;
            case "POST_SETTLE", "ONPOSTSETTLE", "ON_POST_SETTLE" -> POST_SETTLE;
            default -> throw new IllegalArgumentException("Unknown plugin stage: " + value);
        };
    }
}
