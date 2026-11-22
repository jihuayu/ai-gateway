package com.jihuayu.aigateway.hot;

import java.util.Map;

public class GatewayException extends RuntimeException {
    private final int statusCode;
    private final String code;
    private final Map<String, Object> details;

    public GatewayException(int statusCode, String code, String message) {
        this(statusCode, code, message, null);
    }

    public GatewayException(int statusCode, String code, String message, Map<String, Object> details) {
        super(message);
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getCode() {
        return code;
    }

    public Map<String, Object> getDetails() {
        return details;
    }
}
