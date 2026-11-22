package com.jihuayu.aigateway.netty;

import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotBilling;
import com.jihuayu.aigateway.hot.RequestContext;
import com.jihuayu.aigateway.netty.DataPlaneExecutor.HttpResponsePayload;
import com.jihuayu.aigateway.netty.DataPlaneExecutor.ResponseSender;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyGatewayServer {
    private final String host;
    private final int port;
    private final String nodeId;
    private final DataPlaneExecutor dataPlane;
    private final HotBilling billing;
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyGatewayServer(String host, int port, DataPlaneExecutor dataPlane, HotBilling billing) {
        this(host, port, defaultNodeId(), dataPlane, billing);
    }

    public NettyGatewayServer(String host, int port, String nodeId, DataPlaneExecutor dataPlane, HotBilling billing) {
        this.host = host;
        this.port = port;
        this.nodeId = (nodeId != null && !nodeId.isBlank()) ? nodeId.trim() : defaultNodeId();
        this.dataPlane = dataPlane;
        this.billing = billing;
    }

    private static String defaultNodeId() {
        String envId = System.getenv("GATEWAY_NODE_ID");
        if (envId != null && !envId.isBlank()) {
            return envId.trim();
        }
        return "node-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String getNodeId() {
        return nodeId;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast("codec", new HttpServerCodec())
                        .addLast("aggregator", new HttpObjectAggregator(DataPlaneExecutor.MAX_BODY_BYTES))
                        .addLast("handler", new GatewayInboundHandler(nodeId, dataPlane, billing, virtualExecutor));
                }
            });
        ChannelFuture f = b.bind(host, port).sync();
        this.serverChannel = f.channel();
    }

    public int getBoundPort() {
        if (serverChannel != null && serverChannel.localAddress() instanceof InetSocketAddress isa) {
            return isa.getPort();
        }
        return port;
    }

    public void stop() {
        try {
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
        } finally {
            if (workerGroup != null) workerGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
            if (bossGroup != null) bossGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
            virtualExecutor.shutdown();
        }
    }

    private static class GatewayInboundHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final String nodeId;
        private final DataPlaneExecutor dataPlane;
        private final HotBilling billing;
        private final ExecutorService virtualExecutor;

        GatewayInboundHandler(String nodeId, DataPlaneExecutor dataPlane, HotBilling billing, ExecutorService virtualExecutor) {
            this.nodeId = nodeId;
            this.dataPlane = dataPlane;
            this.billing = billing;
            this.virtualExecutor = virtualExecutor;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            String requestId = UUID.randomUUID().toString();
            if (req.decoderResult().isFailure()) {
                Throwable cause = req.decoderResult().cause();
                int status = (cause != null && cause.getClass().getSimpleName().contains("TooLongFrame")) ? 413 : 400;
                sendJsonDirect(ctx.channel(), status, requestId, nodeId, "{\"error\":{\"code\":\"invalid_request\",\"message\":\"请求体无效或过大。\"},\"request_id\":\"" + requestId + "\"}", null);
                return;
            }
            String uri = req.uri();
            String path = uri.contains("?") ? uri.substring(0, uri.indexOf('?')) : uri;
            HttpMethod method = req.method();
            String authHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION);
            String xApiKeyHeader = req.headers().get("x-api-key");

            ByteBuf content = req.content();
            byte[] bodyBytes = new byte[content.readableBytes()];
            content.readBytes(bodyBytes);
            Channel ch = ctx.channel();

            virtualExecutor.submit(() ->
                ScopedValue.where(
                    RequestContext.CURRENT,
                    new RequestContext(requestId, "anon", "anon", nodeId, System.nanoTime())
                ).run(() -> {
                    String scopedNode = RequestContext.CURRENT.get().nodeId();
                    if (HttpMethod.GET.equals(method) && "/health/live".equals(path)) {
                        sendJsonDirect(ch, 200, requestId, scopedNode, "{\"status\":\"ok\",\"node_id\":\"" + scopedNode + "\"}", null);
                        return;
                    }
                    if (HttpMethod.GET.equals(method) && "/health/ready".equals(path)) {
                        boolean ready = billing.ready();
                        sendJsonDirect(ch, ready ? 200 : 503, requestId, scopedNode,
                            ready ? "{\"status\":\"ready\",\"node_id\":\"" + scopedNode + "\"}" : "{\"status\":\"unavailable\",\"node_id\":\"" + scopedNode + "\"}", null);
                        return;
                    }
                    if (HttpMethod.GET.equals(method) && "/metrics".equals(path)) {
                        String metricsText = dataPlane.scrapePrometheusMetrics();
                        sendTextDirect(ch, 200, requestId, scopedNode, "text/plain; version=0.0.4; charset=utf-8", metricsText);
                        return;
                    }
                    if (HttpMethod.POST.equals(method) && path.startsWith("/mcp/") && path.length() > "/mcp/".length()) {
                        String serverAlias = path.substring("/mcp/".length()).trim();
                        HttpResponsePayload payload = dataPlane.handleMcpProxy(serverAlias, authHeader, bodyBytes, requestId);
                        sendJsonDirect(ch, payload.status(), requestId, scopedNode, payload.jsonBody(), null, payload.headers());
                        return;
                    }
                    if (HttpMethod.GET.equals(method) && "/v1/models".equals(path)) {
                        HttpResponsePayload payload = dataPlane.handleModels(authHeader, requestId);
                        String retry = payload.headers().get("retry-after");
                        sendJsonDirect(ch, payload.status(), requestId, scopedNode, payload.jsonBody(), retry != null ? Integer.parseInt(retry) : null);
                        return;
                    }
                    if (HttpMethod.POST.equals(method) && "/v1/chat/completions".equals(path)) {
                        NettyResponseSender sender = new NettyResponseSender(ch, scopedNode);
                        dataPlane.handleChat(authHeader, bodyBytes, requestId, sender);
                        return;
                    }
                    if (HttpMethod.POST.equals(method) && "/v1/embeddings".equals(path)) {
                        NettyResponseSender sender = new NettyResponseSender(ch, scopedNode);
                        dataPlane.handleEmbeddings(authHeader, bodyBytes, requestId, sender);
                        return;
                    }
                    if (HttpMethod.POST.equals(method) && ("/v1/rerank".equals(path) || "/v1/rank".equals(path))) {
                        NettyResponseSender sender = new NettyResponseSender(ch, scopedNode);
                        dataPlane.handleRerank(authHeader, bodyBytes, requestId, sender);
                        return;
                    }
                    if (HttpMethod.POST.equals(method) && ("/v1/images/generations".equals(path) || "/v1/images/edits".equals(path))) {
                        NettyResponseSender sender = new NettyResponseSender(ch, scopedNode);
                        dataPlane.handleImageGenerations(authHeader, bodyBytes, requestId, sender);
                        return;
                    }
                    if (HttpMethod.POST.equals(method) && "/v1/messages".equals(path)) {
                        NettyResponseSender sender = new NettyResponseSender(ch, scopedNode);
                        dataPlane.handleAnthropicMessages(authHeader, xApiKeyHeader, bodyBytes, requestId, sender);
                        return;
                    }
                    sendJsonDirect(ch, 404, requestId, scopedNode, "{\"error\":{\"code\":\"not_found\",\"message\":\"Not found\"},\"request_id\":\"" + requestId + "\"}", null);
                })
            );
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private static void addSecurityHeaders(HttpResponse resp, String nodeId) {
        resp.headers().set("X-Content-Type-Options", "nosniff");
        resp.headers().set("Referrer-Policy", "same-origin");
        resp.headers().set("X-Frame-Options", "DENY");
        resp.headers().set("Cache-Control", "no-store");
        resp.headers().set("x-gateway-node", nodeId);
        resp.headers().set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'");
    }

    private static void sendTextDirect(Channel ch, int status, String requestId, String nodeId, String contentType, String body) {
        if (!ch.isActive()) return;
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse resp = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.valueOf(status),
            Unpooled.wrappedBuffer(bytes)
        );
        addSecurityHeaders(resp, nodeId);
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        resp.headers().set("x-request-id", requestId);
        ch.writeAndFlush(resp);
    }

    private static void sendJsonDirect(Channel ch, int status, String requestId, String nodeId, String json, Integer retryAfterSec) {
        sendJsonDirect(ch, status, requestId, nodeId, json, retryAfterSec, Map.of());
    }

    private static void sendJsonDirect(Channel ch, int status, String requestId, String nodeId, String json, Integer retryAfterSec, Map<String, String> extraHeaders) {
        if (!ch.isActive()) return;
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse resp = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.valueOf(status),
            Unpooled.wrappedBuffer(bytes)
        );
        addSecurityHeaders(resp, nodeId);
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
        resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        resp.headers().set("x-request-id", requestId);
        if (retryAfterSec != null && retryAfterSec > 0) {
            resp.headers().set("retry-after", String.valueOf(retryAfterSec));
        }
        if (extraHeaders != null) {
            for (Map.Entry<String, String> e : extraHeaders.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    resp.headers().set(e.getKey(), e.getValue());
                }
            }
        }
        ch.writeAndFlush(resp);
    }

    private static class NettyResponseSender implements ResponseSender {
        private final Channel ch;
        private final String nodeId;
        private final AtomicBoolean sseStarted = new AtomicBoolean(false);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Map<String, String> extraHeaders = new ConcurrentHashMap<>();

        NettyResponseSender(Channel ch, String nodeId) {
            this.ch = ch;
            this.nodeId = nodeId;
            ch.closeFuture().addListener(f -> closed.set(true));
        }

        @Override
        public void addResponseHeaders(Map<String, String> headers) {
            if (headers != null) {
                for (Map.Entry<String, String> e : headers.entrySet()) {
                    if (e.getKey() != null && e.getValue() != null) {
                        extraHeaders.put(e.getKey(), e.getValue());
                    }
                }
            }
        }

        @Override
        public boolean isClientClosed() {
            return closed.get() || !ch.isActive();
        }

        @Override
        public void sendJson(int status, String requestId, String jsonBody, Integer retryAfterSec) {
            sendJsonDirect(ch, status, requestId, nodeId, jsonBody, retryAfterSec, extraHeaders);
        }

        @Override
        public void startSse(String requestId) {
            if (sseStarted.compareAndSet(false, true)) {
                DefaultHttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
                addSecurityHeaders(resp, nodeId);
                resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/event-stream; charset=utf-8");
                resp.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-store");
                resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                resp.headers().set("x-accel-buffering", "no");
                resp.headers().set("x-request-id", requestId);
                for (Map.Entry<String, String> e : extraHeaders.entrySet()) {
                    if (e.getKey() != null && e.getValue() != null) {
                        resp.headers().set(e.getKey(), e.getValue());
                    }
                }
                ch.writeAndFlush(resp);
            }
        }

        @Override
        public boolean isSseStarted() {
            return sseStarted.get();
        }

        @Override
        public void writeSseFrame(String frame, long deadlineAt) {
            if (isClientClosed()) {
                throw new GatewayException(499, "client_closed", "客户端已断开连接。");
            }
            byte[] bytes = frame.getBytes(StandardCharsets.UTF_8);
            ChannelFuture future = ch.writeAndFlush(new DefaultHttpContent(Unpooled.wrappedBuffer(bytes)));
            long waitMs = Math.max(1L, Math.min(120_000L, deadlineAt - System.currentTimeMillis()));
            try {
                if (!future.await(waitMs, TimeUnit.MILLISECONDS)) {
                    ch.close();
                    throw new GatewayException(504, "client_write_timeout", "向客户端写入响应超时。");
                }
                if (!future.isSuccess()) {
                    throw new GatewayException(499, "client_closed", "客户端已断开连接。");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new GatewayException(499, "client_closed", "客户端已断开连接。");
            }
        }

        @Override
        public void finishSse() {
            if (sseStarted.get() && ch.isActive()) {
                ch.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
