package com.jihuayu.aigateway.netty;

import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotBilling;
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
import io.netty.handler.codec.TooLongFrameException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyGatewayServer {
    private final String host;
    private final int port;
    private final DataPlaneExecutor dataPlane;
    private final HotBilling billing;
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyGatewayServer(String host, int port, DataPlaneExecutor dataPlane, HotBilling billing) {
        this.host = host;
        this.port = port;
        this.dataPlane = dataPlane;
        this.billing = billing;
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
                        .addLast("handler", new GatewayInboundHandler(dataPlane, billing, virtualExecutor));
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
        private final DataPlaneExecutor dataPlane;
        private final HotBilling billing;
        private final ExecutorService virtualExecutor;

        GatewayInboundHandler(DataPlaneExecutor dataPlane, HotBilling billing, ExecutorService virtualExecutor) {
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
                sendJsonDirect(ctx.channel(), status, requestId, "{\"error\":{\"code\":\"invalid_request\",\"message\":\"请求体无效或过大。\"},\"request_id\":\"" + requestId + "\"}", null);
                return;
            }
            String uri = req.uri();
            String path = uri.contains("?") ? uri.substring(0, uri.indexOf('?')) : uri;
            HttpMethod method = req.method();
            String authHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION);

            ByteBuf content = req.content();
            byte[] bodyBytes = new byte[content.readableBytes()];
            content.readBytes(bodyBytes);
            Channel ch = ctx.channel();

            virtualExecutor.submit(() -> {
                if (HttpMethod.GET.equals(method) && "/health/live".equals(path)) {
                    sendJsonDirect(ch, 200, requestId, "{\"status\":\"ok\"}", null);
                    return;
                }
                if (HttpMethod.GET.equals(method) && "/health/ready".equals(path)) {
                    boolean ready = billing.ready();
                    sendJsonDirect(ch, ready ? 200 : 503, requestId, ready ? "{\"status\":\"ready\"}" : "{\"status\":\"unavailable\"}", null);
                    return;
                }
                if (HttpMethod.GET.equals(method) && "/v1/models".equals(path)) {
                    HttpResponsePayload payload = dataPlane.handleModels(authHeader, requestId);
                    String retry = payload.headers().get("retry-after");
                    sendJsonDirect(ch, payload.status(), requestId, payload.jsonBody(), retry != null ? Integer.parseInt(retry) : null);
                    return;
                }
                if (HttpMethod.POST.equals(method) && "/v1/chat/completions".equals(path)) {
                    NettyResponseSender sender = new NettyResponseSender(ch);
                    dataPlane.handleChat(authHeader, bodyBytes, requestId, sender);
                    return;
                }
                sendJsonDirect(ch, 404, requestId, "{\"error\":{\"code\":\"not_found\",\"message\":\"Not found\"},\"request_id\":\"" + requestId + "\"}", null);
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private static void addSecurityHeaders(HttpResponse resp) {
        resp.headers().set("X-Content-Type-Options", "nosniff");
        resp.headers().set("Referrer-Policy", "same-origin");
        resp.headers().set("X-Frame-Options", "DENY");
        resp.headers().set("Cache-Control", "no-store");
        resp.headers().set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'");
    }

    private static void sendJsonDirect(Channel ch, int status, String requestId, String json, Integer retryAfterSec) {
        if (!ch.isActive()) return;
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse resp = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.valueOf(status),
            Unpooled.wrappedBuffer(bytes)
        );
        addSecurityHeaders(resp);
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
        resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        resp.headers().set("x-request-id", requestId);
        if (retryAfterSec != null && retryAfterSec > 0) {
            resp.headers().set("retry-after", String.valueOf(retryAfterSec));
        }
        ch.writeAndFlush(resp);
    }

    private static class NettyResponseSender implements ResponseSender {
        private final Channel ch;
        private final AtomicBoolean sseStarted = new AtomicBoolean(false);
        private final AtomicBoolean closed = new AtomicBoolean(false);

        NettyResponseSender(Channel ch) {
            this.ch = ch;
            ch.closeFuture().addListener(f -> closed.set(true));
        }

        @Override
        public boolean isClientClosed() {
            return closed.get() || !ch.isActive();
        }

        @Override
        public void sendJson(int status, String requestId, String jsonBody, Integer retryAfterSec) {
            sendJsonDirect(ch, status, requestId, jsonBody, retryAfterSec);
        }

        @Override
        public void startSse(String requestId) {
            if (sseStarted.compareAndSet(false, true)) {
                DefaultHttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
                addSecurityHeaders(resp);
                resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/event-stream; charset=utf-8");
                resp.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-store");
                resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                resp.headers().set("x-accel-buffering", "no");
                resp.headers().set("x-request-id", requestId);
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
