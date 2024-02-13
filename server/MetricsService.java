/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.vaticle.typedb.core.server;

import com.eclipsesource.json.JsonObject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.ssl.SslContext;

import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.concurrent.executor.Executors.scheduled;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.TimeUnit.HOURS;

public class MetricsService {
    enum NetworkRequestKind {
        CONNECTION_OPEN,
        SERVERS_ALL,
        USER_MANAGEMENT,
        USER,
        DATABASE_MANAGEMENT,
        DATABASE,
        SESSION,
        TRANSACTION,
    }

    enum GaugeKind {
        DATABASE_COUNT,
        SESSION_COUNT,
        TRANSACTION_COUNT,
    }

    private final ConcurrentMap<NetworkRequestKind, AtomicLong> attemptedRequestCounts = new ConcurrentHashMap<>();
    private final ConcurrentMap<NetworkRequestKind, AtomicLong> successfulRequestCounts = new ConcurrentHashMap<>();
    private final ConcurrentMap<GaugeKind, AtomicLong> gauges = new ConcurrentHashMap<>();

    // FIXME this should either go away with either the metrics push endpoint providing a trusted cert,
    // or this should be initialized with correct certs
    private SSLContext sslContext;

    private final String serverID;
    private final String name;
    private final String version;
    private final String reportingURI;

    private ScheduledFuture<?> pushScheduledTask;

    MetricsService(
        String serverID, String name, String version,
        boolean isReportingEnabled, String reportingURI,
        boolean isScrapeEndpointEnabled, Integer scrapePort
    ) {
        this.serverID = serverID;
        this.name = name;
        this.version = version;
        this.reportingURI = reportingURI;

        for (var kind : NetworkRequestKind.values()) {
            attemptedRequestCounts.put(kind, new AtomicLong(0));
            successfulRequestCounts.put(kind, new AtomicLong(0));
        }
        for (var kind : GaugeKind.values()) {
            gauges.put(kind, new AtomicLong(0));
        }

        if (isScrapeEndpointEnabled) {
            (new Thread(() -> this.serve(scrapePort, null))).start();
        }

        if (isReportingEnabled) {
        try {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[] {
                    new X509TrustManager() {
                        @Override public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                        @Override public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                        @Override public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                    }
            }, new java.security.SecureRandom());
            push();
        } catch (Exception ignored) {
            sslContext = null;
        }
        }
    }

    private void push() {
        try {
            HttpsURLConnection conn = (HttpsURLConnection)(new URL(reportingURI)).openConnection();

            conn.setSSLSocketFactory(sslContext.getSocketFactory());

            conn.setRequestMethod("POST");

            conn.setRequestProperty("Charset", "utf-8");
            conn.setRequestProperty("Connection", "close");
            conn.setRequestProperty("Content-Type", "application/json");

            conn.setDoOutput(true);
            conn.getOutputStream().write(formatJSON().getBytes(StandardCharsets.UTF_8));

            conn.connect();

            conn.getInputStream().readAllBytes();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pushScheduledTask = scheduled().schedule(this::push, 1, HOURS);
        }
    }

    public void requestAttempt(NetworkRequestKind kind) {
        attemptedRequestCounts.get(kind).incrementAndGet();
    }

    public void requestSuccess(NetworkRequestKind kind) {
        successfulRequestCounts.get(kind).incrementAndGet();
    }

    public void setGauge(GaugeKind kind, long value) {
        gauges.get(kind).set(value);
    }

    public void serve(Integer scrapePort, @Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap
                .group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new MetricsInitializer(sslContext, middleware));
            Channel channel = bootstrap.bind(scrapePort).sync().channel();
            channel.closeFuture().sync();
        } catch (InterruptedException ignored) {
            // do nothing
        } finally {
            group.shutdownGracefully();
        }
    }

    class MetricsInitializer extends ChannelInitializer<SocketChannel> {
        private final SslContext sslContext;
        private final ChannelInboundHandlerAdapter[] middleware;
        MetricsInitializer(@Nullable SslContext sslContext,ChannelInboundHandlerAdapter... middleware) {
            this.sslContext = sslContext;
            this.middleware = middleware;
        }

        @Override
        protected void initChannel(SocketChannel socketChannel) {
            ChannelPipeline pipeline = socketChannel.pipeline();
            if (sslContext != null) {
                pipeline.addLast(sslContext.newHandler(socketChannel.alloc()));
            }
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new HttpServerExpectContinueHandler());
            for (var mw: middleware) {
                pipeline.addLast(mw);
            }
            pipeline.addLast(new MetricsHandler());
        }
    }


    class MetricsHandler extends SimpleChannelInboundHandler<HttpObject> {
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;

                if (!req.uri().equals("/metrics")) {
                    FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND);
                    response.headers()
                            .set(CONTENT_TYPE, TEXT_PLAIN)
                            .setInt(CONTENT_LENGTH, response.content().readableBytes());
                    response.headers().set(CONNECTION, CLOSE);
                    ctx.write(response);
                    return;
                }

                boolean keepAlive = HttpUtil.isKeepAlive(req);
                FullHttpResponse response = new DefaultFullHttpResponse(
                        req.protocolVersion(), OK, Unpooled.wrappedBuffer((formatPrometheus()).getBytes(StandardCharsets.UTF_8))
                );
                response.headers()
                        .set(CONTENT_TYPE, TEXT_PLAIN)
                        .setInt(CONTENT_LENGTH, response.content().readableBytes());

                if (keepAlive) {
                    if (!req.protocolVersion().isKeepAliveDefault()) {
                        response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                    }
                } else {
                    response.headers().set(CONNECTION, CLOSE);
                }
                ChannelFuture f = ctx.write(response);
                if (!keepAlive) {
                    f.addListener(ChannelFutureListener.CLOSE);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private String formatPrometheus() {
        StringBuilder buf = new StringBuilder(
                "# TypeDB version: " + name + " " + version + "\n" +
                        "# Time zone: " + TimeZone.getDefault().getID() + "\n" +
                        "# Java version: " + System.getProperty("java.vendor") + " " + System.getProperty("java.version") + "\n" +
                        "# Platform: " + System.getProperty("os.name") + " " + System.getProperty("os.arch") + " " + System.getProperty("os.version") + "\n" +
                        "\n" +
                        "# TYPE attempted_requests_total counter\n"
        );

        for (var kind : NetworkRequestKind.values()) {
            buf.append("attempted_requests_total{kind=\"").append(kind).append("\"} ").append(attemptedRequestCounts.get(kind)).append("\n");
        }

        buf.append("\n# TYPE successful_requests_total counter\n");
        for (var kind : NetworkRequestKind.values()) {
            buf.append("successful_requests_total{kind=\"").append(kind).append("\"} ").append(successfulRequestCounts.get(kind)).append("\n");
        }

        buf.append("\n# TYPE current_count gauge\n");
        for (var kind : GaugeKind.values()) {
            buf.append("current_count{kind=\"").append(kind).append("\"} ").append(gauges.get(kind)).append("\n");
        }
        return buf.toString();
    }

    private String formatJSON() {
        JsonObject metrics = new JsonObject();

        JsonObject system = new JsonObject();
        system.add("TypeDB version", name + " " + version);
        system.add("Server ID", serverID);
        system.add("Time zone", TimeZone.getDefault().getID());
        system.add("Java version", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        system.add("Platform", System.getProperty("os.name") + " " + System.getProperty("os.arch") + " " + System.getProperty("os.version"));
        metrics.add("system", system);

        JsonObject requests = new JsonObject();
        for (var kind : NetworkRequestKind.values()) {
            JsonObject requestStats = new JsonObject();
            requestStats.add("attempted", attemptedRequestCounts.get(kind).get());
            requestStats.add("successful", successfulRequestCounts.get(kind).get());
            requests.add(kind.name(), requestStats);
        }
        metrics.add("requests", requests);

        JsonObject current = new JsonObject();
        for (var kind : GaugeKind.values()) {
            current.add(kind.name(), gauges.get(kind).get());
        }
        metrics.add("current", current);

        return metrics.toString();
    }
}
