/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
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
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class MonitoringServer {
    protected static final Logger LOG = LoggerFactory.getLogger(MonitoringServer.class);

    private final Metrics metrics;
    private final int scrapePort;

    public MonitoringServer(Metrics metrics, int scrapePort) {
        this.metrics = metrics;
        this.scrapePort = scrapePort;
    }

    void startServing(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {
        (new Thread(() -> this.serve(sslContext, middleware))).start();
    }

    private void serve(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(group).channel(NioServerSocketChannel.class).childHandler(new MetricsInitializer(sslContext, middleware));
            Channel channel = bootstrap.bind(scrapePort).sync().channel();
            channel.closeFuture().sync();
        } catch (InterruptedException ignored) {
            if (LOG.isTraceEnabled()) LOG.trace("Monitoring server interrupted.");
            // do nothing
        } finally {
            group.shutdownGracefully();
        }
    }

    class MetricsInitializer extends ChannelInitializer<SocketChannel> {
        private final SslContext sslContext;
        private final ChannelInboundHandlerAdapter[] middleware;

        MetricsInitializer(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {
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
            for (ChannelInboundHandlerAdapter handler : middleware) {
                pipeline.addLast(handler);
            }
            pipeline.addLast(new MetricsHandler());
        }
    }

    class MetricsHandler extends SimpleChannelInboundHandler<HttpObject> {
        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;
                URI uri = URI.create(req.uri());

                if (!uri.getPath().equals("/metrics")) {
                    FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND);
                    response.headers().set(CONTENT_TYPE, TEXT_PLAIN).setInt(CONTENT_LENGTH, response.content().readableBytes()).set(CONNECTION, CLOSE);
                    ctx.write(response);
                    return;
                }

                FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK);
                String query = Objects.requireNonNullElse(uri.getQuery(), "");
                if (query.toLowerCase().contains("format=json")) {
                    response.content().writeBytes(Unpooled.wrappedBuffer((metrics.formatJSON()).getBytes(StandardCharsets.UTF_8)));
                    response.headers().set(CONTENT_TYPE, APPLICATION_JSON);
                } else {
                    response.content().writeBytes(Unpooled.wrappedBuffer((metrics.formatPrometheus()).getBytes(StandardCharsets.UTF_8)));
                    response.headers().set(CONTENT_TYPE, TEXT_PLAIN);
                }
                response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes()).set(CONNECTION, CLOSE);

                ctx.write(response).addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (LOG.isTraceEnabled()) LOG.trace("Failed to respond to a metrics request", cause);
            // do nothing
        }
    }
}
