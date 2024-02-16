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
 *
 */

package com.vaticle.typedb.core.common.diagnostics;

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
import java.nio.charset.StandardCharsets;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public interface MonitoringEndpoint {
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
            for (var mw : middleware) {
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
                    response.headers().set(CONTENT_TYPE, TEXT_PLAIN).setInt(CONTENT_LENGTH, response.content().readableBytes());
                    response.headers().set(CONNECTION, CLOSE);
                    ctx.write(response);
                    return;
                }

                boolean keepAlive = HttpUtil.isKeepAlive(req);
                FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                        Unpooled.wrappedBuffer((Diagnostics.get().metrics.formatPrometheus()).getBytes(StandardCharsets.UTF_8)));
                response.headers().set(CONTENT_TYPE, TEXT_PLAIN).setInt(CONTENT_LENGTH, response.content().readableBytes());

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
}
