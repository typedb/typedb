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

package com.vaticle.typedb.core.common.diagnostics;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;

import javax.annotation.Nullable;

public class CoreMonitoringEndpoint implements MonitoringEndpoint {
    CoreMonitoringEndpoint(Integer scrapePort) {
        (new Thread(() -> this.serve(scrapePort, null))).start();
    }

    void serve(Integer scrapePort, @Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(group).channel(NioServerSocketChannel.class).childHandler(new MetricsInitializer(sslContext, middleware));
            Channel channel = bootstrap.bind(scrapePort).sync().channel();
            channel.closeFuture().sync();
        } catch (InterruptedException ignored) {
            // do nothing
        } finally {
            group.shutdownGracefully();
        }
    }
}
