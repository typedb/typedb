/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.concurrent.common;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.common.exception.GraknException;
import grakn.core.concurrent.actor.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;

public class ExecutorService {

    public static int PARALLELISATION_FACTOR = -1;

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorService.class);
    private static final String GRAKN_CORE_MAIN_POOL_NAME = "grakn-core-main";
    private static final String GRAKN_CORE_ASYNC_POOL_NAME = "grakn-core-async";
    private static final String GRAKN_CORE_NETWORK_POOL_NAME = "grakn-core-network";
    private static final String GRAKN_CORE_EVENTLOOP_POOL_NAME = "grakn-core-eventloop";
    private static final String GRAKN_CORE_SCHEDULED_POOL_NAME = "grakn-core-scheduled";
    private static final int GRAKN_CORE_SCHEDULED_POOL_SIZE = 1;

    private static ExecutorService singleton = null;

    private final java.util.concurrent.ExecutorService mainPool;
    private final java.util.concurrent.ExecutorService asyncPool;
    private final NioEventLoopGroup networkPool;
    private final EventLoopGroup eventLoopPool;
    private final ScheduledThreadPoolExecutor scheduledThreadPool;

    private ExecutorService(int parallelisation) {
        mainPool = Executors.newFixedThreadPool(parallelisation, new NamedThreadFactory(GRAKN_CORE_MAIN_POOL_NAME));
        asyncPool = Executors.newFixedThreadPool(parallelisation, new NamedThreadFactory(GRAKN_CORE_ASYNC_POOL_NAME));
        eventLoopPool = new EventLoopGroup(parallelisation, new NamedThreadFactory(GRAKN_CORE_EVENTLOOP_POOL_NAME));
        networkPool = new NioEventLoopGroup(parallelisation, NamedThreadFactory.create(GRAKN_CORE_NETWORK_POOL_NAME));
        scheduledThreadPool = new ScheduledThreadPoolExecutor(
                GRAKN_CORE_SCHEDULED_POOL_SIZE, new NamedThreadFactory(GRAKN_CORE_SCHEDULED_POOL_NAME)
        );
        scheduledThreadPool.setRemoveOnCancelPolicy(true);
    }

    public static synchronized void initialise(int parallelisationFactor) {
        if (isInitialised()) throw GraknException.of(ILLEGAL_OPERATION);
        PARALLELISATION_FACTOR = parallelisationFactor;
        singleton = new ExecutorService(parallelisationFactor);
    }

    public static boolean isInitialised() {
        return singleton != null;
    }

    public static java.util.concurrent.ExecutorService main() {
        assert isInitialised();
        return singleton.mainPool;
    }

    public static java.util.concurrent.ExecutorService async() {
        assert isInitialised();
        return singleton.asyncPool;
    }

    public static NioEventLoopGroup network() {
        assert isInitialised();
        return singleton.networkPool;
    }

    public static EventLoopGroup eventLoop() {
        assert isInitialised();
        return singleton.eventLoopPool;
    }

    public static ScheduledThreadPoolExecutor scheduled() {
        assert isInitialised();
        return singleton.scheduledThreadPool;
    }
}
