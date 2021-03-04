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
import grakn.core.concurrent.actor.ActorExecutorGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static java.lang.Math.max;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class Executors {

    public static int PARALLELISATION_FACTOR = -1;

    private static final Logger LOG = LoggerFactory.getLogger(Executors.class);
    private static final String GRAKN_CORE_SERVICE_THREAD_NAME = "grakn-core-service";
    private static final String GRAKN_CORE_ASYNC_THREAD_1_NAME = "grakn-core-async-1";
    private static final String GRAKN_CORE_ASYNC_THREAD_2_NAME = "grakn-core-async-2";
    private static final String GRAKN_CORE_NETWORK_THREAD_NAME = "grakn-core-network";
    private static final String GRAKN_CORE_ACTOR_THREAD_NAME = "grakn-core-actor";
    private static final String GRAKN_CORE_SCHEDULED_THREAD_NAME = "grakn-core-scheduled";
    private static final int GRAKN_CORE_SCHEDULED_THREAD_SIZE = 1;

    private static Executors singleton = null;

    private final ExecutorService serviceExecutorService;
    private final ExecutorService asyncExecutorService1;
    private final ExecutorService asyncExecutorService2;
    private final ActorExecutorGroup actorExecutorService;
    private final NioEventLoopGroup networkExecutorService;
    private final ScheduledThreadPoolExecutor scheduledThreadPool;

    private Executors(int parallelisation) {
        serviceExecutorService = newFixedThreadPool(max(1, parallelisation / 4), threadFactory(GRAKN_CORE_SERVICE_THREAD_NAME));
        asyncExecutorService1 = newFixedThreadPool(parallelisation, threadFactory(GRAKN_CORE_ASYNC_THREAD_1_NAME));
        asyncExecutorService2 = newFixedThreadPool(parallelisation, threadFactory(GRAKN_CORE_ASYNC_THREAD_2_NAME));
        actorExecutorService = new ActorExecutorGroup(parallelisation, threadFactory(GRAKN_CORE_ACTOR_THREAD_NAME));
        networkExecutorService = new NioEventLoopGroup(parallelisation, threadFactory(GRAKN_CORE_NETWORK_THREAD_NAME));
        scheduledThreadPool = new ScheduledThreadPoolExecutor(GRAKN_CORE_SCHEDULED_THREAD_SIZE,
                                                              threadFactory(GRAKN_CORE_SCHEDULED_THREAD_NAME));
        scheduledThreadPool.setRemoveOnCancelPolicy(true);
    }

    private NamedThreadFactory threadFactory(String threadNamePrefix) {
        return NamedThreadFactory.create(threadNamePrefix);
    }

    public static synchronized void initialise(int parallelisationFactor) {
        if (isInitialised()) throw GraknException.of(ILLEGAL_OPERATION);
        PARALLELISATION_FACTOR = parallelisationFactor;
        singleton = new Executors(parallelisationFactor);
    }

    public static boolean isInitialised() {
        return singleton != null;
    }

    public static ExecutorService service() {
        assert isInitialised();
        return singleton.serviceExecutorService;
    }

    public static ExecutorService async1() {
        assert isInitialised();
        return singleton.asyncExecutorService1;
    }

    public static ExecutorService async2() {
        assert isInitialised();
        return singleton.asyncExecutorService2;
    }

    public static ActorExecutorGroup actor() {
        assert isInitialised();
        return singleton.actorExecutorService;
    }

    public static NioEventLoopGroup network() {
        assert isInitialised();
        return singleton.networkExecutorService;
    }

    public static ScheduledThreadPoolExecutor scheduled() {
        assert isInitialised();
        return singleton.scheduledThreadPool;
    }
}
