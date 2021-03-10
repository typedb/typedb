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

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
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
        if (parallelisation <= 0) throw GraknException.of(ILLEGAL_ARGUMENT);
        // TODO: this is temporarily not used until we enable TransactionExecutor
        // serviceExecutorService = newFixedThreadPool(serviceThreadCount(parallelisation), threadFactory(GRAKN_CORE_SERVICE_THREAD_NAME));
        serviceExecutorService = newFixedThreadPool(parallelisation, threadFactory(GRAKN_CORE_SERVICE_THREAD_NAME));
        asyncExecutorService1 = newFixedThreadPool(parallelisation, threadFactory(GRAKN_CORE_ASYNC_THREAD_1_NAME));
        asyncExecutorService2 = newFixedThreadPool(parallelisation, threadFactory(GRAKN_CORE_ASYNC_THREAD_2_NAME));
        actorExecutorService = new ActorExecutorGroup(parallelisation, threadFactory(GRAKN_CORE_ACTOR_THREAD_NAME));
        networkExecutorService = new NioEventLoopGroup(parallelisation, threadFactory(GRAKN_CORE_NETWORK_THREAD_NAME));
        scheduledThreadPool = new ScheduledThreadPoolExecutor(GRAKN_CORE_SCHEDULED_THREAD_SIZE,
                                                              threadFactory(GRAKN_CORE_SCHEDULED_THREAD_NAME));
        scheduledThreadPool.setRemoveOnCancelPolicy(true);
    }

    // TODO: this is temporarily not used until we enable TransactionExecutor
    private int serviceThreadCount(int parallelisation) {
        assert parallelisation > 0;
        if (parallelisation <= 2) return 1;
        else if (parallelisation <= 8) return 2;
        else if (parallelisation <= 16) return 3;
        else if (parallelisation <= 32) return 4;
        else if (parallelisation <= 48) return 5;
        else if (parallelisation <= 64) return 6;
        else if (parallelisation <= 96) return 7;
        else if (parallelisation <= 128) return 8;
        else return 8 + ((parallelisation - 128) / 16);
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
