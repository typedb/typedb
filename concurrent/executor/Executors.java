/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.concurrent.executor;

import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;

public class Executors {

    public static int PARALLELISATION_FACTOR = -1;

    private static final Logger LOG = LoggerFactory.getLogger(Executors.class);
    private static final String TYPEDB_CORE_SERVICE_THREAD_NAME = "typedb-service";
    private static final String TYPEDB_CORE_ASYNC_THREAD_1_NAME = "typedb-async-1";
    private static final String TYPEDB_CORE_ASYNC_THREAD_2_NAME = "typedb-async-2";
    private static final String TYPEDB_CORE_NETWORK_THREAD_NAME = "typedb-network";
    private static final String TYPEDB_CORE_ACTOR_THREAD_NAME = "typedb-actor";
    private static final String TYPEDB_CORE_SCHEDULED_THREAD_NAME = "typedb-scheduled";
    private static final int TYPEDB_CORE_SCHEDULED_THREAD_SIZE = 1;

    private static Executors singleton = null;

    private final ParallelThreadPoolExecutor serviceExecutorService;
    private final ParallelThreadPoolExecutor asyncExecutorService1;
    private final ParallelThreadPoolExecutor asyncExecutorService2;
    private final ActorExecutorGroup actorExecutorService;
    private final NioEventLoopGroup networkExecutorService;
    private final ScheduledThreadPoolExecutor scheduledThreadPool;

    private Executors(int parallelisation) {
        if (parallelisation <= 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
        serviceExecutorService = new ParallelThreadPoolExecutor(parallelisation, threadFactory(TYPEDB_CORE_SERVICE_THREAD_NAME));
        asyncExecutorService1 = new ParallelThreadPoolExecutor(parallelisation, threadFactory(TYPEDB_CORE_ASYNC_THREAD_1_NAME));
        asyncExecutorService2 = new ParallelThreadPoolExecutor(parallelisation, threadFactory(TYPEDB_CORE_ASYNC_THREAD_2_NAME));
        actorExecutorService = new ActorExecutorGroup(parallelisation, threadFactory(TYPEDB_CORE_ACTOR_THREAD_NAME));
        networkExecutorService = new NioEventLoopGroup(parallelisation, threadFactory(TYPEDB_CORE_NETWORK_THREAD_NAME));
        scheduledThreadPool = new ScheduledThreadPoolExecutor(TYPEDB_CORE_SCHEDULED_THREAD_SIZE,
                                                              threadFactory(TYPEDB_CORE_SCHEDULED_THREAD_NAME));
        scheduledThreadPool.setRemoveOnCancelPolicy(true);
    }

    private NamedThreadFactory threadFactory(String threadNamePrefix) {
        return NamedThreadFactory.create(threadNamePrefix);
    }

    public static synchronized void initialise(int parallelisationFactor) {
        if (isInitialised()) throw TypeDBException.of(ILLEGAL_OPERATION);
        PARALLELISATION_FACTOR = parallelisationFactor;
        singleton = new Executors(parallelisationFactor);
    }

    public static boolean isInitialised() {
        return singleton != null;
    }

    public static ParallelThreadPoolExecutor service() {
        assert isInitialised();
        return singleton.serviceExecutorService;
    }

    public static ParallelThreadPoolExecutor async1() {
        assert isInitialised();
        return singleton.asyncExecutorService1;
    }

    public static ParallelThreadPoolExecutor async2() {
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
