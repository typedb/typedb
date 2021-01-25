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

import grakn.core.concurrent.actor.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ExecutorService {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorService.class);

    public static int PARALLELISATION_FACTOR = -1;
    private static ExecutorService singleton = null;

    private final ForkJoinPool forkJoinPool;
    private final EventLoopGroup eventLoopGroup;
    private final ScheduledThreadPoolExecutor scheduledThreadPool;

    private ExecutorService(int parallelisation) {
        forkJoinPool = (ForkJoinPool) Executors.newWorkStealingPool(parallelisation);
        eventLoopGroup = new EventLoopGroup(parallelisation, "grakn-elg");
        scheduledThreadPool = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPool.setRemoveOnCancelPolicy(true);
    }

    public static synchronized void init(int parallelisationFactor) {
        assert PARALLELISATION_FACTOR == -1 || PARALLELISATION_FACTOR == parallelisationFactor;
        if (singleton == null) {
            PARALLELISATION_FACTOR = parallelisationFactor;
            singleton = new ExecutorService(parallelisationFactor);
        }
    }

    public static ForkJoinPool forkJoinPool() {
        assert singleton != null;
        return singleton.forkJoinPool;
    }

    public static ScheduledThreadPoolExecutor scheduledThreadPool() {
        assert singleton != null;
        return singleton.scheduledThreadPool;
    }

    public static EventLoopGroup eventLoopGroup() {
        assert singleton != null;
        return singleton.eventLoopGroup;
    }
}
