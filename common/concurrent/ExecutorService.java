/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.common.concurrent;

import grakn.common.concurrent.actor.EventLoopGroup;

import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

public class ExecutorService {

    private static ExecutorService singleton;

    private ForkJoinPool forkJoinPool;
    private EventLoopGroup eventLoopGroup;

    private ExecutorService(int forkJoinPoolSize, int eventLoopGroupSize) {
        forkJoinPool = (ForkJoinPool) Executors.newWorkStealingPool(forkJoinPoolSize);
        eventLoopGroup = new EventLoopGroup(eventLoopGroupSize, "grakn-elg");
    }


    public static synchronized void init(int forkJoinPoolSize, int eventLoopGroupSize) {
        if (singleton == null) singleton = new ExecutorService(forkJoinPoolSize, eventLoopGroupSize);
    }

    public static ForkJoinPool forkJoinPool() {
        assert singleton != null;
        return singleton.forkJoinPool;
    }

    public static EventLoopGroup eventLoopGroup() {
        assert singleton != null;
        return singleton.eventLoopGroup;
    }
}
