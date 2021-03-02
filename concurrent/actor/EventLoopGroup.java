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
 */

package grakn.core.concurrent.actor;

import grakn.common.concurrent.NamedThreadFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

// TODO: Make this class extend java.util.concurrent.ExecutorService
@ThreadSafe
public class EventLoopGroup {

    private final EventLoop[] eventLoops;
    private final AtomicInteger nextIndex;

    public EventLoopGroup(int threadCount) {
        this(threadCount, new NamedThreadFactory("grakn-core-elg"));
    }

    public EventLoopGroup(int threadCount, ThreadFactory threadFactory) {
        this(threadCount, threadFactory, System::currentTimeMillis);
    }

    public EventLoopGroup(int threadCount, ThreadFactory threadFactory, Supplier<Long> clock) {
        eventLoops = new EventLoop[threadCount];
        for (int i = 0; i < threadCount; i++) {
            eventLoops[i] = new EventLoop(threadFactory, clock);
        }
        nextIndex = new AtomicInteger(0);
    }

    public EventLoop nextEventLoop() {
        return eventLoops[nextIndexAndIncrement()];
    }

    public void await() throws InterruptedException {
        for (int i = 0; i < eventLoops.length; i++) {
            eventLoops[i].await();
        }
    }

    public void stop() throws InterruptedException {
        for (int i = 0; i < eventLoops.length; i++) {
            eventLoops[i].stop();
        }
    }

    private int nextIndexAndIncrement() {
        return nextIndex.getAndUpdate(index -> (index + 1) % eventLoops.length);
    }
}
