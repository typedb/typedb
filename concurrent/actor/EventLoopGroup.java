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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class EventLoopGroup {
    private final EventLoop[] eventLoops;
    private int nextIndex;

    public EventLoopGroup(int threadCount, String prefix) {
        this(threadCount, prefix, System::currentTimeMillis, ThreadLocalRandom.current());
    }

    public EventLoopGroup(int threadCount, String prefix, Supplier<Long> clock, Random random) {
        NamedThreadFactory threadFactory = new NamedThreadFactory(prefix);
        eventLoops = new EventLoop[threadCount];
        for (int i = 0; i < threadCount; i++) {
            eventLoops[i] = new EventLoop(threadFactory, clock, random);
        }
        nextIndex = 0;
    }

    public synchronized EventLoop assignEventLoop() {
        EventLoop eventLoop = eventLoops[nextIndex];
        nextIndex = (nextIndex + 1) % eventLoops.length;
        return eventLoop;
    }

    public synchronized void await() throws InterruptedException {
        for (int i = 0; i < eventLoops.length; i++) {
            eventLoops[i].await();
        }
    }

    public synchronized void stop() throws InterruptedException {
        for (int i = 0; i < eventLoops.length; i++) {
            eventLoops[i].stop();
        }
    }
}
