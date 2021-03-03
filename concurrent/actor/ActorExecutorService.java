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

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@ThreadSafe
public class ActorExecutorService {

    private final ActorExecutor[] executors;
    private final AtomicInteger nextIndex;

    public ActorExecutorService(int size, ThreadFactory threadFactory) {
        this(size, threadFactory, System::currentTimeMillis);
    }

    public ActorExecutorService(int size, ThreadFactory threadFactory, Supplier<Long> clock) {
        executors = new ActorExecutor[size];
        for (int i = 0; i < size; i++) {
            executors[i] = new ActorExecutor(threadFactory, clock);
        }
        nextIndex = new AtomicInteger(0);
    }

    ActorExecutor nextExecutor() {
        return executors[nextIndexAndIncrement()];
    }

    public void await() throws InterruptedException {
        for (int i = 0; i < executors.length; i++) {
            executors[i].await();
        }
    }

    public void stop() throws InterruptedException {
        for (int i = 0; i < executors.length; i++) {
            executors[i].stop();
        }
    }

    private int nextIndexAndIncrement() {
        return nextIndex.getAndUpdate(index -> (index + 1) % executors.length);
    }
}
