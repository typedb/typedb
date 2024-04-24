/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concurrent.actor;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@ThreadSafe
public class ActorExecutorGroup {

    private final ActorExecutor[] executors;
    private final AtomicInteger nextIndex;

    public ActorExecutorGroup(int size, ThreadFactory threadFactory) {
        this(size, threadFactory, System::currentTimeMillis);
    }

    public ActorExecutorGroup(int size, ThreadFactory threadFactory, Supplier<Long> clock) {
        executors = new ActorExecutor[size];
        for (int i = 0; i < size; i++) executors[i] = new ActorExecutor(threadFactory, clock);
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
