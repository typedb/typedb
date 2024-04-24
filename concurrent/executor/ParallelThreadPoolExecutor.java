/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concurrent.executor;

import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ParallelThreadPoolExecutor implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelThreadPoolExecutor.class);

    private final ThreadPoolExecutor[] executors;

    public ParallelThreadPoolExecutor(int executors, NamedThreadFactory threadFactory) {
        this.executors = new ThreadPoolExecutor[executors];
        for (int i = 0; i < executors; i++) {
            this.executors[i] = new ThreadPoolExecutor(1, 1, 0, MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
        }
    }

    private ThreadPoolExecutor next() {
        int next = 0, smallest = Integer.MAX_VALUE;
        for (int i = 0; i < executors.length; i++) {
            int tasks = executors[i].getQueue().size() + executors[i].getActiveCount();
            if (tasks < smallest) {
                smallest = tasks;
                next = i;
            }
        }
        return executors[next];
    }

    @Override
    public void execute(@Nonnull Runnable runnable) {
        next().execute(runnable);
    }
}
