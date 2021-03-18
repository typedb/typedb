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

package grakn.core.concurrent.executor;

import grakn.common.concurrent.NamedThreadFactory;
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
            if (executors[i].getQueue().size() < smallest) {
                smallest = executors[i].getQueue().size();
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
