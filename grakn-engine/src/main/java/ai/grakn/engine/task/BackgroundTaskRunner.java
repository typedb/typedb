/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.task;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import ai.grakn.util.ErrorMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Helper class which wraps a {@link java.util.concurrent.ScheduledExecutorService} which runs periodically
 *     and executes {@link BackgroundTask}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class BackgroundTaskRunner implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BackgroundTaskRunner.class);

    private final Set<BackgroundTask> registeredTasks = new HashSet<>();
    private final ScheduledExecutorService threadPool;

    public BackgroundTaskRunner(GraknConfig graknConfig) {
        int numThread = graknConfig.getProperty(GraknConfigKey.NUM_BACKGROUND_THREADS);
        threadPool = Executors.newScheduledThreadPool(numThread);
    }

    /**
     *  Submit a {@link BackgroundTask} to run periodically
     *
     * @param backgroundTask The Background Task To Run Periodically
     */
    public void register(BackgroundTask backgroundTask){
        if(!registeredTasks.contains(backgroundTask)) {
            LOG.info("Registering a new background task.");
            registeredTasks.add(backgroundTask);
            threadPool.scheduleAtFixedRate(() -> {
                try {
                    backgroundTask.run();
                } catch (Exception e) {
                    LOG.error(ErrorMessage.BACKGROUND_TASK_UNHANDLED_EXCEPTION.getMessage(backgroundTask), e);
                }
            }, backgroundTask.period(), backgroundTask.period(), TimeUnit.SECONDS);

        }
    }

    @Override
    public void close(){
        registeredTasks.forEach(BackgroundTask::close);
        threadPool.shutdown();
    }

    /**
     * Returns the set of all tasks registered to run as {@link BackgroundTask}s.
     * These tasks are run periodically.
     *
     * @return the set of all tasks registered to run as {@link BackgroundTask}s
     */
    @VisibleForTesting
    public Set<BackgroundTask> tasks(){
        return ImmutableSet.copyOf(registeredTasks);
    }
}
