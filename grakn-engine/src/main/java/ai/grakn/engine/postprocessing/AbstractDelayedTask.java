/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.postprocessing;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.engine.GraknEngineConfig.POST_PROCESSING_DELAY;
import static java.time.Instant.now;

/**
 * <p>
 *     Task that controls when delayed tasks begin,
 * </p>
 *
 * <p>
 *     This utility class is used to help with background tasks which need to wait for a time before running.
 *     This task should be scheduled as recurring- at each execution it will evaluate if enough time has
 *     passed for it to start running.
 * </p>
 *
 * @author alexandraorth, fppt
 */
public abstract class AbstractDelayedTask implements BackgroundTask {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDelayedTask.class);
    private static final String DELAYED_RUN = "Post processing Job should run [{}] time elapsed [{}]";
    private long maxTimeLapse = GraknEngineConfig.getInstance().getPropertyAsLong(POST_PROCESSING_DELAY);

    //TODO MAJOR Make this distributed in distributed environment
    public static final AtomicLong lastDelayedTaskCreated = new AtomicLong(System.currentTimeMillis());

    /**
     * Task that will call the {@link AbstractDelayedTask#runDelayedTask} method after the specified delay has passed.
     * @return False if the task should be marked as stopped because the task has run, true otherwise
     */
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration) {
        Instant lastJobAdded = Instant.ofEpochMilli(lastDelayedTaskCreated.get());
        long timeElapsed = Duration.between(lastJobAdded, now()).toMillis();
        boolean delayedTaskShouldRun = timeElapsed > maxTimeLapse;

        LOG.info(DELAYED_RUN, delayedTaskShouldRun, timeElapsed);

        // Only try to run if enough time has passed
        if(timeElapsed >= maxTimeLapse){
            return runDelayedTask(saveCheckpoint, configuration);
        }

        return true;
    }

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException("Delayed task cannot be stopped while in progress");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Delayed task cannot be paused");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException("Delayed task cannot be resumed");
    }

    public void setTimeLapse(long time){
        this.maxTimeLapse = time;
    }

    /**
     * Functionality to be executed when the delay is complete
     *
     * @return False if the task should be marked as stopped, true otherwise
     */
    abstract boolean runDelayedTask(Consumer<TaskCheckpoint> checkpointConsumer, TaskConfiguration configuration);

}
