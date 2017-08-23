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

package ai.grakn.engine.tasks;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.TaskCheckpoint;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskSubmitter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Interface which all tasks that wish to be scheduled for later execution as background tasks must implement.
 *
 * @author Denis Lobanov
 */
public abstract class BackgroundTask {

    private @Nullable
    TaskSubmitter taskSubmitter = null;
    private @Nullable
    TaskConfiguration configuration = null;
    private @Nullable Consumer<TaskCheckpoint> saveCheckpoint = null;
    private @Nullable GraknEngineConfig engineConfig = null;
    private @Nullable
    EngineGraknTxFactory factory = null;
    private @Nullable RedisCountStorage redis = null;
    private @Nullable MetricRegistry metricRegistry = null;
    private @Nullable LockProvider lockProvider = null;

    /**
     * Initialize the {@link BackgroundTask}. This must be called prior to any other call to {@link BackgroundTask}.
     *
     * @param saveCheckpoint Consumer<String> which can be called at any time to save a state checkpoint that would allow
     *                       the task to resume from this point should it crash.
     * @param configuration  The configuration needed to execute the task
     * @param taskSubmitter  Allows followup tasks to be submitted for processing
     * @param metricRegistry Metric registry
     */
    public final void initialize(
            Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration,
            TaskSubmitter taskSubmitter, GraknEngineConfig engineConfig, RedisCountStorage redis,
            EngineGraknTxFactory factory, LockProvider lockProvider, MetricRegistry metricRegistry)  {
        this.configuration = configuration;
        this.taskSubmitter = taskSubmitter;
        this.saveCheckpoint = saveCheckpoint;
        this.engineConfig = engineConfig;
        this.redis = redis;
        this.lockProvider = lockProvider;
        this.metricRegistry = metricRegistry;
        this.factory = factory;
    }

    /**
     * Called to start execution of the task, may be called on a newly scheduled or previously stopped task.
     * @return true if the task successfully completed, or false if it was stopped.
     */
    public abstract boolean start();

    /**
     * Called to stop execution of the task, may be called on a running or paused task.
     * Task should stop gracefully.
     * <p>
     * This implementation always throws {@link UnsupportedOperationException}.
     *
     * @return true if the task was successfully stopped, or false if it could not be stopped.
     *
     * @throws UnsupportedOperationException if stopping the task is not supported
     *
     * TODO: Should we allow start() to be called after stop()?
     */
    public boolean stop() {
        throw new UnsupportedOperationException(this.getClass().getName() + " task cannot be stopped while in progress");
    }

    /**
     * Called to suspend the execution of a currently running task. The object may be destroyed after this call.
     * <p>
     * This implementation always throws {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException if pausing the task is not supported
     *
     * TODO: stop running
     */
    public void pause() {
        throw new UnsupportedOperationException(this.getClass().getName() + " task cannot be paused");
    }

    /**
     * This method may be called when resuming from a paused state or recovering from a crash or failure of any kind.
     * <p>
     * This implementation always throws {@link UnsupportedOperationException}.
     *
     * @param lastCheckpoint The last checkpoint as sent to saveCheckpoint.
     *
     * @throws UnsupportedOperationException if resuming the task is not supported
     */
    public boolean resume(TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException(this.getClass().getName() + " task cannot be resumed");
    }

    public final void saveCheckpoint(TaskCheckpoint checkpoint) {
        Preconditions.checkNotNull(saveCheckpoint, "BackgroundTask#initialise must be called before saving checkpoints");
        saveCheckpoint.accept(checkpoint);
    }

    /**
     * Submit a new task for execution
     * @param taskState state describing the task
     */
    public final void addTask(TaskState taskState, TaskConfiguration configuration) {
        Preconditions.checkNotNull(taskSubmitter, "BackgroundTask#initialise must be called before adding tasks");
        taskSubmitter.addTask(taskState, configuration);
    }

    /**
     * Get the configuration needed to execute the task
     */
    public final TaskConfiguration configuration() {
        Preconditions.checkNotNull(configuration, "BackgroundTask#initialise must be called before retrieving configuration");
        return configuration;
    }

    public final GraknEngineConfig engineConfiguration() {
        Preconditions.checkNotNull(engineConfig, "BackgroundTask#initialise must be called before retrieving engine configuration");
        return engineConfig;
    }

    public final RedisCountStorage redis() {
        Preconditions.checkNotNull(redis, "BackgroundTask#initialise must be called before retrieving redis connection");
        return redis;
    }

    public final EngineGraknTxFactory factory(){
        Preconditions.checkNotNull(factory, "BackgroundTask#initialise must be called before retrieving the engine factory");
        return factory;
    }

    public final MetricRegistry metricRegistry() {
        Preconditions.checkNotNull(metricRegistry, "BackgroundTask#initialise must be called before retrieving metrics registry");
        return metricRegistry;
    }

    public LockProvider getLockProvider() {
        Preconditions.checkNotNull(lockProvider, "Lock provider was null, possible race condition in initialisation");
        return lockProvider;
    }
}
