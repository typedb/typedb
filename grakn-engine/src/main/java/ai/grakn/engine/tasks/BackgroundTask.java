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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.tasks;

import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.postprocessing.IndexPostProcessor;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Interface which all tasks that wish to be scheduled for later execution as background tasks must implement.
 *
 * @author Denis Lobanov
 */
public abstract class BackgroundTask {

    private @Nullable
    TaskConfiguration configuration = null;
    private @Nullable
    GraknConfig engineConfig = null;
    private @Nullable
    EngineGraknTxFactory factory = null;
    private @Nullable MetricRegistry metricRegistry = null;
    private @Nullable IndexPostProcessor postProcessor = null;

    /**
     * Initialize the {@link BackgroundTask}. This must be called prior to any other call to {@link BackgroundTask}.
     *
     * @param configuration  The configuration needed to execute the task
     * @param metricRegistry Metric registry
     */
    public final void initialize(
            TaskConfiguration configuration,
            GraknConfig engineConfig,
            EngineGraknTxFactory factory, MetricRegistry metricRegistry, IndexPostProcessor postProcessor)  {
        this.configuration = configuration;
        this.engineConfig = engineConfig;
        this.metricRegistry = metricRegistry;
        this.factory = factory;
        this.postProcessor = postProcessor;
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

    public final TaskConfiguration configuration() {
        return defaultNullCheck(configuration);
    }

    public final GraknConfig engineConfiguration() {
        return defaultNullCheck(engineConfig);
    }

    public final EngineGraknTxFactory factory(){
        return defaultNullCheck(factory);
    }

    public final MetricRegistry metricRegistry() {
        return defaultNullCheck(metricRegistry);
    }

    public final IndexPostProcessor postProcessor(){
        return defaultNullCheck(postProcessor);
    }

    private static <X> X defaultNullCheck(X someThing){
        Preconditions.checkNotNull(someThing, String.format("BackgroundTask#initialise must be called before retrieving {%s}", someThing.getClass().getSimpleName()));
        return someThing;
    }
}
