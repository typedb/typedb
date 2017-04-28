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

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.util.ErrorMessage;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.engine.GraknEngineConfig.LOADER_REPEAT_COMMITS;
import static ai.grakn.util.REST.Request.KEYSPACE;

/**
 * <p>
 *     Abstract task for executing a graph mutation task multiple times should a {@link GraknBackendException} occur.
 * </p>
 *
 * <p>
 *     This utility class is used to help with background tasks which need to mutate a graph
 * </p>
 *
 * @author alexandraorth, fppt
 */
public abstract class AbstractGraphMutationTask implements BackgroundTask {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractGraphMutationTask.class);
    private static final int MAX_RETRY = GraknEngineConfig.getInstance().getPropertyAsInt(LOADER_REPEAT_COMMITS);

    /**
     * Implementation should mutate the given graph using the given task configuration
     *
     * @param graph Graph object on which to perform mutations
     * @return Implementation of the graph mutating code
     */
    public abstract boolean runGraphMutatingTask(GraknGraph graph, Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration);

    /**
     *  Template method calls the abstract method {@link #runGraphMutatingTask(GraknGraph, Consumer, TaskConfiguration)}
     *
     *  May execute the method up to {@literal MAX_RETRIES} number of times if GraknBackendExceptions
     *  are thrown. Any other exception will cause the method to return.
     */
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration){
        String keyspace = configuration.json().at(KEYSPACE).asString();

        for(int retry = 0; retry < MAX_RETRY; retry++) {
            try(GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.BATCH))  {

                return runGraphMutatingTask(graph, saveCheckpoint, configuration);
            } catch (GraknBackendException e){
                // retry...
                LOG.debug(ErrorMessage.GRAPH_MUTATION_ERROR.getMessage(e.getMessage()), e);
            } catch(GraknValidationException e){
                throw new RuntimeException(ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()), e);
            } catch (Throwable t) {
                throw new RuntimeException(ErrorMessage.GRAPH_MUTATION_ERROR.getMessage(t.getMessage()), t);
            }

            performRetry(retry);
        }

        throw new RuntimeException(ErrorMessage.UNABLE_TO_MUTATE_GRAPH.getMessage(keyspace));
    }

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException("Graph mutation task cannot be stopped while in progress");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Graph mutation task cannot be paused");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        return false;
    }

    /**
     * Sleep the current thread for a random amount of time
     * @param retry Seed with which to calculate sleep time
     */
    private static void performRetry(int retry){
        double seed = 1.0 + (Math.random() * 5.0);
        double waitTime = (retry * 2.0)  + seed;
        LOG.debug(ErrorMessage.BACK_OFF_RETRY.getMessage(waitTime));

        try {
            Thread.sleep((long) Math.ceil(waitTime * 1000));
        } catch (InterruptedException e1) {
            LOG.error("Exception",e1);
        }
    }
}