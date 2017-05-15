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
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.util.ErrorMessage;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.engine.GraknEngineConfig.LOADER_REPEAT_COMMITS;
import static ai.grakn.util.REST.Request.KEYSPACE;

/**
 * <p>
 *     Abstract class containing utilities for graph mutations
 * </p>
 *
 * <p>
 *     This utility class is used to help with background tasks which need to mutate a graph
 * </p>
 *
 * @author alexandraorth, fppt
 */
public abstract class GraphMutators {

    private static final Logger LOG = LoggerFactory.getLogger(GraphMutators.class);
    private static final int MAX_RETRY = GraknEngineConfig.getInstance().getPropertyAsInt(LOADER_REPEAT_COMMITS);

    /**
     *
     * @param configuration
     * @return
     */
    protected static String getKeyspace(TaskConfiguration configuration){
        return configuration.json().at(KEYSPACE).asString();
    }

    /**
     *
     *
     * @param configuration
     * @param mutatingFunction Function that accepts a graph object and will mutate the given graph
     */
    public static void runBatchMutationWithRetry(TaskConfiguration configuration, Consumer<GraknGraph> mutatingFunction){
        runGraphMutationWithRetry(configuration, GraknTxType.BATCH, mutatingFunction);
    }

    /**
     *
     * @param configuration
     * @param mutatingFunction Function that accepts a graph object and will mutate the given graph
     * @return
     */
    public static void runGraphMutationWithRetry(TaskConfiguration configuration, Consumer<GraknGraph> mutatingFunction){
        runGraphMutationWithRetry(configuration, GraknTxType.WRITE, mutatingFunction);
    }

    /**
     *
     * @param configuration
     * @param mutatingFunction Function that accepts a graph object and will mutate the given graph
     * @return
     */
    private static void runGraphMutationWithRetry(TaskConfiguration configuration, GraknTxType txType, Consumer<GraknGraph> mutatingFunction){
        String keyspace = getKeyspace(configuration);

        for(int retry = 0; retry < MAX_RETRY; retry++) {
            try(GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(keyspace, txType))  {

                mutatingFunction.accept(graph);

                return;
            } catch (GraknBackendException e){
                // retry...
                LOG.debug(ErrorMessage.GRAPH_MUTATION_ERROR.getMessage(e.getMessage()), e);
            }

            performRetry(retry);
        }

        throw new RuntimeException(ErrorMessage.UNABLE_TO_MUTATE_GRAPH.getMessage(keyspace));
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