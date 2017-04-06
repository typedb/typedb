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

package ai.grakn.engine.loader;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.QueryBuilder;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static ai.grakn.engine.GraknEngineConfig.LOADER_REPEAT_COMMITS;
import static ai.grakn.util.ErrorMessage.FAILED_VALIDATION;
import static ai.grakn.util.ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.TASK_LOADER_INSERTS;
import static java.util.stream.Collectors.toList;

/**
 * Task that will load data into a graph. It uses the engine running on the
 * engine executing the task.
 *
 * The task will then submit all modified concepts for post processing.
 *
 * @author Alexandra Orth
 */
public class LoaderTask implements BackgroundTask {

    private static final Logger LOG = LoggerFactory.getLogger(LoaderTask.class);
    private static final int repeatCommits = GraknEngineConfig.getInstance().getPropertyAsInt(LOADER_REPEAT_COMMITS);
    private final QueryBuilder builder = Graql.withoutGraph().infer(false);

    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        attemptInsertions(
                getKeyspace(configuration),
                getInserts(configuration));
        return true;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Loader task cannot be paused");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException("Loader task cannot be resumed");
    }

    private void attemptInsertions(String keyspace, Collection<InsertQuery> inserts) {
        try(GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.BATCH)) {
            for (int i = 0; i < repeatCommits; i++) {
                if(insertQueriesInOneTransaction(graph, inserts)){
                    return;
                }
            }

            throwException("Could not insert");
        }
    }

    /**
     * Insert the given queries into the given graph. Return if the operation was successfully completed.
     * @param graph grakn graph in which to insert the data
     * @param inserts graql queries to insert into the graph
     * @return true if the data was inserted, false otherwise
     */
    private boolean insertQueriesInOneTransaction(GraknGraph graph, Collection<InsertQuery> inserts) {

        try {
            graph.showImplicitConcepts(true);

            inserts.forEach(q -> q.withGraph(graph).execute());

            // commit the transaction
            graph.admin().commit(EngineCacheProvider.getCache());
        } catch (GraknValidationException e) {
            //If it's a validation exception there is no point in re-trying
            throwException(FAILED_VALIDATION.getMessage(e.getMessage()), inserts);
        } catch (IllegalArgumentException e){
            throwException(ILLEGAL_ARGUMENT_EXCEPTION.getMessage(e.getMessage()), inserts);
        } catch (Throwable throwable){
            handleError(throwable, 1);
            return false;
        }

        return true;
    }

    /**
     * Throw a RuntimeException with the given message
     * @param message cause of the error
     */
    private void throwException(String message){
        throwException(message, Collections.emptyList());
    }

    /**
     * Throw a RuntimeException with the given information
     * @param message cause of the error
     * @param inserts insert queries that caused the error
     */
    private void throwException(String message, Collection<InsertQuery> inserts){
        throw new RuntimeException(message + inserts);
    }

    /**
     * Log the exception and sleep
     * @param e exception to log
     * @param i amount of time to sleep
     */
    private void handleError(Throwable e, long i) {
        LOG.error("Caught exception ", e);
        try {
            Thread.sleep((i + 2) * 1000);
        } catch (InterruptedException e1) {
            LOG.error("Caught exception ", e1);
        }
    }

    /**
     * Extract insert queries from a configuration object
     * @param configuration JSONObject containing configuration
     * @return insert queries from the configuration
     */
    private Collection<InsertQuery> getInserts(Json configuration){
        if(configuration.has(TASK_LOADER_INSERTS)){
            List<String> inserts = new ArrayList<>();
            configuration.at(TASK_LOADER_INSERTS).asJsonList().forEach(i -> inserts.add(i.asString()));

            return inserts.stream()
                    .map(builder::<InsertQuery>parse)
                    .collect(toList());
        }

        throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXCEPTION.getMessage("No inserts", configuration));
    }

    /**
     * Extract the keyspace from a configuration object
     * @param configuration JSONObject containing configuration
     * @return keyspace from the configuration
     */
    private String getKeyspace(Json configuration){
        if(configuration.has(KEYSPACE)){
            return configuration.at(KEYSPACE).asString();
        }

        //TODO default graph name
        throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXCEPTION.getMessage("No keyspace", configuration));
    }
}
