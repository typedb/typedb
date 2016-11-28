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
import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory
;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static ai.grakn.engine.util.ConfigProperties.LOADER_REPEAT_COMMITS;

import static ai.grakn.util.ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION;
import static ai.grakn.util.ErrorMessage.FAILED_VALIDATION;
import static ai.grakn.util.REST.Request.TASK_LOADER_INSERTS;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;

import static java.util.stream.Collectors.toList;

/**
 * Task that will load data into the graph
 */
public class LoaderTask implements BackgroundTask {

    private static final Logger LOG = LoggerFactory.getLogger(Loader.class);
    private static final Cache cache = Cache.getInstance();

    private static int repeatCommits = ConfigProperties.getInstance().getPropertyAsInt(LOADER_REPEAT_COMMITS);

    @Override
    public void start(Consumer<String> saveCheckpoint, JSONObject configuration) {
        attemptInsertions(
                getKeyspace(configuration),
                getInserts(configuration));
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("Loader task cannot be stopped");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Loader task cannot be paused");
    }

    @Override
    public void resume(Consumer<String> saveCheckpoint, String lastCheckpoint) {
        throw new UnsupportedOperationException("Loader task cannot be resumed");
    }

    private void attemptInsertions(String keyspace, Collection<InsertQuery> inserts) {
        try(GraknGraph graph = GraphFactory.getInstance().getGraph(keyspace)) {
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
            // execute each of the insert queries
            inserts.forEach(q -> q.withGraph(graph).execute());

            // commit the transaction
            graph.commit();

            cache.addJobCasting(graph.getKeyspace(), ((AbstractGraknGraph) graph).getConceptLog().getModifiedCastingIds());
            cache.addJobResource(graph.getKeyspace(), ((AbstractGraknGraph) graph).getConceptLog().getModifiedCastingIds());
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
    private void handleError(Throwable e, int i) {
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
    private Collection<InsertQuery> getInserts(JSONObject configuration){
        if(configuration.has(TASK_LOADER_INSERTS)){
            List<String> inserts = new ArrayList<>();
            configuration.getJSONArray(TASK_LOADER_INSERTS).forEach(i -> inserts.add((String) i));

            return inserts.stream()
                    .map(Graql::parse)
                    .map(p -> (InsertQuery) p)
                    .collect(toList());
        }

        throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXCEPTION.getMessage("No inserts", configuration));
    }

    /**
     * Extract the keyspace from a configuration object
     * @param configuration JSONObject containing configuration
     * @return keyspace from the configuration
     */
    private String getKeyspace(JSONObject configuration){
        if(configuration.has(KEYSPACE_PARAM)){
            return configuration.getString(KEYSPACE_PARAM);
        }

        //TODO default graph name
        throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXCEPTION.getMessage("No keyspace", configuration));
    }
}
