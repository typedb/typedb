/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.loader;

import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graql.InsertQuery;
import ai.grakn.util.ErrorMessage;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.Consumer;

import static ai.grakn.engine.util.ConfigProperties.LOADER_REPEAT_COMMITS;

/**
 * Task that will load data into the graph
 */
public class LoaderTask implements BackgroundTask {

    final Logger LOG = LoggerFactory.getLogger(Loader.class);
    private static Cache cache = Cache.getInstance();

    private static int repeatCommits = ConfigProperties.getInstance().getPropertyAsInt(LOADER_REPEAT_COMMITS);

    private Collection<InsertQuery> inserts;
    private String keyspace;

    public LoaderTask(Collection<InsertQuery> inserts, String keyspace) {
        this.inserts = inserts;
        this.keyspace = keyspace;
    }

    @Override
    public void start(Consumer<String> saveCheckpoint, JSONObject configuration) {
        attemptInsertionsMultipleTimes();
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

    private void attemptInsertionsMultipleTimes() {
        GraknGraph graph = GraphFactory.getInstance().getGraph(keyspace);
        try {
            for (int i = 0; i < repeatCommits; i++) {
                if(insertQueriesInOneTransaction(graph)){
                    return;
                }
            }

            throwException("Could not insert");
        } finally {
            graph.close();
            try {
                ((AbstractGraknGraph) graph).getTinkerPopGraph().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean insertQueriesInOneTransaction(GraknGraph graph) {

        try {
            // execute each of the insert queries
            inserts.forEach(q -> q.withGraph(graph).execute());

            // commit the transaction
            graph.commit();

            cache.addJobCasting(keyspace, ((AbstractGraknGraph) graph).getModifiedCastingIds());
            cache.addJobResource(keyspace, ((AbstractGraknGraph) graph).getModifiedCastingIds());
        } catch (GraknValidationException e) {
            //If it's a validation exception there is no point in re-trying
            throwException(ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()));
        } catch (IllegalArgumentException e){
            throwException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION.getMessage(e.getMessage()));
        } catch (Throwable throwable){
            handleError(throwable, 1);
            return false;
        }

        return true;
    }

    private void throwException(String message){
        message += inserts;
        throw new RuntimeException(message);
    }

    protected void handleError(Throwable e, int i) {
        LOG.error("Caught exception ", e);
        try {
            Thread.sleep((i + 2) * 1000);
        } catch (InterruptedException e1) {
            LOG.error("Caught exception ", e1);
        }
    }
}
