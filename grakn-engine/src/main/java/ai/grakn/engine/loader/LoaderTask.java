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
import ai.grakn.engine.postprocessing.AbstractGraphMutationTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.QueryBuilder;

import java.util.Collection;
import java.util.function.Consumer;

import static ai.grakn.util.ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION;
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
public class LoaderTask extends AbstractGraphMutationTask {

    private final QueryBuilder builder = Graql.withoutGraph().infer(false);

    @Override
    public boolean runGraphMutatingTask(GraknGraph graph, Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration) {
        return insertQueriesInOneTransaction(graph, getInserts(configuration));
    }

    @Override
    public boolean stop() {
        return false;
    }

    /**
     * Insert the given queries into the given graph. Return if the operation was successfully completed.
     * @param graph grakn graph in which to insert the data
     * @param inserts graql queries to insert into the graph
     * @return true if the data was inserted, false otherwise
     */
    private boolean insertQueriesInOneTransaction(GraknGraph graph, Collection<InsertQuery> inserts) {
        graph.showImplicitConcepts(true);

        inserts.forEach(q -> q.withGraph(graph).execute());

        //TODO This commit uses the rest API, it shouldn't
        graph.commit();

        return true;
    }

    /**
     * Extract insert queries from a configuration object
     * @param configuration JSONObject containing configuration
     * @return insert queries from the configuration
     */
    private Collection<InsertQuery> getInserts(TaskConfiguration configuration){
        if(configuration.json().has(TASK_LOADER_INSERTS)){
            return configuration.json().at(TASK_LOADER_INSERTS).asJsonList()
                    .stream()
                    .map(j -> j.asString())
                    .map(builder::<InsertQuery>parse)
                    .collect(toList());
        }

        throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXCEPTION.getMessage("No inserts", configuration));
    }
}
