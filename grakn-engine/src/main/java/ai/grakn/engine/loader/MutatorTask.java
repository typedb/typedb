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
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.postprocessing.GraphMutators;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.util.REST;
import com.codahale.metrics.Timer.Context;
import mjson.Json;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION;
import static ai.grakn.util.ErrorMessage.READ_ONLY_QUERY;
import static ai.grakn.util.REST.Request.TASK_LOADER_MUTATIONS;
import static com.codahale.metrics.MetricRegistry.name;

/**
 * Task that will mutate data in a graph. It uses the engine running on the
 * engine executing the task.
 *
 * The task will then submit all modified concepts for post processing.
 *
 * @author Alexandra Orth
 */
public class MutatorTask extends BackgroundTask {

    private final QueryBuilder builder = Graql.withoutGraph().infer(false);

    @Override
    public boolean start() {
        Collection<Query> inserts = getInserts(configuration());
        metricRegistry().histogram(name(MutatorTask.class, "jobs")).update(inserts.size());
        String keyspace = configuration().json().at(REST.Request.KEYSPACE).asString();
        int maxRetry = engineConfiguration().getPropertyAsInt(GraknEngineConfig.LOADER_REPEAT_COMMITS);

        GraphMutators.runBatchMutationWithRetry(factory(), keyspace, maxRetry, (graph) ->
                insertQueriesInOneTransaction(graph, inserts)
        );

        return true;
    }

    /**
     * Execute the given queries against the given graph. Return if the operation was successfully completed.
     * @param graph grakn graph in which to insert the data
     * @param inserts graql queries to insert into the graph
     * @return true if the data was inserted, false otherwise
     */
    private boolean insertQueriesInOneTransaction(GraknGraph graph, Collection<Query> inserts) {
        try(Context context = metricRegistry().timer(name(MutatorTask.class, "execution")).time()) {
            if (inserts.isEmpty()) {
                metricRegistry().meter(name(MutatorTask.class, "empty")).mark();
                return false;
            } else {
                inserts.forEach(q -> {
                    try(Context contextSingle = metricRegistry().timer(name(MutatorTask.class, "execution-single")).time()){
                        q.withGraph(graph).execute();
                    }
                });

                Optional<String> result = graph.admin().commitNoLogs();
                if(result.isPresent()){ // Submit more tasks if commit resulted in created commit logs
                    String logs = result.get();
                    addTask(PostProcessingTask.createTask(this.getClass(), engineConfiguration()
                                    .getPropertyAsInt(GraknEngineConfig.POST_PROCESSING_TASK_DELAY)),
                            PostProcessingTask.createConfig(graph.getKeyspace(), logs));
                    addTask(UpdatingInstanceCountTask.createTask(this.getClass()),
                            UpdatingInstanceCountTask.createConfig(graph.getKeyspace(), logs));
                }
                return true;
            }
        }
    }


    /**
     * Extract mutate queries from a configuration object
     * @param configuration JSONObject containing configuration
     * @return graql queries from the configuration
     */
    private Collection<Query> getInserts(TaskConfiguration configuration){
        if(configuration.json().has(TASK_LOADER_MUTATIONS)){
            return configuration.json().at(TASK_LOADER_MUTATIONS).asJsonList().stream()
                    .map(Json::asString)
                    .map(builder::<Query<?>>parse)
                    .map(query -> {
                        if (query.isReadOnly()) {
                            throw new IllegalArgumentException(READ_ONLY_QUERY.getMessage(query.toString()));
                        }
                        return query;
                    })
                    .collect(Collectors.toList());
        }

        throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXCEPTION.getMessage("No inserts", configuration));
    }
}
