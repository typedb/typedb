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

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.engine.postprocessing.GraknTxMutators;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.util.REST;
import com.codahale.metrics.Timer.Context;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(MutatorTask.class);

    private final QueryBuilder builder = Graql.withoutGraph().infer(false);

    @Override
    public boolean start() {
        Collection<Query> inserts = getInserts(configuration());
        metricRegistry().histogram(name(MutatorTask.class, "jobs")).update(inserts.size());
        Keyspace keyspace = Keyspace.of(configuration().json().at(REST.Request.KEYSPACE).asString());
        int maxRetry = engineConfiguration().getProperty(GraknConfigKey.LOADER_REPEAT_COMMITS);

        GraknTxMutators.runBatchMutationWithRetry(factory(), keyspace, maxRetry, (graph) ->
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
    private boolean insertQueriesInOneTransaction(GraknTx graph, Collection<Query> inserts) {
        try(Context context = metricRegistry().timer(name(MutatorTask.class, "execution")).time()) {
            if (inserts.isEmpty()) {
                metricRegistry().meter(name(MutatorTask.class, "empty")).mark();
                return false;
            } else {
                inserts.forEach(q -> {
                    try(Context contextSingle = metricRegistry().timer(name(MutatorTask.class, "execution-single")).time()){
                        q.withTx(graph).execute();
                    } catch (Exception e) {
                        LOG.error("Error while executing insert for query: \n{}\nError: {}", q, e.getMessage());
                        throw e;
                    }
                });

                Optional<String> result = graph.admin().commitSubmitNoLogs();
                if(result.isPresent()){ // Submit more tasks if commit resulted in created commit logs
                    String logs = result.get();
                    addTask(PostProcessingTask.createTask(this.getClass(), engineConfiguration()
                                    .getProperty(GraknConfigKey.POST_PROCESSING_TASK_DELAY)),
                            PostProcessingTask.createConfig(graph.keyspace(), logs));

                    postProcessor().updateCounts(graph.keyspace(), Json.read(logs));
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
