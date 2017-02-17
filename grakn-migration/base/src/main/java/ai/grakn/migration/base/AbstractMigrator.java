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

package ai.grakn.migration.base;

import ai.grakn.engine.TaskStatus;
import ai.grakn.client.LoaderClient;
import ai.grakn.exception.GraqlTemplateParsingException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.graql.macro.Macro;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * <p>
 *     Abstract migrator class containing methods and functionality needed by
 *     all extending migrator classes.
 * </p>
 *
 * @author alexandraorth
 */
public abstract class AbstractMigrator implements Migrator {

    private final static AtomicInteger numberQueriedSubmitted = new AtomicInteger(0);
    private final static Logger LOG = LoggerFactory.getLogger(AbstractMigrator.class);
    private final QueryBuilderImpl queryBuilder = (QueryBuilderImpl) Graql.withoutGraph().infer(false);
    public static final int BATCH_SIZE = 25;
    public static final int ACTIVE_TASKS = 25;
    public static final boolean RETRY = false;

    /**
     * Register a macro to use in templating
     */
    public AbstractMigrator registerMacro(Macro macro){
        queryBuilder.registerMacro(macro);
        return this;
    }

    /**
     * Migrate data constrained by this migrator using a loader configured
     * by the provided parameters.
     *
     * Uses the default batch size and number of active tasks.
     *
     * @param uri Uri where one instance of Grakn Engine is running
     * @param keyspace The name of the keyspace where the data should be persisted
     */
    public void load(String uri, String keyspace) {
        load(uri, keyspace, AbstractMigrator.BATCH_SIZE, AbstractMigrator.ACTIVE_TASKS, AbstractMigrator.RETRY);
    }

    /**
     * Migrate data constrained by this migrator using a loader configured
     * by the provided parameters.
     *
     * @param uri Uri where one instance of Grakn Engine is running
     * @param keyspace The name of the keyspace where the data should be persisted
     * @param batchSize The number of queries to execute in one transaction. Default is 25.
     * @param numberActiveTasks Number of tasks running on the server at any one time. Consider this a safeguard
     *                  to bot the system load. Default is 25.
     * @param retry If the Loader should continue attempt to send tasks when Engine is not available
     */
    public void load(String uri, String keyspace, int batchSize, int numberActiveTasks, boolean retry){
        LoaderClient loader = new LoaderClient(keyspace, uri, recordMigrationStates());
        loader.setBatchSize(batchSize);
        loader.setNumberActiveTasks(numberActiveTasks);
        loader.setRetryPolicy(retry);

        migrate().forEach(q -> {
            numberQueriedSubmitted.incrementAndGet();
            loader.add(q);
        });
        loader.waitToFinish();
    }

    /**
     * @param template a string representing a templated graql query
     * @param data data used in the template
     * @return an insert query
     */
    protected Optional<InsertQuery> template(String template, Map<String, Object> data){
        try {
            return Optional.of(queryBuilder.parseTemplate(template, data));
        } catch (GraqlTemplateParsingException e){
            LOG.warn("Query was not sent to loader- " + e.getMessage());
            LOG.warn("See the Grakn engine logs for more detail about loading status and any resulting stacktraces");
        }

        return Optional.empty();
    }

    /**
     * Partition a stream into a stream of collections, each with batchSize elements.
     * @param iterator Iterator to partition
     * @param <T> Type of values of iterator
     * @return Stream over a collection that are each of batchSize
     */
    protected <T> Stream<T> stream(Iterator<T> iterator){
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);

    }

    /**
     * Test if an object is a valid Grakn value
     * @param value object to check
     * @return if the value is valid
     */
    protected boolean validValue(Object value){
        return value != null;
    }

    /**
     * Consumer function which will operate on the results of the loader
     * and print the current status of the migrator.
     *
     * @return function that operates on completion of a task
     */
    private Consumer<Json> recordMigrationStates(){
        return (Json json) -> {
            TaskStatus status = TaskStatus.valueOf(json.at("status").asString());
            Json configuration = Json.read(json.at("configuration").asString());
            int batchNumber = configuration.at("batchNumber").asInteger();

            LOG.info("Status of finished batch: " + status);
            LOG.info("Batches finished: " + batchNumber);
            LOG.info("Number Queries finished: " + numberQueriedSubmitted.get());
        };
    }
}
