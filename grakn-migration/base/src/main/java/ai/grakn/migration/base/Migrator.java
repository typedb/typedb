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

import ai.grakn.Keyspace;
import ai.grakn.client.BatchMutatorClient;
import ai.grakn.client.TaskResult;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryParser;
import ai.grakn.graql.macro.Macro;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * <p>
 *     Abstract migrator class containing methods and functionality needed by
 *     all extending migrator classes.
 * </p>
 *
 * @author alexandraorth
 */
public class Migrator {

    private final static AtomicInteger numberQueriesSubmitted = new AtomicInteger(0);
    private final static AtomicInteger numberBatchesCompleted = new AtomicInteger(0);

    private final static Logger LOG = LoggerFactory.getLogger(Migrator.class);
    private final QueryParser queryParser = Graql.withoutGraph().infer(false).parser();
    public static final int BATCH_SIZE = 25;
    public static final int ACTIVE_TASKS = 16;
    public static final int DEFAULT_MAX_RETRY = 1;

    private final String uri;
    private final Keyspace keyspace;
    private int batchSize;
    private long startTime;

    /**
     *
     * @param uri Uri where one instance of Grakn Engine is running
     * @param keyspace The {@link Keyspace} where the data should be persisted
     */
    private Migrator(String uri, Keyspace keyspace){
        this.uri = uri;
        this.keyspace = keyspace;
    }

    public static Migrator to(String uri, Keyspace keyspace){
        return new Migrator(uri, keyspace);
    }

    /**
     * Register a macro to use in templating
     */
    public Migrator registerMacro(Macro macro){
        queryParser.registerMacro(macro);
        return this;
    }

    /**
     * Migrate data constrained by this migrator using a loader configured
     * by the provided parameters.
     *
     * Uses the default batch size and number of active tasks.
     *
     * NOTE: Currently only used for testing purposes
     *
     * @param template
     * @param converter
     */
    public void load(String template, Stream<Map<String, Object>> converter) {
        load(template, converter, Migrator.BATCH_SIZE, Migrator.ACTIVE_TASKS, Migrator.DEFAULT_MAX_RETRY, true);
    }

    /**
     * Template the data and print to standard out.
     * @param template
     * @param converter
     */
    public void print(String template, Stream<Map<String, Object>> converter){
        converter.flatMap(d -> template(template, d))
                 .forEach(System.out::println);
    }

    /**
     * Migrate data constrained by this migrator using a loader configured
     * by the provided parameters.
     *
     * @param template
     * @param converter
     * @param batchSize The number of queries to execute in one transaction. Default is 25.
     * @param numberActiveTasks Number of tasks running on the server at any one time. Consider this a safeguard
     *                  to bot the system load. Default is 25.
     * @param retrySize If the Loader should continue attempt to send tasks when Engine is not available or an exception occurs
     */
    public void load(String template, Stream<Map<String, Object>> converter,
                     int batchSize, int numberActiveTasks, int retrySize, boolean debug){
        this.startTime = System.currentTimeMillis();
        this.batchSize = batchSize;

        BatchMutatorClient loader = new BatchMutatorClient(keyspace, uri, recordMigrationStates(), true, debug, retrySize);
        loader.setBatchSize(batchSize);
        loader.setNumberActiveTasks(numberActiveTasks);
        loader.setTaskCompletionConsumer(taskResult -> {
            String stackTrace = taskResult.getStackTrace();
            if (stackTrace != null && !stackTrace.isEmpty()) {
                if(debug){
                    throw GraknBackendException.migrationFailure(stackTrace);
                } else {
                    System.err.println(stackTrace);
                }
            }
        });

        converter
                .flatMap(d -> template(template, d))
                .forEach(q -> {
                    numberQueriesSubmitted.incrementAndGet();
                    loader.add(q);
                });
        loader.waitToFinish();
        loader.close();
    }

    /**
     * @param template a string representing a templated graql query
     * @param data data used in the template
     * @return an insert query
     */
    protected Stream<Query> template(String template, Map<String, Object> data){
        try {
            return queryParser.parseTemplate(template, data);
        } catch (GraqlSyntaxException e){
            LOG.warn("Query not sent to server: " + e.getMessage());
        }

        return Stream.empty();
    }

    /**
     * Consumer function which will operate on the results of the loader
     * and print the current status of the migrator.
     *
     * @return function that operates on completion of a task
     */
    private Consumer<TaskResult> recordMigrationStates(){
        return (TaskResult taskId) -> {
            numberBatchesCompleted.incrementAndGet();

            long timeElapsedSeconds = (System.currentTimeMillis() - startTime)/1000;
            long numberQueriesCompleted = numberBatchesCompleted.get() * batchSize;

            LOG.info(format("Number queries submitted: %s", numberQueriesSubmitted.get()));
            LOG.info(format("Number batches completed: %s", numberBatchesCompleted.get()));
            LOG.info(format("~Number queries completed: %s", numberQueriesCompleted));
            LOG.info(format("~Rate of completion (queries/second): %s", numberQueriesCompleted / timeElapsedSeconds));
        };
    }
}
