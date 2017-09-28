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

package ai.grakn.client;

import ai.grakn.Keyspace;
import ai.grakn.graql.Query;
import static ai.grakn.util.ConcurrencyUtil.all;
import static ai.grakn.util.ErrorMessage.READ_ONLY_QUERY;
import static ai.grakn.util.REST.Request.BATCH_NUMBER;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.TASK_LOADER_MUTATIONS;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import static java.util.stream.Collectors.toList;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to batch load qraql queries into Grakn that mutate the graph.
 *
 * Provides methods to batch load queries. Optionally can provide a consumer
 * that will execute when a batch finishes loading. BatchMutatorClient will block when the configured
 * resources are being used to execute tasks.
 *
 * @author alexandraorth
 */
public class BatchMutatorClient {

    private final Logger LOG = LoggerFactory.getLogger(BatchMutatorClient.class);


    // TODO Make configurable
    private static final Duration EXPIRE_TIME = Duration.ofMillis(200);
    private final int maxRetries;
    private final Set<CompletableFuture<Void>> futures;
    private final Collection<Query> queries;
    private final Keyspace keyspace;
    private final TaskClient taskClient;
    private final Timer addTimer;
    private final Timer batchSendToLoaderTimer;
    private final Timer batchSendToEngineTimer;
    private final Meter failureMeter;
    private final boolean debugOn;

    private long lastSentTimestamp;
    private Consumer<TaskResult> onCompletionOfTask;
    private AtomicInteger batchNumber;
    private int batchSize;
    private ExecutorService threadPool;

    public BatchMutatorClient(Keyspace keyspace, String uri, boolean debugOn, int maxRetries) {
        this(keyspace, uri, (TaskResult t) -> {}, true, debugOn, maxRetries);
    }

    public BatchMutatorClient(Keyspace keyspace, String uri, Consumer<TaskResult> onCompletionOfTask, boolean debugOn, int maxRetries) {
        this(keyspace, uri, onCompletionOfTask, false, debugOn, maxRetries);
    }

    public BatchMutatorClient(Keyspace keyspace, String uri, Consumer<TaskResult> onCompletionOfTask, boolean reportStats, boolean debugOn, int maxRetries) {
        this.keyspace = keyspace;
        this.queries = new ArrayList<>();
        this.futures = new HashSet<>();
        this.onCompletionOfTask = onCompletionOfTask;
        this.batchNumber = new AtomicInteger(0);
        this.debugOn = debugOn;
        this.maxRetries = maxRetries;
        this.lastSentTimestamp = 0L;
        // Some extra logic here since we don't provide a well formed URI by default
        if (uri.startsWith("http")) {
            try {
                URI parsedUri = new URI(uri);
                this.taskClient = TaskClient.of(parsedUri.getHost(), parsedUri.getPort());
            } catch (URISyntaxException e) {
                throw new RuntimeException("Could not parse given uri " + uri);
            }
        } else if (uri.contains(":")){
            String[] splitUri = uri.split(":");
            this.taskClient = TaskClient.of(splitUri[0], Integer.parseInt(splitUri[1]));
        } else {
            throw new RuntimeException("Invalid uri " + uri);
        }

        setBatchSize(25);
        setNumberActiveTasks(4);

        MetricRegistry metricRegistry = new MetricRegistry();
        batchSendToLoaderTimer = metricRegistry
                .timer(name(BatchMutatorClient.class, "batch_send_to_loader"));
        batchSendToEngineTimer = metricRegistry
                .timer(name(BatchMutatorClient.class, "batch_send_to_engine"));
        addTimer = metricRegistry
                .timer(name(BatchMutatorClient.class, "add"));
        failureMeter = metricRegistry
                .meter(name(BatchMutatorClient.class, "failure"));
        if (reportStats) {
            final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(MILLISECONDS)
                    .build();
            reporter.start(1, TimeUnit.MINUTES);
        }
    }

    /**
     * Provide a consumer function to execute upon task completion
     * @param onCompletionOfTask function applied to the last state of the task
     */
    public BatchMutatorClient setTaskCompletionConsumer(Consumer<TaskResult> onCompletionOfTask){
        this.onCompletionOfTask = onCompletionOfTask;
        return this;
    }

    /**
     * Set the size of the each transaction in terms of number of vars.
     * @param size number of vars in each transaction
     */
    public BatchMutatorClient setBatchSize(int size){
        this.batchSize = size;
        return this;
    }

    /**
     * Get the number of queries in each transaction
     * @return the batch size
     */
    public int getBatchSize(){
        return batchSize;
    }

    /**
     * Number of active tasks running on the server at any one time.
     * Consider this a safeguard on system load.
     *
     * The Loader {@link #add(Query)} method will block on the value of this field.
     *
     * @param size number of tasks to allow to run at any given time
     */
    public BatchMutatorClient setNumberActiveTasks(int size){
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("grakn-batch-mutator-%d").build();
        this.threadPool = Executors.newFixedThreadPool(size, namedThreadFactory);
        return this;
    }

    /**
     * Add an insert query to the queue.
     *
     * This method will block while the number of currently executing tasks
     * is equal to the set {@link #setNumberActiveTasks(int)}.
     * It will become unblocked as tasks are completed.
     *
     * @param query insert query to be executed
     */
    public void add(Query query){
        try (Context ignored = addTimer.time()) {
            if (query.isReadOnly()) {
                throw new IllegalArgumentException(READ_ONLY_QUERY.getMessage(query.toString()));
            }
            queries.add(query);
        }
        sendQueriesWhenBatchLargerThanValue(batchSize-1);
    }

    /**
     * Load any remaining batches in the queue.
     */
    private void flush(){
        sendQueriesWhenBatchLargerThanValue(0);
    }

    private void sendQueriesWhenBatchLargerThanValue(int value) {
        if(queries.size() > value || expired()){
            try (Context ignored = batchSendToLoaderTimer.time()) {
                sendQueriesToLoader(new ArrayList<>(queries));
            }
            queries.clear();
        }
    }

    private boolean expired() {
        long now = System.currentTimeMillis();
        return Duration.ofMillis(lastSentTimestamp - now).compareTo(EXPIRE_TIME) > 0;
    }

    /**
     * Wait for all of the submitted tasks to have been completed
     */
    public void waitToFinish(){
        flush();
        all(futures).join();
        futures.clear();
        System.out.println("All tasks completed");
    }

    /**
     * Wait for all of the submitted tasks to have been completed
     */
    public void close(){
        threadPool.shutdownNow();
    }

    /**
     * Send a collection of queries to the TasksController, blocking until
     * there is availability to send.
     * If the collection contains read only queries throw an illegal argument exception.
     *
     * Release the semaphore when a task completes.
     * If there was an error communicating with the host to get the status, throw an exception.
     *
     * @param queries Queries to be inserted
     */
    void sendQueriesToLoader(Collection<Query> queries){
        lastSentTimestamp = System.currentTimeMillis();
        Json configuration = Json.object()
                .set(KEYSPACE_PARAM, keyspace.getValue())
                .set(BATCH_NUMBER, batchNumber)
                .set(TASK_LOADER_MUTATIONS,
                        queries.stream().map(Query::toString).collect(toList()));

        Callable<TaskResult> callable = () -> {
            try (Context ignored = batchSendToEngineTimer.time()) {
                return taskClient
                        .sendTask("ai.grakn.engine.loader.MutatorTask",
                                BatchMutatorClient.class.getName(),
                                Instant.ofEpochMilli(new Date().getTime()), null, configuration, 10000, true);
            }
        };

        Retryer<TaskResult> sendQueryRetry = RetryerBuilder.<TaskResult>newBuilder()
                .retryIfExceptionOfType(IOException.class)
                .retryIfRuntimeException()
                .retryIfResult(r -> r != null && r.getCode().startsWith("5"))
                .withStopStrategy(StopStrategies.stopAfterAttempt(maxRetries))
                .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS))
                .build();

        CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
            LOG.debug("Sending {} queries", queries.size());
            try {
                TaskResult taskId = sendQueryRetry.call(callable);
                onCompletionOfTask.accept(taskId);
            } catch (Exception e) {
                failureMeter.mark();
                printError("Error while executing queries:\n{" + queries + "} \n", e);
            }
            return null;
        }, threadPool);
        futures.add(future);
    }

    private void printError(String message, Throwable error){
        if(debugOn) {
            System.err.println(message);
            if(error != null){
                System.err.println("Caused by: ");
                error.printStackTrace();

                //Cancel and clear the futures
                futures.forEach(f -> f.cancel(true));
                futures.clear();

                throw new RuntimeException(error);
            }
        }
    }
}

