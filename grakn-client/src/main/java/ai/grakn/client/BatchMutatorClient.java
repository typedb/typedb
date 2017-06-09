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

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import ai.grakn.graql.Query;
import static ai.grakn.util.ErrorMessage.READ_ONLY_QUERY;
import static ai.grakn.util.REST.Request.BATCH_NUMBER;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.TASK_LOADER_MUTATIONS;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import java.io.IOException;
import static java.lang.String.format;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
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

    private static final Logger LOG = LoggerFactory.getLogger(BatchMutatorClient.class);

    // Change in behaviour in v0.14 Previously infinite, now limited
    private static final int MAX_RETRIES = 100;

    private final Map<Integer,CompletableFuture> futures;
    private final Collection<Query> queries;
    private final String keyspace;
    private final TaskClient taskClient;

    private Consumer<TaskStatus> onCompletionOfTask;
    private AtomicInteger batchNumber;
    private Semaphore blocker;
    private int batchSize;
    private int blockerSize;
    private boolean retry = false;

    public BatchMutatorClient(String keyspace, String uri) {
        this(keyspace, uri, (TaskStatus t) -> {});
    }

    public BatchMutatorClient(String keyspace, String uri, Consumer<TaskStatus> onCompletionOfTask) {
        this.keyspace = keyspace;
        this.queries = new ArrayList<>();
        this.futures = new ConcurrentHashMap<>();
        this.onCompletionOfTask = onCompletionOfTask;
        this.batchNumber = new AtomicInteger(0);
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
        setNumberActiveTasks(25);
    }

    /**
     * Tell the {@link BatchMutatorClient} if it should retry sending tasks when the Engine
     * server is not available
     *
     * @param retry boolean representing if engine should retry
     */
    public BatchMutatorClient setRetryPolicy(boolean retry){
        this.retry = retry;
        return this;
    }

    /**
     * Provide a consumer function to execute upon task completion
     * @param onCompletionOfTask function applied to the last state of the task
     */
    public BatchMutatorClient setTaskCompletionConsumer(Consumer<TaskStatus> onCompletionOfTask){
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
        this.blockerSize = size;
        this.blocker = new Semaphore(size);
        return this;
    }

    /**
     * Add an insert query to the queue.
     *
     * This method will block while the number of currently executing tasks
     * is equal to the set {@link #blockerSize} which can be set with {@link #setNumberActiveTasks(int)}.
     * It will become unblocked as tasks are completed.
     *
     * @param query insert query to be executed
     */
    public void add(Query query){
        if (query.isReadOnly()) {
            throw new IllegalArgumentException(READ_ONLY_QUERY.getMessage(query.toString()));
        }
        queries.add(query);
        sendQueriesWhenBatchLargerThanValue(batchSize-1);
    }

    /**
     * Load any remaining batches in the queue.
     */
    public void flush(){
        sendQueriesWhenBatchLargerThanValue(0);
    }

    private void sendQueriesWhenBatchLargerThanValue(int value) {
        if(queries.size() > value){
            sendQueriesToLoader(new ArrayList<>(queries));
            queries.clear();
        }
    }

    /**
     * Wait for all of the submitted tasks to have been completed
     */
    public void waitToFinish(){
        flush();
        while(!futures.values().stream().allMatch(CompletableFuture::isDone)
                && blocker.availablePermits() != blockerSize){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
        }
        LOG.info("All tasks completed");
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
        try {
            blocker.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            Json configuration = Json.object()
                    .set(KEYSPACE_PARAM, keyspace)
                    .set(BATCH_NUMBER, batchNumber)
                    .set(TASK_LOADER_MUTATIONS,
                            queries.stream().map(Query::toString).collect(toList()));

            Callable<TaskId> callable = () -> taskClient
                    .sendTask("ai.grakn.engine.loader.MutatorTask",
                            BatchMutatorClient.class.getName(),
                            Instant.ofEpochMilli(new Date().getTime()), null, configuration, 10000);

            Retryer<TaskId> retryer = RetryerBuilder.<TaskId>newBuilder()
                    .retryIfExceptionOfType(IOException.class)
                    .retryIfRuntimeException()
                    .withStopStrategy(StopStrategies.stopAfterAttempt(retry ? MAX_RETRIES : 1))
                    .build();

            TaskId taskId =retryer.call(callable);
            CompletableFuture<TaskStatus> status = makeTaskCompletionFuture(taskId);
            // Add this status to the set of completable futures
            // TODO: use an async client, there's a potential race condition in case of failure just after the unblock
            futures.put(status.hashCode(), status);
            status
                    // Unblock and log errors when task completes
                    .handle((result, error) -> {
                        unblock(status);

                        // Log any errors
                        if(error != null){
                            LOG.error("Error while executing mutator", error);
                        }

                        return result;
                    })
                    // Execute registered completion function
                    .thenAcceptAsync(onCompletionOfTask)
                    // Log errors in completion function
                    .exceptionally(t -> {
                        LOG.error("Error in callback for mutator", t);
                        throw new RuntimeException(t);
                    });
        } catch (Throwable e) {
            LOG.error("Error while executing queries:\n{}", queries);
            blocker.release();
        }
    }

    private void unblock(CompletableFuture<TaskStatus> status){
        blocker.release();
        futures.remove(status.hashCode());
    }

    /**
     * A completable future that polls the Task Controller to check for the status of the
     * given ID. It terminates when the status of that task is COMPLETED, FAILED or STOPPED.
     *
     * @param id ID of the task to wait on completion
     * @return Completable future that will await completion of the given task
     */
    private CompletableFuture<TaskStatus> makeTaskCompletionFuture(TaskId id) {
        return CompletableFuture.supplyAsync(() -> {
            Retryer<TaskStatus> retryer = RetryerBuilder.<TaskStatus>newBuilder()
                    .retryIfExceptionOfType(IOException.class)
                    .retryIfExceptionOfType(IllegalArgumentException.class)
                    .retryIfRuntimeException()
                    .retryIfResult(Objects::isNull)
                    // Keeping compatibility with previous version, retrying many times
                    .withStopStrategy(StopStrategies.stopAfterAttempt(MAX_RETRIES))
                    .build();

            try {
                return retryer.call(() -> {
                            try {
                                TaskStatus status = taskClient.getStatus(id);
                                if (status == COMPLETED || status == FAILED || status == STOPPED) {
                                    return status;
                                } else {
                                    LOG.error(format("Task [%s] had unexpected state [%s]", id, status));
                                    return null;
                                }
                            } catch (IllegalArgumentException e) {
                                // Means the task has not yet been stored: we want to log the error, but continue looping
                                LOG.warn(format("Task [%s] not found on server. Attempting to get status again.", id));
                                throw e;
                            } catch (Throwable t) {
                                LOG.error(format("Task [%s] could not be retrieved, retrying", id));
                                throw new RuntimeException(t);
                            }
                        }
                );
            } catch (Exception e) {
                throw new RuntimeException("Could not get task " + id);
            }
        });
    }
}

