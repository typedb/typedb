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
import ai.grakn.graql.InsertQuery;
import ai.grakn.util.ErrorMessage;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpRetryException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.util.REST.HttpConn.APPLICATION_POST_TYPE;
import static ai.grakn.util.REST.HttpConn.CONTENT_TYPE;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.LIMIT_PARAM;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_LOADER_INSERTS;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;
import static ai.grakn.util.REST.WebPath.TASKS_URI;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Client to load qraql queries into Grakn.
 *
 * Provides methods to batch insert queries. Optionally can provide a consumer
 * that will execute when a batch finishes loading. LoaderClient will block when the configured
 * resources are being used to execute tasks.
 *
 * @author alexandraorth
 */
public class LoaderClient extends Client {

    private static final Logger LOG = LoggerFactory.getLogger(LoaderClient.class);

    private static final String POST = "http://%s" + TASKS_SCHEDULE_URI;
    private static final String GET = "http://%s" + TASKS_URI + "/{id}";

    private final Set<TaskId> submittedTasks;
    private final Collection<InsertQuery> queries;
    private final String keyspace;
    private final String uri;

    private Consumer<Json> onCompletionOfTask;
    private AtomicInteger batchNumber;
    private Semaphore blocker;
    private int batchSize;
    private int blockerSize;
    private boolean retry = false;

    public LoaderClient(String keyspace, String uri) {
        this(keyspace, uri, (Json t) -> {});
    }

    public LoaderClient(String keyspace, String uri, Consumer<Json> onCompletionOfTask){
        this.uri = uri;
        this.keyspace = keyspace;
        this.queries = new HashSet<>();
        this.submittedTasks = ConcurrentHashMap.newKeySet();
        this.onCompletionOfTask = onCompletionOfTask;
        this.batchNumber = new AtomicInteger(0);

        setBatchSize(25);
        setNumberActiveTasks(25);
    }

    /**
     * Tell the {@link LoaderClient} if it should retry sending tasks when the Engine
     * server is not available
     *
     * @param retry boolean representing if engine should retry
     */
    public LoaderClient setRetryPolicy(boolean retry){
        this.retry = retry;
        return this;
    }

    /**
     * Provide a consumer function to execute upon task completion
     * @param onCompletionOfTask function applied to the last state of the task
     */
    public LoaderClient setTaskCompletionConsumer(Consumer<Json> onCompletionOfTask){
        this.onCompletionOfTask = onCompletionOfTask;
        return this;
    }

    /**
     * Set the size of the each transaction in terms of number of vars.
     * @param size number of vars in each transaction
     */
    public LoaderClient setBatchSize(int size){
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
     * The Loader {@link #add(InsertQuery)} method will block on the value of this field.
     *
     * @param size number of tasks to allow to run at any given time
     */
    public LoaderClient setNumberActiveTasks(int size){
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
    public void add(InsertQuery query){
        queries.add(query);
        if(queries.size() >= batchSize){
            sendQueriesToLoader(new HashSet<>(queries));
            queries.clear();
        }
    }

    /**
     * Load any remaining batches in the queue.
     */
    public void flush(){
        if(queries.size() > 0){
            sendQueriesToLoader(new HashSet<>(queries));
            queries.clear();
        }
    }

    /**
     * Wait for all of the submitted tasks to have been completed
     */
    public void waitToFinish(){
        flush();
        while(!submittedTasks.isEmpty() && blocker.availablePermits() != blockerSize) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    /**
     * Send a collection of insert queries to the TasksController, blocking until
     * there is availability to send.
     *
     * Release the semaphore when a task completes.
     * If there was an error communicating with the host to get the status, throw an exception.
     *
     * @param queries Queries to be inserted
     */
    private void sendQueriesToLoader(Collection<InsertQuery> queries){
        TaskId taskId;
        CompletableFuture<Json> status;

        try {
            blocker.acquire();

            taskId = TaskId.of(executePost(getConfiguration(queries, batchNumber.incrementAndGet())));

            // Add this task to the set of submitted tasks
            submittedTasks.add(taskId);
            status = makeTaskCompletionFuture(taskId);

        } catch (Throwable throwable){
            LOG.error(getFullStackTrace(throwable));
            onCompletionOfTask.accept(null);
            blocker.release();
            return;
        }

        // Function to execute when the task completes
        status.whenComplete((result, error) -> {
            blocker.release();

            if (error != null) {
                LOG.error(getFullStackTrace(error));
            }

            onCompletionOfTask.accept(result);
        }).whenComplete((result, error) ->
            submittedTasks.remove(taskId)
        );
    }

    /**
     * Set POST request to host containing information
     * to execute a Loading Tasks with the insert queries as the body of the request
     *
     * @return A Completable future that terminates when the task is finished
     */
    private String executePost(String body) {
        try {
            HttpResponse<Json> response = Unirest.post(format(POST, uri))
                    .queryString(TASK_CLASS_NAME_PARAMETER, "ai.grakn.engine.loader.LoaderTask")
                    .queryString(TASK_RUN_AT_PARAMETER, String.valueOf(new Date().getTime()))
                    .queryString(LIMIT_PARAM, "10000")
                    .queryString(TASK_CREATOR_PARAMETER, LoaderClient.class.getName())
                    .header(CONTENT_TYPE, APPLICATION_POST_TYPE)
                    .body(body)
                    .asObject(Json.class);

            return response.getBody().at("id").asString();
        } catch (UnirestException e) {
            if(retry){
                return executePost(body);
            } else {
                throw new RuntimeException(ErrorMessage.ERROR_COMMUNICATING_TO_HOST.getMessage(uri), e);
            }
        }
    }

    /**
     * Fetch the status of a single task from the Tasks Controller
     * @param id ID of the task to be fetched
     * @return Json object containing status of the task
     */
    private Json getStatus(TaskId id) throws HttpRetryException {
        try {
            HttpResponse<Json> response = Unirest.get(format(GET, uri))
                    .routeParam("id", id.getValue())
                    .asObject(Json.class);

            if(response.getStatus() == 404){
                throw new IllegalArgumentException("Not found in Grakn task storage: " + id);
            }

            // get response
            return response.getBody();
        } catch (UnirestException e) {
            throw new HttpRetryException(ErrorMessage.ERROR_COMMUNICATING_TO_HOST.getMessage(uri), 404);
        }
    }

    /**
     * A completable future that polls the Task Controller to check for the status of the
     * given ID. It terminates when the status of that task is COMPLETED, FAILED or STOPPED.
     *
     * @param id ID of the task to wait on completion
     * @return Completable future that will await completion of the given task
     */
    private CompletableFuture<Json> makeTaskCompletionFuture(TaskId id){
        return CompletableFuture.supplyAsync(() -> {
            while (true) {
                try {
                    Json taskState = getStatus(id);
                    TaskStatus status = TaskStatus.valueOf(taskState.at(TASK_STATUS_PARAMETER).asString());
                    if (status == COMPLETED || status == FAILED || status == STOPPED) {
                        return taskState;
                    }
                } catch (IllegalArgumentException e) {
                    // Means the task has not yet been stored: we want to log the error, but continue looping
                    LOG.warn(format("Task [%s] not found on server. Attempting to get status again.", id));
                } catch (HttpRetryException e){
                    LOG.warn(format("Could not communicate with host %s for task [%s] ", uri, id));
                    if(retry){
                        LOG.warn(format("Attempting communication again with host %s for task [%s]", uri, id));
                    } else {
                        throw new RuntimeException(e);
                    }
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                } finally {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /**
     * Transform queries into Json configuration needed by the Loader task
     * @param queries queries to include in configuration
     * @param batchNumber number of the current batch being sent
     * @return configuration for the loader task
     */
    private String getConfiguration(Collection<InsertQuery> queries, int batchNumber){
        return Json.object()
                .set(KEYSPACE_PARAM, keyspace)
                .set("batchNumber", batchNumber)
                .set(TASK_LOADER_INSERTS, queries.stream().map(InsertQuery::toString).collect(toList()))
                .toString();
    }

}

