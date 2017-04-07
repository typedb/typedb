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

import ai.grakn.engine.TaskStatus;
import ai.grakn.graql.InsertQuery;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpRetryException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.TASK_LOADER_INSERTS;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.WebPath.Tasks.TASKS;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.LIMIT_PARAM;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;

/**
 * Client to load qraql queries into Grakn.
 *
 * Provides methods to batch insert queries. Optionally can provide a consumer
 * that will execute when a batch finishes loading. LoaderClient will block when the configured
 * resources are being used to execute tasks.
 *
 * @author alexandraorth
 */
public class LoaderClient {

    private static final Logger LOG = LoggerFactory.getLogger(LoaderClient.class);

    private final String POST = "http://%s" + TASKS;
    private final String GET = "http://%s" + TASKS + "/%s";

    private final Map<Integer,CompletableFuture> futures;
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
        this.futures = new ConcurrentHashMap<>();
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
     * Send a collection of insert queries to the TasksController, blocking until
     * there is availability to send.
     *
     * Release the semaphore when a task completes.
     * If there was an error communicating with the host to get the status, throw an exception.
     *
     * @param queries Queries to be inserted
     */
    void sendQueriesToLoader(Collection<InsertQuery> queries){
        try {
            blocker.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            String taskId = executePost(getConfiguration(queries, batchNumber.incrementAndGet()));

            CompletableFuture<Json> status = makeTaskCompletionFuture(taskId);

            // Add this status to the set of completable futures
            futures.put(status.hashCode(), status);

            // Function to execute when the task completes
            status.whenComplete((result, error) -> {
                unblock(status);

                if(error != null){
                    LOG.error("Error", error);
                }

                onCompletionOfTask.accept(result);
            });
        } catch (Throwable throwable){
            LOG.error("Error", throwable);
            blocker.release();
        }
    }

    private void unblock(CompletableFuture<Json> status){
        blocker.release();
        futures.remove(status.hashCode());
    }

    /**
     * Set POST request to host containing information
     * to execute a Loading Tasks with the insert queries as the body of the request
     *
     * @return A Completable future that terminates when the task is finished
     */
    private String executePost(String body) throws HttpRetryException {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(format(POST, uri) + "?" + getPostParams());

            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);

            // create post
            connection.setRequestMethod(REST.HttpConn.POST_METHOD);
            connection.addRequestProperty(REST.HttpConn.CONTENT_TYPE, REST.HttpConn.APPLICATION_POST_TYPE);

            // add body and execute
            connection.setRequestProperty(REST.HttpConn.CONTENT_LENGTH, Integer.toString(body.length()));
            connection.getOutputStream().write(body.getBytes(REST.HttpConn.UTF8));
            connection.getOutputStream().flush();

            // get response
            Json response = Json.read(readResponse(connection.getInputStream()));

            return response.at("id").asString();
        }
        catch (IOException e){
            if(retry){
                return executePost(body);
            } else {
                throw new RuntimeException(ErrorMessage.ERROR_COMMUNICATING_TO_HOST.getMessage(uri));
            }
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Fetch the status of a single task from the Tasks Controller
     * @param id ID of the task to be fetched
     * @return Json object containing status of the task
     */
    private Json getStatus(String id) throws HttpRetryException {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(format(GET, uri, id));

            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);

            // create post
            connection.setRequestMethod(REST.HttpConn.GET_METHOD);

            if(connection.getResponseCode() == 404){
                throw new IllegalArgumentException("Not found in Grakn task storage: " + id);
            }

            // get response
            return Json.read(readResponse(connection.getInputStream()));
        }
        catch (IOException e){
            throw new HttpRetryException(ErrorMessage.ERROR_COMMUNICATING_TO_HOST.getMessage(uri), 404);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * A completable future that polls the Task Controller to check for the status of the
     * given ID. It terminates when the status of that task is COMPLETED, FAILED or STOPPED.
     *
     * @param id ID of the task to wait on completion
     * @return Completable future that will await completion of the given task
     */
    private CompletableFuture<Json> makeTaskCompletionFuture(String id){
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
                }
                
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private String getPostParams(){
        return TASK_CLASS_NAME_PARAMETER + "=ai.grakn.engine.loader.LoaderTask&" +
                TASK_RUN_AT_PARAMETER + "=" + new Date().getTime() + "&" +
                LIMIT_PARAM + "=" + 10000 + "&" +
                TASK_CREATOR_PARAMETER + "=" + LoaderClient.class.getName();
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

    /**
     * Read the input stream from a HttpURLConnection into a String
     * @return String containing response from the server
     */
    private String readResponse(InputStream inputStream) throws IOException {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))){
            StringBuilder response = new StringBuilder();
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                response.append(inputLine);
            }

            return response.toString();
        }
    }
}

