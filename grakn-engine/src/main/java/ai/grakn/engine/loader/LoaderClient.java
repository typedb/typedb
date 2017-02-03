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

import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.graql.InsertQuery;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import com.auth0.jwt.internal.org.apache.commons.io.IOUtils;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.FAILED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.TASK_LOADER_INSERTS;
import static ai.grakn.util.REST.WebPath.TASKS_URI;
import static java.util.stream.Collectors.toList;

import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.LIMIT_PARAM;

import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;

/**
 * Client to load t
 */
public class LoaderClient {

    private static final Logger LOG = LoggerFactory.getLogger(LoaderClient.class);

    private final String POST = "http://%s" + TASKS_SCHEDULE_URI;
    private final String GET = "http://%s" + TASKS_URI + "/%s";

    private final Consumer<Json> onCompletionOfTask;
    private final Collection<CompletableFuture> futures;
    private final Collection<InsertQuery> queries;
    private final String keyspace;
    private final String uri;

    private AtomicInteger batchNumber;
    private Semaphore blocker;
    private int batchSize;

    public LoaderClient(String keyspace, String uri) {
        this(keyspace, uri, (Json t) -> {});
    }

    public LoaderClient(String keyspace, String uri, Consumer<Json> onCompletionOfTask){
        this.uri = uri;
        this.keyspace = keyspace;
        this.queries = new HashSet<>();
        this.futures = new HashSet<>();
        this.onCompletionOfTask = onCompletionOfTask;
        this.batchNumber = new AtomicInteger(0);

        setBatchSize(25);
        setQueueSize(25);
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
     * Set the size of the queue- this is equivalent to the size of the semaphore.
     * @param size the size of the queue
     */
    public LoaderClient setQueueSize(int size){
        this.blocker = new Semaphore(size);
        return this;
    }

    /**
     * Add an insert query to the queue
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
        while(!futures.stream().allMatch(CompletableFuture::isDone)){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    /**
     * Send a collection of insert queries to the TasksController, blocking until
     * there is availability to send.
     *
     * Release the semaphore when a task completes
     *
     * @param queries Queries to be inserted
     */
    private void sendQueriesToLoader(Collection<InsertQuery> queries){
        try {
            blocker.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        CompletableFuture<Json> status = executePost(getConfiguration(queries, batchNumber.incrementAndGet()));

        if(status == null){
            LOG.error("Could not send to host: " + queries);
            return;
        }

        // Add this status to the set of completable futures
        futures.add(status);

        // When this task completes release the semaphore
        status.thenAccept(finalTaskStatus -> {
            blocker.release();
            futures.remove(status);
            onCompletionOfTask.accept(finalTaskStatus);
        });
    }

    /**
     * Set POST request to host containing information
     * to execute a Loading Tasks with the insert queries as the body of the request
     *
     * @return A Completable future that terminates when the task is finished
     */
    private CompletableFuture<Json> executePost(String body){
        HttpURLConnection connection = null;
        try {
            URL url = new URL(String.format(POST, uri) + "?" + getPostParams());

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
            Json response = Json.read(IOUtils.toString(connection.getInputStream()));
            String id = response.at("id").asString();

            return await(id);
        }
        catch (IOException e){
            LOG.error(ErrorMessage.ERROR_IN_DISTRIBUTED_TRANSACTION.getMessage(getResponseMessage(connection)));
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }

        return null;
    }

    /**
     * Fetch the status of a single task from the Tasks Controller
     * @param id ID of the task to be fetched
     * @return Json object containing status of the task
     */
    private Json getStatus(String id){
        HttpURLConnection connection = null;
        try {
            URL url = new URL(String.format(GET, uri, id));

            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);

            // create post
            connection.setRequestMethod(REST.HttpConn.GET_METHOD);

            // get response
            return Json.read(IOUtils.toString(connection.getInputStream()));
        }
        catch (IOException e){
            LOG.error(ErrorMessage.ERROR_COMMUNICATING_TO_HOST.getMessage(getResponseMessage(connection)));
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }

        return null;
    }

    /**
     * A completable future that polls the Task Controller to check for the status of the
     * given ID. It terminates when the status of that task is COMPLETED, FAILED or STOPPED.
     *
     * @param id ID of the task to wait on completion
     * @return Completable future that will await completion of the given task
     */
    private CompletableFuture<Json> await(String id){
        return CompletableFuture.supplyAsync(() -> {
            while (true) {
                Json taskState = getStatus(id);
                TaskStatus status = TaskStatus.valueOf(taskState.at("status").asString());
                if (status == COMPLETED || status == FAILED || status == STOPPED) {
                    return taskState;
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage());
                }
            }
        });
    }

    /**
     * Get the response message from an http connection
     * @param connection to get response message from
     * @return the response message
     */
    private String getResponseMessage(HttpURLConnection connection){
        try {
            return connection.getResponseMessage();
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

    private String getPostParams(){
        return TASK_CLASS_NAME_PARAMETER + "=" + LoaderTask.class.getName() + "&" +
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
}

