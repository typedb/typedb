/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.loader.client;

import ai.grakn.engine.loader.Loader;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.graql.InsertQuery;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import com.google.common.collect.Lists;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import javax.xml.ws.http.HTTPException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static ai.grakn.util.REST.Request.Task.Loader.INSERTS;
import static ai.grakn.util.REST.Request.Task.Loader.KEYSPACE;
import static ai.grakn.engine.util.ConfigProperties.BATCH_SIZE_PROPERTY;
import static ai.grakn.engine.util.ConfigProperties.POLLING_FREQUENCY_PROPERTY;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.FAILED;

import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;
import static ai.grakn.util.REST.WebPath.TASKS_URI;

import static ai.grakn.engine.util.ConfigProperties.SERVER_PORT_NUMBER;

import static java.util.stream.Collectors.toList;

public class LoaderClient implements Loader {

    private static final Logger LOG = LoggerFactory.getLogger(Loader.class);
    private static final ConfigProperties properties = ConfigProperties.getInstance();
    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private Future future;
    private long pollingFrequency;
    private int batchSize;
    private Collection<InsertQuery> queries;
    private String keyspace;

    private int currentHost;
    private List<String> hosts;

    private Map<String, Semaphore> availability;
    private Map<String, Integer> jobsTerminated;

    private static final String POST = post();

    private static final String GET = "http://%s:" + properties.getProperty(SERVER_PORT_NUMBER) + TASKS_URI;

    public LoaderClient(String keyspace, Collection<String> hosts) {
        this.hosts = Lists.newArrayList(hosts);
        currentHost = 0;

        setBatchSize(properties.getPropertyAsInt(BATCH_SIZE_PROPERTY));
        setPollingFrequency(properties.getPropertyAsLong(POLLING_FREQUENCY_PROPERTY));

        setQueueSize(25);
        resetJobsTerminated();

        this.keyspace = keyspace;
        this.queries = new HashSet<>();
    }

    /**
     * @return the current batch size - minimum number of vars to be loaded in a transaction
     */
    public int getBatchSize(){
        return this.batchSize;
    }

    /**
     * Set the size of the each transaction in terms of number of vars.
     * @param size number of vars in each transaction
     */
    public Loader setBatchSize(int size){
        this.batchSize = size;
        return this;
    }

    /**
     * Set the size of the queue- this is equivalent to the size of the semaphore.
     * @param size the size of the queue
     */
    public Loader setQueueSize(int size){
        // create availability map
        availability = new HashMap<>();
        hosts.forEach(h -> availability.put(h, new Semaphore(size)));
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
     * Amount of time to wait before checking number completed jobs with slaves
     */
    public void setPollingFrequency(long number){
        this.pollingFrequency = number;
    }

    /**
     * Load any remaining batches in the queue.
     */
    public void flush(){
        if(queries.size() > 0){
            sendQueriesToLoader(queries);
            queries.clear();
        }
    }

    /**
     * Block the main thread until all of the transactions have finished loading
     */
    public void waitToFinish() {
        waitToFinish(100000);
    }

    public void waitToFinish(int time){
        flush();
        if(future != null){
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    public void sendQueriesToLoader(Collection<InsertQuery> queries) {

        HttpURLConnection currentConn = acquireNextHost();
        executePost(currentConn, getConfiguration(queries));

        int responseCode = getResponseCode(currentConn);
        if (responseCode != REST.HttpConn.HTTP_TRANSACTION_CREATED) {
            throw new HTTPException(responseCode);
        }

        LOG.info("Transaction sent to host: " + hosts.get(currentHost));

        if(future == null){ startCheckingStatus(); }
    }

    /**
     * Get the state of a given host
     * @param host host to check the state of
     * @return if the given transaction has finished
     */
    private String getHostState(String host) {
        HttpURLConnection connection = getHost(host, GET);
        String response = getResponseBody(connection);
        connection.disconnect();

        return response;
    }

    /**
     * Start checking the status of the transactions in another thread.
     */
    private void startCheckingStatus() {
        future = executor.submit(this::checkForStatusLoop);
    }

    /**
     * Re-sets the executor and indicates the system is no longer checking the status of the transactions
     */
    private void stopCheckingStatus() {
        executor.shutdownNow();
        executor = Executors.newSingleThreadExecutor();
        future = null;
    }

    /**
     * Loop checking the status of the hosts every 30 seconds.
     * This method updates the semaphore availability.
     */
    public void checkForStatusLoop() {
        boolean finished = false;

        while (!finished) {

            int runningCreated = 0;
            int runningScheduled = 0;
            int runningCompleted = 0;
            int runningRunning = 0;
            int runningFailed = 0;

            // loop through the hosts
            for (String host : availability.keySet()) {

                Json state = Json.read(getHostState(host));
                LOG.info("State from host ["+host+"]:");
                LOG.info(state.toString());

                int created = state.at(CREATED.name()).asInteger();
                int scheduled = state.at(SCHEDULED.name()).asInteger();
                int completed = state.at(COMPLETED.name()).asInteger();
                int running = state.at(RUNNING.name()).asInteger();
                int failed = state.at(FAILED.name()).asInteger();

                int terminated = completed + failed;

                int permitsToRelease = terminated - jobsTerminated.get(host);
                availability.get(host).release(permitsToRelease);
                jobsTerminated.put(host, terminated);

                printLoaderState(created, scheduled, completed, running, failed);

                runningCreated += created;
                runningScheduled += scheduled;
                runningCompleted += completed;
                runningRunning += running;
                runningFailed += failed;
            }

            printLoaderState(runningCreated, runningScheduled, runningCompleted, runningRunning, runningFailed);

            if((runningCreated + runningRunning + runningScheduled) == 0){
                finished = true;
            }

            try {
                Thread.sleep(pollingFrequency);
            } catch (InterruptedException e) {
                LOG.error("Exception",e);
            }
        }

        stopCheckingStatus();
    }

    /**
     * Block until there is a host available
     *
     * @return the available http connection
     */
    private HttpURLConnection acquireNextHost() {
        String host = nextHost();

        // check availability
        while (!availability.get(host).tryAcquire()) {
            host = nextHost();
        }

        return getHost(host, POST);
    }

    /**
     * Return the string ip of the next host using round-robin technique
     *
     * @return ip of the next host
     */
    private String nextHost() {
        currentHost++;
        if (currentHost == hosts.size()) {
            currentHost = 0;
        }

        return hosts.get(currentHost);
    }

    /**
     * Get a HTTP Connection to the engine instance running on the ip of the given host
     *
     * @param host ip of the machine where engine is running
     * @return http connection to the machine where engine is running
     */
    private HttpURLConnection getHost(String host, String format) {
        HttpURLConnection urlConn = null;
        try {
            String url = String.format(format, host, keyspace);
            urlConn = (HttpURLConnection) new URL(url).openConnection();
            urlConn.setDoOutput(true);
        } catch (IOException e) {
            LOG.error("IOException",e);
        }
        return urlConn;
    }

    /**
     * Send an insert query to one host
     */
    private String executePost(HttpURLConnection connection, String body){

        try {
            // create post
            connection.setRequestMethod(REST.HttpConn.POST_METHOD);
            connection.addRequestProperty(REST.HttpConn.CONTENT_TYPE, REST.HttpConn.APPLICATION_POST_TYPE);

            // add body and execute
            connection.setRequestProperty(REST.HttpConn.CONTENT_LENGTH, Integer.toString(body.length()));
            connection.getOutputStream().write(body.getBytes(REST.HttpConn.UTF8));

            // get response
            return connection.getResponseMessage();
        }
        catch (HTTPException e){
            LOG.error(ErrorMessage.ERROR_IN_DISTRIBUTED_TRANSACTION.getMessage(
                    connection.getURL().toString(),
                    e.getStatusCode(),
                    getResponseMessage(connection),
                    body));
        }
        catch (IOException e){
            LOG.error(ErrorMessage.ERROR_COMMUNICATING_TO_HOST.getMessage(connection.getURL().toString()));
        }

        return null;
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
            LOG.error("IOException",e);
        }
        return null;
    }

    /**
     * Get the response body from an HTTP connection safely
     * @param connection to get response body from
     * @return the response body
     */
    private String getResponseBody(HttpURLConnection connection){
        try {
            return IOUtils.toString(connection.getInputStream());
        } catch (IOException e) {
            LOG.error("IOException",e);
        }
        return null;
    }

    /**
     * Get the response code from an HTTP Connection safely
     * @param connection to get response code from
     * @return response code
     */
    private int getResponseCode(HttpURLConnection connection){
        try {
            return connection.getResponseCode();
        } catch (IOException e) {
            LOG.error("IOException",e);
        }
        return 0;
    }

    /**
     * Transform queries into Json configuration needed by the Loader task
     * @param queries queries to include in configuration
     * @return configuration for the loader task
     */
    private String getConfiguration(Collection<InsertQuery> queries){
        return Json.object()
                .set(KEYSPACE, keyspace)
                .set(INSERTS, queries.stream().map(InsertQuery::toString).collect(toList()))
                .toString();
    }

    /**
     * Method that logs the current state of loading transactions
     */
    private void printLoaderState(int created, int scheduled, int completed, int running, int failed) {
        LOG.info(Json.object()
                .set(CREATED.name(), created)
                .set(SCHEDULED.name(), scheduled)
                .set(COMPLETED.name(), completed)
                .set(RUNNING.name(), running)
                .set(FAILED.name(), failed)
                .toString());
    }

    /**
     * Reset the jobs terminated map
     */
    private void resetJobsTerminated(){
        jobsTerminated = new HashMap<>();
        hosts.forEach(h -> jobsTerminated.put(h, 0));
    }

    private static String get(){
        return "http://%s:" +
                properties.getProperty(SERVER_PORT_NUMBER) +

                SCHEDULED + "?" +
                GRAPH_NAME_PARAM + "=%s";
    }
}
