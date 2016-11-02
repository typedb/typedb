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

package io.mindmaps.engine.loader;

import com.google.common.collect.Lists;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.REST;
import mjson.Json;
import spark.utils.IOUtils;

import javax.xml.ws.http.HTTPException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static io.mindmaps.engine.loader.TransactionState.State;

/**
 * RESTLoader that distributes computation to multiple Mindmaps Engine instances
 */
public class DistributedLoader extends Loader {

    private static ConfigProperties prop = ConfigProperties.getInstance();
    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private Future future;
    private long pollingFrequency;
    private String graphName;

    private int currentHost;
    private List<String> hosts;

    private Map<String, Semaphore> availability;
    private Map<String, Integer> jobsTerminated;

    private static final String POST = "http://%s:" +
            ConfigProperties.getInstance().getProperty(ConfigProperties.SERVER_PORT_NUMBER) +
            REST.WebPath.NEW_TRANSACTION_URI + "?" +
            REST.Request.GRAPH_NAME_PARAM + "=%s";

    private static final String GET = "http://%s:" +
            ConfigProperties.getInstance().getProperty(ConfigProperties.SERVER_PORT_NUMBER) +
            REST.WebPath.LOADER_STATE_URI;

    public DistributedLoader(String graphName, Collection<String> hosts) {
        this.hosts = Lists.newArrayList(hosts);
        currentHost = 0;

        setBatchSize(prop.getPropertyAsInt(ConfigProperties.BATCH_SIZE_PROPERTY));
        setPollingFrequency(prop.getPropertyAsLong(ConfigProperties.POLLING_FREQUENCY_PROPERTY));
        setThreadsNumber(prop.getAvailableThreads());

        resetAvailabilityMap();
        resetJobsTerminated();

        this.graphName = graphName;
    }

    @Override
    public void setThreadsNumber(int number){
        this.threadsNumber = number;
        resetAvailabilityMap();
    }

    public void resetAvailabilityMap(){
        // create availability map
        availability = new HashMap<>();
        hosts.forEach(h -> availability.put(h, new Semaphore(threadsNumber * 3)));
    }

    public void resetJobsTerminated(){
        jobsTerminated = new HashMap<>();
        hosts.forEach(h -> jobsTerminated.put(h, 0));
    }

    /**
     * Amount of time to wait before checking number completed jobs with slaves
     */
    public void setPollingFrequency(long number){
        this.pollingFrequency = number;
    }

    /**
     * Block the main thread until all of the transactions have finished loading
     */
    public void waitToFinish() {
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

        Json inserts = Json.array();
        queries.stream().map(InsertQuery::toString).forEach(inserts::add);

        HttpURLConnection currentConn = acquireNextHost();
        executePost(currentConn, inserts.toString());

        int responseCode = getResponseCode(currentConn);
        if (responseCode != REST.HttpConn.HTTP_TRANSACTION_CREATED) {
            throw new HTTPException(responseCode);
        }

        loadingJobs.incrementAndGet();
        LOG.info("Transaction sent to host: " + hosts.get(currentHost));

        if(future == null){ startCheckingStatus(); }

        currentConn.disconnect();
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
     * Check if all transactions are finished
     *
     * @return true if all transactions have finished
     */
    private boolean transactionsIsEmpty() {
        return loadingJobs.get() + enqueuedJobs.get() == 0;
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
        while (!transactionsIsEmpty()) {

            int runningQueued = 0;
            int runningLoading = 0;
            int runningError = 0;
            int runningFinished = 0;

            // loop through the hosts
            for (String host : availability.keySet()) {

                Json state = Json.read(getHostState(host));
                LOG.info("State from host ["+host+"]:");
                LOG.info(state.toString());

                int queued = state.at(State.QUEUED.name()).asInteger();
                int loading = state.at(State.LOADING.name()).asInteger();
                int error = state.at(State.ERROR.name()).asInteger();
                int finished = state.at(State.FINISHED.name()).asInteger();

                int terminated = finished + error;

                int permitsToRelease = terminated - jobsTerminated.get(host);
                availability.get(host).release(permitsToRelease);
                jobsTerminated.put(host, terminated);

                runningQueued += queued;
                runningLoading += loading;
                runningError += error;
                runningFinished += finished;
            }

            enqueuedJobs.set(runningQueued);
            loadingJobs.set(runningLoading);
            errorJobs.set(runningError);
            finishedJobs.set(runningFinished);

            printLoaderState();

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
            String url = String.format(format, host, graphName);
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
}
