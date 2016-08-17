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

package io.mindmaps.loader;

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.constants.RESTUtil;
import io.mindmaps.graql.Var;
import io.mindmaps.util.ConfigProperties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.IOUtil;

import javax.xml.ws.http.HTTPException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/**
 * RESTLoader that distributes computation to multiple Mindmaps Engine instances
 */
public class DistributedLoader extends Loader {

    private final Logger LOG = LoggerFactory.getLogger(DistributedLoader.class);
    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private String graphName;
    private int currentHost;
    private String[] hostsArray;

    private Map<String, Semaphore> availability;
    private Map<String, Set<String>> transactions;

    public DistributedLoader(String graphNameInit, Collection<String> hosts) {
        ConfigProperties prop = ConfigProperties.getInstance();
        batchSize = prop.getPropertyAsInt(ConfigProperties.BATCH_SIZE_PROPERTY);
        graphName = graphNameInit;
        batch = new HashSet<>();
        hostsArray = hosts.toArray(new String[hosts.size()]);
        currentHost = 0;

        // create availability map
        availability = new HashMap<>();
        hosts.forEach(h -> availability.put(h, new Semaphore(1)));

        // instantiate transactions map
        transactions = new HashMap<>();
        hosts.forEach(h -> transactions.put(h, new HashSet<>()));

        executor.submit(this::checkForStatusLoop);
    }

    public void waitToFinish(){
        flush();
        while(!transactions.values().stream().allMatch(Set::isEmpty)){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Done!");
    }

    public void submitBatch(Collection<Var> batch) {
        String batchedString = batch.stream().map(Object::toString).collect(Collectors.joining(";"));

        if (batchedString.length() == 0) {
            return;
        }

        HttpURLConnection currentConn = acquireNextHost();
        int respCode;
        String respMessage = null;
        try {
            String query = RESTUtil.HttpConn.INSERT_PREFIX + batchedString;
            currentConn.setRequestProperty(RESTUtil.HttpConn.CONTENT_LENGTH, Integer.toString(query.length()));
            currentConn.getOutputStream().write(query.getBytes(RESTUtil.HttpConn.UTF8));
            respCode = currentConn.getResponseCode();
            respMessage = currentConn.getResponseMessage();
            if (respCode != RESTUtil.HttpConn.HTTP_TRANSACTION_CREATED) {
                throw new HTTPException(respCode);
            }

            String transactionId = IOUtils.toString(currentConn.getInputStream());
            transactions.get(hostsArray[currentHost]).add(transactionId);

        } catch (HTTPException e) {
            LOG.error(ErrorMessage.ERROR_IN_DISTRIBUTED_TRANSACTION.getMessage(currentConn.getURL().toString(), e.getStatusCode(), respMessage, batchedString));
            e.printStackTrace();
        } catch (IOException e) {
        } finally {
            currentConn.disconnect();
        }
    }

    private HttpURLConnection acquireNextHost() {
        String host = nextHost();

        // check availability
        while (availability.get(host).availablePermits() == 0) {
            host = nextHost();
        }

        return getHost(host);
    }

    /**
     * Return the string ip of the next host
     *
     * @return ip of the next host
     */
    private String nextHost() {
        currentHost++;
        if (currentHost == hostsArray.length) {
            currentHost = 0;
        }

        return hostsArray[currentHost];
    }

    /**
     * Get a HTTP Connection to the engine instance running on the ip of the given host
     *
     * @param host ip of the machine where engine is running
     * @return http connection to the machine where engine is running
     */
    private HttpURLConnection getHost(String host) {
        HttpURLConnection urlConn = null;
        try {
            String url = "http://" + host + ":" + ConfigProperties.getInstance().getProperty(ConfigProperties.SERVER_PORT_NUMBER) + RESTUtil.WebPath.NEW_TRANSACTION_URI + "?" + RESTUtil.Request.GRAPH_NAME_PARAM + "=" + graphName;
            urlConn = (HttpURLConnection) new URL(url).openConnection();
            urlConn.setDoOutput(true);
            urlConn.setRequestMethod(RESTUtil.HttpConn.POST_METHOD);
            urlConn.addRequestProperty(RESTUtil.HttpConn.CONTENT_TYPE, RESTUtil.HttpConn.APPLICATION_POST_TYPE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return urlConn;
    }

    public void checkForStatusLoop(){
        try {
            while (true) {

                // loop through the hosts
                for (String host : transactions.keySet()) {

                    // loop through the transactions of each host
                    Iterator<String> transactionsIter = transactions.get(host).iterator();
                    while(transactionsIter.hasNext()){
                        String transaction = transactionsIter.next();

                        if (isFinished(host, transaction)) {
                            availability.get(host).release();
                            transactionsIter.remove();
                        }
                    }
                }

                Thread.sleep(500);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check if transaction is finished
     *
     * @param transaction transaction to check if has finished
     * @return if the given transaction has finished
     */
    private boolean isFinished(String host, String transaction) {

        String url = "http://" + host + ":" + ConfigProperties.getInstance().getProperty(ConfigProperties.SERVER_PORT_NUMBER) +
                "/transactionStatus/" + transaction + "?" + RESTUtil.Request.GRAPH_NAME_PARAM + "=" + graphName;

        HttpURLConnection urlConn = null;
        try {
            urlConn = (HttpURLConnection) new URL(url).openConnection();
            urlConn.setDoOutput(true);

            String response = IOUtils.toString(urlConn.getInputStream());
            if (response.equals(State.FINISHED.name())) {
                System.out.println("finished " + transaction);
                return true;
            }
        }
        catch (IOException e){
            LOG.error(e.getMessage());
            return true;
        }
        finally {
            urlConn.disconnect();
        }

        return false;
    }
}
