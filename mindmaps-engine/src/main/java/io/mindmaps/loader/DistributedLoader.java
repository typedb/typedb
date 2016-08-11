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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.ws.http.HTTPException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;

public class DistributedLoader {

    private final Logger LOG = LoggerFactory.getLogger(DistributedLoader.class);

    private Collection<Var> batch;
    private int batchSize;
    private String graphName;
    private int currentHost;
    private String[] hostsArray;

    public DistributedLoader(String graphNameInit, Collection<String> hosts) {
        ConfigProperties prop = ConfigProperties.getInstance();
        batchSize = prop.getPropertyAsInt(ConfigProperties.BATCH_SIZE_PROPERTY);
        graphName = graphNameInit;
        batch = new HashSet<>();
        hostsArray = hosts.toArray(new String[hosts.size()]);
        currentHost = 0;
    }


    public void addToQueue(Collection<Var> vars) {
        batch.addAll(vars);
        if (batch.size() >= batchSize) {
            sendToNextHost(batch.stream().map(Object::toString).collect(Collectors.joining(";")));
            batch = new HashSet<>();
        }
    }

    public void sendToNextHost(String batchedString) {

        if (batchedString.length() == 0) return;
        HttpURLConnection currentConn = nextHost();
        int respCode;
        String respMessage= null;
        try {
            String query = RESTUtil.HttpConn.INSERT_PREFIX + batchedString;
            currentConn.setRequestProperty(RESTUtil.HttpConn.CONTENT_LENGTH, Integer.toString(query.length()));
            currentConn.getOutputStream().write(query.getBytes(RESTUtil.HttpConn.UTF8));
            respCode = currentConn.getResponseCode();
            respMessage = currentConn.getResponseMessage();
            if (respCode != RESTUtil.HttpConn.HTTP_TRANSACTION_CREATED)
                throw new HTTPException(respCode);
        } catch (HTTPException e) {
            LOG.error(ErrorMessage.ERROR_IN_DISTRIBUTED_TRANSACTION.getMessage(currentConn.getURL().toString(), e.getStatusCode(), respMessage, batchedString));
            e.printStackTrace();
        } catch(IOException e){

        }
        finally {
            currentConn.disconnect();
        }

    }

    private HttpURLConnection nextHost() {
        currentHost++;
        if (currentHost == hostsArray.length) currentHost = 0;
        HttpURLConnection urlConn = null;
        try {
            String url = "http://" + hostsArray[currentHost] + ":"+ConfigProperties.getInstance().getProperty(ConfigProperties.SERVER_PORT_NUMBER)+RESTUtil.WebPath.NEW_TRANSACTION_URI+"?" + RESTUtil.Request.GRAPH_NAME_PARAM + "=" + graphName;
            urlConn = (HttpURLConnection) new URL(url).openConnection();
            urlConn.setDoOutput(true);
            urlConn.setRequestMethod(RESTUtil.HttpConn.POST_METHOD);
            urlConn.addRequestProperty(RESTUtil.HttpConn.CONTENT_TYPE, RESTUtil.HttpConn.APPLICATION_POST_TYPE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return urlConn;
    }

    public void setBatchSize(int size) {
        batchSize = size;
    }


    public void addToQueue(Var var) {
        addToQueue(Collections.singletonList(var));
    }


}
