/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ai.grakn.batch;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.graql.Query;
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.Status.Family;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static ai.grakn.util.REST.Request.Graql.ALLOW_MULTIPLE_QUERIES;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
import static ai.grakn.util.REST.Request.Graql.LOADING_DATA;
import static ai.grakn.util.REST.Request.Graql.TX_TYPE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;

/**
 * Default implementation of {@link GraknClient}.
 *
 * @author Domenico Corapi
 */
public class GraknClientImpl implements GraknClient {

    private final Logger LOG = LoggerFactory.getLogger(GraknClientImpl.class);

    private final Client client;
    private final SimpleURI uri;

    GraknClientImpl(SimpleURI url)  {
        this.client = Client.create();
        this.client.setConnectTimeout(CONNECT_TIMEOUT_MS);
        this.client.setReadTimeout(CONNECT_TIMEOUT_MS * 2);
        this.uri = url;
    }

    @Override
    public List<QueryResponse> graqlExecute(List<Query<?>> queryList, Keyspace keyspace)
            throws GraknClientException {
        LOG.debug("Sending query list size {} to keyspace {}", queryList.size(), keyspace);

        String body = queryList.stream().map(Object::toString).reduce("; ", String::concat).substring(2);
        URI fullURI = UriBuilder.fromUri(uri.toURI())
                .path(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, keyspace.getValue()))
                .queryParam(ALLOW_MULTIPLE_QUERIES, true)
                .queryParam(EXECUTE_WITH_INFERENCE, false) //Making inference true could lead to non-deterministic loading of data
                .queryParam(LOADING_DATA, true) //Skip serialising responses for the sake of efficiency
                .queryParam(TX_TYPE, GraknTxType.BATCH)
                .build();
        ClientResponse response = client.resource(fullURI)
                .accept(APPLICATION_JSON)
                .post(ClientResponse.class, body);
        try {
            Response.StatusType status = response.getStatusInfo();
            String entity = response.getEntity(String.class);
            if (!status.getFamily().equals(Family.SUCCESSFUL)) {
                String queries = queryList.stream().map(Object::toString).collect(Collectors.joining("\n"));

                String error = Json.read(entity).at("exception").asString();
                throw new GraknClientException("Failed graqlExecute. Error status: " + status.getStatusCode() + ", error info: " + error + "\nqueries: " + queries, response.getStatusInfo());
            }
            LOG.debug("Received {}", status.getStatusCode());
            return queryList.stream().map(q -> QueryResponse.INSTANCE).collect(Collectors.toList());
        } finally {
            response.close();
        }
    }

    @Override
    public Optional<Keyspace> keyspace(String keyspace) throws GraknClientException {
        URI fullURI = UriBuilder.fromUri(uri.toURI())
                .path(REST.resolveTemplate(REST.WebPath.KB_KEYSPACE, keyspace))
                .build();
        ClientResponse response = client.resource(fullURI)
                .accept(APPLICATION_JSON)
                .get(ClientResponse.class);
        Response.StatusType status = response.getStatusInfo();
        LOG.debug("Received {}", status.getStatusCode());
        if (status.getStatusCode() == Status.NOT_FOUND.getStatusCode()) {
            return Optional.empty();
        }
        String entity = response.getEntity(String.class);
        if (!status.getFamily().equals(Family.SUCCESSFUL)) {
            throw new GraknClientException("Failed keyspace. Error status: " + status.getStatusCode() + ", error info: " + entity, response.getStatusInfo());
        }
        response.close();
        return Optional.of(Keyspace.of(keyspace));
    }
}
