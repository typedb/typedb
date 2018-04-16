/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */
package ai.grakn.client;

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
