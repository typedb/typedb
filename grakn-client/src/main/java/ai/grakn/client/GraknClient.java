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

import ai.grakn.Keyspace;
import ai.grakn.graql.Query;
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.Status.Family;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import static ai.grakn.util.REST.Request.Graql.MULTI;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;

/**
 * Grakn http client. Extend this for more http endpoint.
 *
 * @author Domenico Corapi
 */
public class GraknClient {

    public static final int CONNECT_TIMEOUT_MS = 5 * 60 * 1000;
    public static final int DEFAULT_MAX_RETRY = 3;
    private final Logger LOG = LoggerFactory.getLogger(GraknClient.class);

    private final Client client;
    private final SimpleURI uri;

    public GraknClient(SimpleURI url)  {
        this.client = Client.create();
        this.client.setConnectTimeout(CONNECT_TIMEOUT_MS);
        this.client.setReadTimeout(CONNECT_TIMEOUT_MS * 2);
        this.uri = url;
    }

    public List<QueryResponse> graqlExecute(List<Query<?>> queryList, Keyspace keyspace)
            throws GraknClientException {
        String body = queryList.stream().map(Object::toString).reduce("; ", String::concat).substring(2);
        URI fullURI = UriBuilder.fromUri(uri.toURI())
                .path(REST.resolveTemplate(REST.WebPath.KB.ANY_GRAQL, keyspace.getValue()))
                .queryParam(MULTI, true)
                .build();
        ClientResponse response = client.resource(fullURI)
                .accept(APPLICATION_JSON_GRAQL)
                .post(ClientResponse.class, body);
        try {
            Response.StatusType status = response.getStatusInfo();
            String entity = response.getEntity(String.class);
            if (!status.getFamily().equals(Family.SUCCESSFUL)) {
                throw new GraknClientException("Failed graqlExecute. Error status: " + status.getStatusCode() + ", error info: " + entity, response.getStatusInfo());
            }
            LOG.debug("Received {}", status.getStatusCode());
            return QueryResponse.from(queryList, entity);
        } finally {
            response.close();
        }
    }

    public Optional<Keyspace> keyspace(String keyspace) throws GraknClientException {
        URI fullURI = UriBuilder.fromUri(uri.toURI())
                .path(REST.resolveTemplate(REST.WebPath.System.KB_KEYSPACE, keyspace))
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
