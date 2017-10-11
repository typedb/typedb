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

import ai.grakn.graql.Query;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.Graql.MULTI;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import ai.grakn.util.SimpleURI;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.http.client.utils.URIBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Response;

/**
 * Grakn http client. Extend this for more http endpoint.
 *
 * @author Domenico Corapi
 */
public class GraknClient {

    private final DefaultAsyncHttpClient asyncHttpClient;
    private final URI graqlExecuteURL;

    public GraknClient(URL url)  {
        this.asyncHttpClient = new DefaultAsyncHttpClient();
        try {
            this.graqlExecuteURL = new URI(url.toString() + "/kb/graql/execute");
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unexpected error while processing url " + url);
        }
    }

    public GraknClient(SimpleURI url) {
        this(url.toURL());
    }

    public CompletableFuture<List<QueryResponse>> graqlExecute(List<Query<?>> queryList, String keyspace) {
        String body = queryList.stream().map(Object::toString).reduce("; ", String::concat).substring(2);
        try {
            URI fullURI = new URIBuilder(graqlExecuteURL)
                    .setParameter(MATERIALISE, "false")
                    .setParameter(INFER, "false")
                    .setParameter(MULTI, "true")
                    .setParameter(KEYSPACE, keyspace).build();
            return asyncHttpClient.preparePost(fullURI.toString())
                    .setHeader("Accept", APPLICATION_JSON_GRAQL)
                    .setBody(body)
                    .execute()
                    .toCompletableFuture()
                    .thenApply((Response queries) -> {
                        int statusCode = queries.getStatusCode();
                        if (statusCode != 200) {
                            throw new GraknClientException("Failed graqlExecute. Error status " + statusCode);
                        }
                        return QueryResponse.from(queryList, queries);
                    });
        } catch (URISyntaxException e) {
            throw new RuntimeException("Malformed URI");
        }
    }
}
