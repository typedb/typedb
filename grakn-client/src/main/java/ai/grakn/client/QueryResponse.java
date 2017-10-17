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
import java.util.ArrayList;
import java.util.List;
import mjson.Json;

/**
 * Encapsulates a response. (TODO) We should migrate this away from a json
 *
 * @author Domenico Corapi
 */
public class QueryResponse {
    private final Query<?> query;
    private final Json jsonResponse;

    public QueryResponse(Query<?> query, Json jsonResponse) {
        this.query = query;
        this.jsonResponse = jsonResponse;
    }

    public Json getJsonResponse() {
        return jsonResponse;
    }

    public Query<?> getQuery() {
        return query;
    }

    public static List<QueryResponse> from(List<Query<?>> queries, String response) {
        List<Json> json = Json.read(response).asJsonList();
        ArrayList<QueryResponse> result = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            result.add(new QueryResponse(queries.get(i), json.get(i)));
        }
        return result;
    }
}
