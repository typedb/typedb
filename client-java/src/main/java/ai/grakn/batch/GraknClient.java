/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.batch;

import ai.grakn.Keyspace;
import ai.grakn.graql.Query;
import ai.grakn.util.SimpleURI;

import java.util.List;
import java.util.Optional;

/**
 * Grakn http client. Extend this for more http endpoint.
 *
 * @author Domenico Corapi
 */
public interface GraknClient {
    int CONNECT_TIMEOUT_MS = 30 * 1000;
    int DEFAULT_MAX_RETRY = 3;

    static GraknClient of(SimpleURI url) {
        return new GraknClientImpl(url);
    }

    List<QueryResponse> graqlExecute(List<Query<?>> queryList, Keyspace keyspace) throws GraknClientException;

    Optional<Keyspace> keyspace(String keyspace) throws GraknClientException;
}
