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
