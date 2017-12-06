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
 *
 */

package ai.grakn.test.client;

import ai.grakn.Keyspace;
import ai.grakn.client.BatchExecutorClient;
import ai.grakn.client.GraknClient;
import ai.grakn.client.GraknClientException;
import ai.grakn.client.QueryResponse;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mjson.Json;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchExecutorClientTest {

    @Test
    public void whenBatchExecutorClientCloses_AllTasksHaveCompleted() throws GraknClientException {
        Set<Query<?>> queriesExecuted = Sets.newConcurrentHashSet();
        Keyspace keyspace = Keyspace.of("yes");

        GraknClient graknClient = mock(GraknClient.class);

        when(graknClient.graqlExecute(any(), eq(keyspace))).thenAnswer(args -> {
            List<Query<?>> queries = args.getArgument(0);

            queriesExecuted.addAll(queries);

            return Lists.transform(queries, query -> {
                assert query != null;
                return new QueryResponse(query, Json.object());
            });
        });

        // Make sure there are more queries to execute than are allowed to run at once
        int maxQueries = 10;
        int numQueries = 100;

        BatchExecutorClient.Builder clientBuilder =
                BatchExecutorClient.newBuilder().taskClient(graknClient).maxQueries(maxQueries);

        Set<Query<?>> queriesToExecute =
                IntStream.range(0, numQueries).mapToObj(this::createInsertQuery).collect(toImmutableSet());

        try (BatchExecutorClient client = clientBuilder.build()) {
            for (Query<?> query : queriesToExecute) {
                // If we don't subscribe, the query won't execute
                client.add(query, keyspace).subscribe(a -> {});
            }
        }

        assertThat(queriesExecuted, containsInAnyOrder(queriesToExecute));
    }

    private InsertQuery createInsertQuery(int i) {
        return insert(var("x").id(ConceptId.of("V" + i)));
    }
}
