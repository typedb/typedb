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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.shell;

import ai.grakn.Keyspace;
import ai.grakn.client.BatchExecutorClient;
import ai.grakn.client.GraknClient;
import ai.grakn.client.QueryResponse;
import ai.grakn.graql.Graql;
import ai.grakn.util.SimpleURI;
import com.google.common.base.Charsets;
import rx.Observable;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for sending batch load requests from the {@link GraqlShell}.
 *
 * @author Felix Chapman
 */
final class BatchLoader {

    private static final int DEFAULT_MAX_RETRY = 1;

    static void sendBatchRequest(SimpleURI uri, Keyspace keyspace, Path graqlPath) throws IOException {
        AtomicInteger queriesExecuted = new AtomicInteger(0);
        FileInputStream inputStream = new FileInputStream(graqlPath.toFile());

        try (BatchExecutorClient batchExecutorClient = loaderClient(uri);
             Reader queryReader = new InputStreamReader(inputStream, Charsets.UTF_8)
        ) {
            Graql.parser().parseList(queryReader).forEach(query -> {
                Observable<QueryResponse> observable = batchExecutorClient.add(query, keyspace, false);

                observable.subscribe(
                    /* On success: */ queryResponse -> queriesExecuted.incrementAndGet(),
                    /* On error:   */ System.err::println
                );
            });
        }

        System.out.println("Statements executed: " + queriesExecuted.get());

    }

    private static BatchExecutorClient loaderClient(SimpleURI uri) {
        return BatchExecutorClient.newBuilder()
                .threadPoolCoreSize(Runtime.getRuntime().availableProcessors() * 8)
                .taskClient(GraknClient.of(uri))
                .maxRetries(DEFAULT_MAX_RETRY)
                .build();
    }
}
