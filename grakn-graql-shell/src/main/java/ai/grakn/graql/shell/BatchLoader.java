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

package ai.grakn.graql.shell;

import ai.grakn.Keyspace;
import ai.grakn.client.BatchExecutorClient;
import ai.grakn.client.GraknClient;
import ai.grakn.graql.Graql;
import ai.grakn.util.SimpleURI;
import com.google.common.base.Charsets;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
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

    static void sendBatchRequest(
            SimpleURI uri, Keyspace keyspace, Path graqlPath, PrintStream sout, PrintStream serr
    ) throws IOException {

        AtomicInteger queriesExecuted = new AtomicInteger(0);

        try (FileInputStream inputStream = new FileInputStream(graqlPath.toFile());
             Reader queryReader = new InputStreamReader(inputStream, Charsets.UTF_8);
             BatchExecutorClient batchExecutorClient = loaderClient(uri)
        ) {
            batchExecutorClient.onNext(queryResponse -> queriesExecuted.incrementAndGet());
            batchExecutorClient.onError(serr::println);

            Graql.parser().parseReader(queryReader).forEach(query -> batchExecutorClient.add(query, keyspace));
        }

        sout.println("Statements executed: " + queriesExecuted.get());

    }

    private static BatchExecutorClient loaderClient(SimpleURI uri) {
        return BatchExecutorClient.newBuilder()
                .threadPoolCoreSize(Runtime.getRuntime().availableProcessors() * 8)
                .taskClient(GraknClient.of(uri))
                .maxRetries(DEFAULT_MAX_RETRY)
                .build();
    }
}
