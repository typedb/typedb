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

package ai.grakn.graql.shell;

import ai.grakn.Keyspace;
import ai.grakn.batch.BatchExecutorClient;
import ai.grakn.batch.GraknClient;
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
            batchExecutorClient.onError(e -> e.printStackTrace(serr));

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
