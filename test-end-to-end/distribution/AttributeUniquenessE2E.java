/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.distribution;


import grakn.client.GraknClient;
import graql.lang.Graql;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static grakn.core.distribution.DistributionE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static grakn.core.distribution.DistributionE2EConstants.assertGraknIsNotRunning;
import static grakn.core.distribution.DistributionE2EConstants.assertGraknIsRunning;
import static grakn.core.distribution.DistributionE2EConstants.assertZipExists;
import static grakn.core.distribution.DistributionE2EConstants.unzipGrakn;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static org.junit.Assert.assertEquals;

public class AttributeUniquenessE2E {
    private static Logger LOG = LoggerFactory.getLogger(AttributeUniquenessE2E.class);
    private GraknClient localhostGrakn = new GraknClient("localhost:48555");

    private static ProcessExecutor commandExecutor = new ProcessExecutor()
            .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertZipExists();
        unzipGrakn();
        assertGraknIsNotRunning();
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknIsNotRunning();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    @Test
    public void shouldMergeAttributesInConcurrentTransactions() throws InterruptedException, ExecutionException {
        int numOfUniqueNames = 10;
        int numOfDuplicatesPerName = 673;
        ExecutorService executorServiceForParallelInsertion = Executors.newFixedThreadPool(8);

        try (GraknClient.Session session = localhostGrakn.session("attribute_merging_e2e")) {
            // insert attributes with duplicates
            LOG.info("defining the schema...");
            defineParentChildSchema(session);
            LOG.info("inserting " + numOfUniqueNames + " unique attributes with " + numOfDuplicatesPerName + " duplicates per attribute....");
            insertNameShuffled(session, numOfUniqueNames, numOfDuplicatesPerName, executorServiceForParallelInsertion);

            LOG.info("verifying the number of attributes");
            int countAfterMerging = countTotalNames(session);
            assertEquals(numOfUniqueNames, countAfterMerging);
            LOG.info("test completed successfully. there are " + countAfterMerging + " unique names found");
        }
    }

    private void defineParentChildSchema(GraknClient.Session session) {
        try (GraknClient.Transaction tx = session.transaction().write()) {
            List<grakn.client.answer.ConceptMap> answer = tx.execute(Graql.define(
                    type("name").sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type("parent").sub("role"),
                    type("child").sub("role"),
                    type("person").sub("entity").has("name").plays("parent").plays("child"),
                    type("parentchild").sub("relation").relates("parent").relates("child")));
            tx.commit();
        }
    }

    private static void insertNameShuffled(GraknClient.Session session, int nameCount, int duplicatePerNameCount, ExecutorService executorService)
            throws ExecutionException, InterruptedException {

        List<String> duplicatedNames = new ArrayList<>();
        for (int i = 0; i < nameCount; ++i) {
            for (int j = 0; j < duplicatePerNameCount; ++j) {
                String name = "lorem ipsum dolor sit amet " + i;
                duplicatedNames.add(name);
            }
        }

        Collections.shuffle(duplicatedNames, new Random(1));

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (String name : duplicatedNames) {
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                try (GraknClient.Transaction tx = session.transaction().write()) {
                    List<grakn.client.answer.ConceptMap> answer = tx.execute(Graql.insert(var().isa("name").val(name)));
                    tx.commit();
                }
                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }

        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();
    }

    private int countTotalNames(GraknClient.Session session) {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            return tx.execute(Graql.match(var("x").isa("name")).get().count()).get(0).number().intValue();
        }
    }
}
