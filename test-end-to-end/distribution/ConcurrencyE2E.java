package grakn.core.distribution;

import grakn.client.GraknClient;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;

public class ConcurrencyE2E {

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

    /**
     * This is testing insertion of attributes which have values that repeat in different concurrent transactions.
     * The goal is to make sure we don't introduce ghost vertices when running merging attributes in a very
     * concurrent scenario.
     */

    @Test
    public void concurrentInsertionOfDuplicateAttributes_doesNotCreateGhostVertices() throws ExecutionException, InterruptedException {
        String[] names = new String[]{"Marco", "James", "Ganesh", "Haikal", "Kasper", "Tomas", "Joshua", "Max", "Syed", "Soroush"};
        String[] surnames = new String[]{"Surname1", "Surname2", "Surname3", "Surname4", "Surname5", "Surname6", "Surname7", "Surname8", "Surname9", "Surname10"};
        int[] ages = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        GraknClient graknClient = new GraknClient("localhost:48555");
        GraknClient.Session session = graknClient.session("concurrency");
        GraknClient.Transaction tx = session.transaction().write();
        tx.execute(Graql.parse("define " +
                "person sub entity, has name, has surname, has age; " +
                "name sub attribute, datatype string;" +
                "surname sub attribute, datatype string;" +
                "age sub attribute, datatype long;").asDefine());

        tx.commit();
        ExecutorService executorService = Executors.newFixedThreadPool(36);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        // We need a good amount of parallelism to have a good chance to spot possible issues. Don't use smaller values.
        int numberOfConcurrentTransactions = 56;
        int batchSize = 50;
        Random random = new Random();
        for (int i = 0; i < numberOfConcurrentTransactions; i++) {
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                GraknClient.Transaction threadTx = session.transaction().write();
                for (int j = 0; j < batchSize; j++) {
                    threadTx.execute(Graql.parse("insert $x isa person, has name \"" + names[random.nextInt(10)] + "\"," +
                            "has surname \"" + surnames[random.nextInt(10)] + "\"," +
                            "has age " + ages[random.nextInt(10)] + ";").asInsert());
                    }
                threadTx.commit();

                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }

        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();

        // Retrieve all the attribute values to make sure we don't have any person linked to a broken vertex.
        // This step is needed because it's only when retrieving attributes that we would be able to spot a
        // ghost vertex (which is might be introduced while merging 2 attribute nodes)
        tx = session.transaction().write();
        List<ConceptMap> conceptMaps = tx.execute(Graql.parse("match $x isa person; get;").asGet());
        conceptMaps.forEach(map -> {
            Collection<Concept> concepts = map.concepts();
            concepts.forEach(concept -> {
                Set<Attribute<?>> collect = concept.asThing().attributes().collect(toSet());
                collect.forEach(attribute -> {
                    String value = attribute.value().toString();
                });
            });
        });
        tx.close();


        tx = session.transaction().write();
        int numOfNames = tx.execute(Graql.parse("match $x isa name; get; count;").asGetAggregate()).get(0).number().intValue();
        int numOfSurnames = tx.execute(Graql.parse("match $x isa surname; get; count;").asGetAggregate()).get(0).number().intValue();
        int numOfAges = tx.execute(Graql.parse("match $x isa age; get; count;").asGetAggregate()).get(0).number().intValue();
        tx.close();

        assertEquals(10, numOfNames);
        assertEquals(10, numOfSurnames);
        assertEquals(10, numOfAges);
    }
}
