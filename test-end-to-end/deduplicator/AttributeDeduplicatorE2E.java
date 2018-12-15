package grakn.core.deduplicator;


import grakn.core.client.GraknClient;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.query.Graql;
import grakn.core.server.Transaction;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static grakn.core.deduplicator.AttributeDeduplicatorE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static grakn.core.deduplicator.AttributeDeduplicatorE2EConstants.assertGraknRunning;
import static grakn.core.deduplicator.AttributeDeduplicatorE2EConstants.assertGraknStopped;
import static grakn.core.deduplicator.AttributeDeduplicatorE2EConstants.assertZipExists;
import static grakn.core.deduplicator.AttributeDeduplicatorE2EConstants.unzipGrakn;
import static grakn.core.graql.query.Graql.count;
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AttributeDeduplicatorE2E {
    private static Logger LOG = LoggerFactory.getLogger(AttributeDeduplicatorE2E.class);
    private GraknClient localhostGrakn = new GraknClient("localhost:48555");
    private Path queuePath = GRAKN_UNZIPPED_DIRECTORY.resolve("db").resolve("queue");

    private static ProcessExecutor commandExecutor = new ProcessExecutor()
            .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertZipExists();
        unzipGrakn();
        assertGraknStopped();
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknRunning();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    @Test
    public void shouldDeduplicateAttributes() throws RocksDBException, InterruptedException, ExecutionException {
        int numOfUniqueNames = 10;
        int numOfDuplicatesPerName = 673;
        ExecutorService executorServiceForParallelInsertion = Executors.newFixedThreadPool(8);

        LOG.info("initiating the shouldDeduplicate10AttributesWithDuplicates test...");
        try (GraknClient.Session session = localhostGrakn.session("attribute_deduplicator_e2e")) {
            // insert 10 attributes, each with 100 duplicates
            LOG.info("defining the schema...");
            defineParentChildSchema(session);
            LOG.info("inserting " + numOfUniqueNames + " unique attributes with " + numOfDuplicatesPerName + " duplicates per attribute....");
            insertNameShuffled(session, numOfUniqueNames, numOfDuplicatesPerName, executorServiceForParallelInsertion);

            // wait until queue is empty
            LOG.info("names and duplicates have been inserted. waiting for the deduplication to finish...");
            long timeoutMs = 60000;
            long pollFrequencyMs = 2000;
            waitUntilAllAttributesDeduplicated(timeoutMs, pollFrequencyMs);
            LOG.info("deduplication has finished.");

            // verify deduplicated attributes
            LOG.info("verifying the number of attributes");
            int countAfterDeduplication = countTotalNames(session);
            assertThat(countAfterDeduplication, equalTo(numOfUniqueNames));
            LOG.info("test completed successfully. there are " + countAfterDeduplication + " unique names found");
        }
    }

    private void defineParentChildSchema(GraknClient.Session session) {
        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            List<ConceptMap> answer = tx.execute(Graql.define(
                    label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("parent").sub("role"),
                    label("child").sub("role"),
                    label("person").sub("entity").has("name").plays("parent").plays("child"),
                    label("parentchild").sub("relationship").relates("parent").relates("child")));
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
        for (String name: duplicatedNames) {
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                    List<ConceptMap> answer = tx.execute(Graql.insert(var().isa("name").val(name)));
                    tx.commit();
                }
                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }

        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[] {})).get();
    }

    private void waitUntilAllAttributesDeduplicated(long timeoutMs, long pollFrequencyMs) throws RocksDBException, InterruptedException {
        long startMs = System.currentTimeMillis();
        int queueSize = countRemainingItemsInQueue(queuePath);
        while (queueSize > 0) {
            LOG.info("deduplication in progress. there are " + queueSize + " attributes left to process.");
            Thread.sleep(pollFrequencyMs);
            long elapsedMs = System.currentTimeMillis() - startMs;
            if (elapsedMs > timeoutMs) {
                String message = "waitUntilAllAttributesDeduplicated - Timeout of '" + timeoutMs + "ms has been exceeded. There are '" + queueSize + "' items remaining in the queue.";
                throw new RuntimeException(message);
            }
            queueSize = countRemainingItemsInQueue(queuePath);
        }
    }

    /**
     * Count the number of elements in the queue
     *
     * @param queuePath the queue to be checked
     * @return the number of elements in the queue
     */
    private int countRemainingItemsInQueue(Path queuePath) throws RocksDBException {
        RocksDB queue = RocksDB.openReadOnly(new Options(), queuePath.toAbsolutePath().toString());
        RocksIterator it = queue.newIterator();
        it.seekToFirst();
        int count = 0;
        while (it.isValid()) {
            it.next();
            count++;
        }
        queue.close();
        return count;
    }

    private int countTotalNames(GraknClient.Session session) {
        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
            return tx.execute(Graql.match(var("x").isa("name")).aggregate(count())).get(0).number().intValue();
        }
    }
}
