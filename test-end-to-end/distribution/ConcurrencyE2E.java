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
import grakn.client.answer.ConceptMap;
import grakn.client.concept.Attribute;
import grakn.client.concept.AttributeType;
import grakn.client.concept.Concept;
import grakn.client.concept.EntityType;
import grakn.core.distribution.element.AttributeElement;
import grakn.core.distribution.element.Element;
import grakn.core.distribution.element.Record;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
                    threadTx.execute(Graql.parse("insert " +
                            "$x isa person, has name \"" + names[random.nextInt(10)] + "\"," +
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
            Collection<Concept> concepts = map.map().values();
            concepts.forEach(concept -> {
                Set<Attribute<?>> collect = (Set<Attribute<?>>) concept.asThing().attributes().collect(toSet());
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

    private <T extends Element> void insertElements(GraknClient.Session session, List<T> elements,
                                                    int threads, int insertsPerCommit) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        int listSize = elements.size();
        int listChunk = listSize / threads + 1;

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            boolean lastChunk = threadNo == threads - 1;
            final int startIndex = threadNo * listChunk;
            int endIndex = (threadNo + 1) * listChunk;
            if (endIndex > listSize && lastChunk) endIndex = listSize;

            List<T> subList = elements.subList(startIndex, endIndex);
            System.out.println("indices: " + startIndex + "-" + endIndex + " , size: " + subList.size());
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                long start2 = System.currentTimeMillis();
                GraknClient.Transaction tx = session.transaction().write();
                int inserted = 0;
                for (int elementId = 0; elementId < subList.size(); elementId++) {
                    T element = subList.get(elementId);
                    GraqlInsert insert = Graql.insert(element.patternise(Graql.var("x" + elementId).var()).statements());
                    tx.execute(insert);
                    if (inserted % insertsPerCommit == 0) {
                        tx.commit();
                        inserted = 0;
                        tx = session.transaction().write();
                    }
                    inserted++;
                }
                tx.commit();
                System.out.println("Thread: " + Thread.currentThread().getId() + " elements: " + subList.size() + " time: " + (System.currentTimeMillis() - start2));
                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }
        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();
        executorService.shutdown();
    }

    private static List<Record> generateRecords(int size, int noOfAttributes){
        List<Record> records = new ArrayList<>();
        for(int i = 0 ; i < size ; i++){
            List<AttributeElement> attributes = new ArrayList<>();
            attributes.add(new AttributeElement("attribute0", i));
            attributes.add(new AttributeElement("attribute1", i % 2 ==0? "even" : "odd"));
            for(int attributeNo = 2; attributeNo < noOfAttributes ; attributeNo++){
                attributes.add(new AttributeElement("attribute" + attributeNo, RandomStringUtils.random(attributeNo+5, true, true)));
            }
            records.add(new Record("someEntity", attributes));
        }
        return records;
    }

    @Test
    public void concurrentInsertionOfMixedAttributeLoad_doesNotCreateGhostVertices() throws ExecutionException, InterruptedException {
        GraknClient graknClient = new GraknClient("localhost:48555");
        GraknClient.Session session = graknClient.session("mixed_attribute_load");
        final int noOfAttributes = 10;

        try(GraknClient.Transaction tx = session.transaction().write()){
            tx.stream(Graql.parse("match $x isa thing;get;").asGet()).forEach(ans -> ans.get("x").delete());
            EntityType someEntity = tx.putEntityType("someEntity");
            someEntity.has(tx.putAttributeType("attribute0", AttributeType.DataType.INTEGER));
            someEntity.has(tx.putAttributeType("attribute1", AttributeType.DataType.STRING));
            for(int attributeNo = 2; attributeNo < noOfAttributes ; attributeNo++){
                someEntity.has(tx.putAttributeType("attribute" + attributeNo, AttributeType.DataType.STRING));
            }
            tx.commit();
        }

        final int insertsPerCommit = 5000;
        final int noOfRecords = 80000;
        final int threads = 8;
        List<Record> records = generateRecords(noOfRecords, noOfAttributes);
        List<AttributeElement> attributes = records.stream().flatMap(r -> r.getAttributes().stream()).collect(Collectors.toList());
        System.out.println("total attributes: " + attributes.size());

        final long start = System.currentTimeMillis();
        insertElements(session, attributes, threads, insertsPerCommit);

        GraknClient.Transaction tx = session.transaction().write();
        final long noOfConcepts = tx.execute(Graql.parse("compute count in thing;").asComputeStatistics()).get(0).number().longValue();
        final long insertedAttributes = tx.execute(Graql.parse("compute count in attribute;").asComputeStatistics()).get(0).number().longValue();
        tx.close();
        final long totalTime = System.currentTimeMillis() - start;
        System.out.println("Concepts: " + noOfConcepts + " totalTime: " + totalTime + " throughput: " + noOfConcepts*1000*60/(totalTime));
        assertEquals(new HashSet<>(attributes).size(), insertedAttributes);
        session.close();
    }
}
