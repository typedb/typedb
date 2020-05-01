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
 *
 */

package grakn.core.server.uniqueness;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.server.uniqueness.element.AttributeElement;
import grakn.core.server.uniqueness.element.Element;
import grakn.core.server.uniqueness.element.Record;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

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
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;

public class ConcurrencyIT {
    private Session session;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() {
        session.close();
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

        Transaction tx = session.transaction(Transaction.Type.WRITE);
        tx.execute(Graql.parse("define " +
                "person sub entity, has name, has surname, has age; " +
                "name sub attribute, value string;" +
                "surname sub attribute, value string;" +
                "age sub attribute, value long;").asDefine());

        tx.commit();
        ExecutorService executorService = Executors.newFixedThreadPool(36);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        // We need a good amount of parallelism to have a good chance to spot possible issues. Don't use smaller values.
        int numberOfConcurrentTransactions = 56;
        int batchSize = 50;
        Random random = new Random();
        for (int i = 0; i < numberOfConcurrentTransactions; i++) {
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                Transaction threadTx = session.transaction(Transaction.Type.WRITE);
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
        tx = session.transaction(Transaction.Type.WRITE);
        List<ConceptMap> conceptMaps = tx.execute(Graql.parse("match $x isa person; get;").asGet());
        Transaction innerTx = tx;
        conceptMaps.forEach(map -> {
            Collection<Concept> concepts = map.map().values();
            concepts.forEach(concept -> {
                Set<Attribute<?>> collect = concept.asThing().attributes().collect(toSet());
                collect.forEach(attribute -> {
                    String value = attribute.value().toString();
                });
            });
        });
        tx.close();


        tx = session.transaction(Transaction.Type.WRITE);
        int numOfNames = tx.execute(Graql.parse("match $x isa name; get; count;").asGetAggregate()).get(0).number().intValue();
        int numOfSurnames = tx.execute(Graql.parse("match $x isa surname; get; count;").asGetAggregate()).get(0).number().intValue();
        int numOfAges = tx.execute(Graql.parse("match $x isa age; get; count;").asGetAggregate()).get(0).number().intValue();
        tx.close();

        assertEquals(10, numOfNames);
        assertEquals(10, numOfSurnames);
        assertEquals(10, numOfAges);
    }

    @Test
    public void concurrentInsertionOfMixedAttributeLoad_doesNotCreateGhostVertices() throws ExecutionException, InterruptedException {
        final int noOfAttributes = 10;

        try(Transaction tx = session.transaction(Transaction.Type.WRITE)){
            tx.stream(Graql.parse("match $x isa thing;get;").asGet()).forEach(ans -> ans.get("x").delete());
            EntityType someEntity = tx.putEntityType("someEntity");
            someEntity.has(tx.putAttributeType("attribute0",  AttributeType.ValueType.INTEGER));
            someEntity.has(tx.putAttributeType("attribute1",  AttributeType.ValueType.STRING));
            for(int attributeNo = 2; attributeNo < noOfAttributes ; attributeNo++){
                someEntity.has(tx.putAttributeType("attribute" + attributeNo,  AttributeType.ValueType.STRING));
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

        Transaction tx = session.transaction(Transaction.Type.WRITE);
        final long noOfConcepts = tx.execute(Graql.parse("compute count in thing;").asComputeStatistics()).get(0).number().longValue();
        final long insertedAttributes = tx.execute(Graql.parse("compute count in attribute;").asComputeStatistics()).get(0).number().longValue();
        tx.close();
        final long totalTime = System.currentTimeMillis() - start;
        System.out.println("Concepts: " + noOfConcepts + " totalTime: " + totalTime + " throughput: " + noOfConcepts*1000*60/(totalTime));
        assertEquals(new HashSet<>(attributes).size(), insertedAttributes);
        session.close();
    }

    private <T extends Element> void insertElements(Session session, List<T> elements,
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
                Transaction tx = session.transaction(Transaction.Type.WRITE);
                int inserted = 0;
                for (int elementId = 0; elementId < subList.size(); elementId++) {
                    T element = subList.get(elementId);
                    GraqlInsert insert = Graql.insert(element.patternise(Graql.var("x" + elementId).var()).statements());
                    tx.execute(insert);
                    if (inserted % insertsPerCommit == 0) {
                        tx.commit();
                        inserted = 0;
                        tx = session.transaction(Transaction.Type.WRITE);
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
}
