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

package grakn.core.server.session.cache;

import grakn.client.GraknClient;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.Role;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertTrue;

/**
 * Tests expected behavior of shared JanusGraph and caching between sessions to the same keyspace
 * as well as synchronizing the KeyspaceCache on commits
 */
@SuppressWarnings("Duplicates")
public class KeyspaceCacheIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();
    private SessionImpl localSession;
    private GraknClient.Session remoteSession;
    private GraknClient graknClient;

    @Before
    public void setUp() {
        localSession = server.sessionWithNewKeyspace();
        graknClient = new GraknClient(server.grpcUri());
        remoteSession = graknClient.session(localSession.keyspace().name());
    }

    @After
    public void tearDown() {
        localSession.close();
        remoteSession.close();
        graknClient.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithSameLocalSession(){
        try (TransactionOLTP tx = localSession.transaction().write()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (TransactionOLTP tx = localSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
    }


    /**
     * Check if schema is accessible in new local session.
     * The new session in created AFTER the schema has been defined
     */
    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionAfterSchemaIsDefined(){
        try (TransactionOLTP tx = localSession.transaction().write()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        SessionImpl testSession = server.sessionFactory().session(localSession.keyspace());
        try (TransactionOLTP tx = testSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    /**
     * Check if schema is accessible in new local session.
     * The new session in created BEFORE the schema has been defined
     */
    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionBeforeSchemaIsDefined(){
        SessionImpl testSession = server.sessionFactory().session(localSession.keyspace());
        try (TransactionOLTP tx = localSession.transaction().write()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (TransactionOLTP tx = testSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionClosingPreviousOne(){
        try (TransactionOLTP tx = localSession.transaction().write()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        localSession.close();
        SessionImpl testSession = server.sessionFactory().session(localSession.keyspace());
        try (TransactionOLTP tx = testSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithRemoteSession(){
        try (TransactionOLTP tx = localSession.transaction().write()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (GraknClient.Transaction tx = remoteSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
            assertTrue(entityTypeSubs.contains("animal"));
        }
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewRemoteSession(){
        try (TransactionOLTP tx = localSession.transaction().write()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        GraknClient.Session testSession = new GraknClient(server.grpcUri()).session(localSession.keyspace().name());
        try (GraknClient.Transaction tx = testSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewRemoteSessionClosingPreviousOne(){
        try (TransactionOLTP tx = localSession.transaction().write()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        remoteSession.close();
        GraknClient.Session testSession = graknClient.session(localSession.keyspace().name());
        try (GraknClient.Transaction tx = testSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void fun() throws ExecutionException, InterruptedException {
        TransactionOLTP tx = localSession.transaction().write();
        tx.execute(Graql.parse("define " +
                "person sub entity, plays friend, has name, has surname; " +
                "friendship sub relation, relates friend; " +
                "name sub attribute, datatype string;" +
                "surname sub attribute, datatype string;").asDefine());

        tx.commit();
        ExecutorService executorService = Executors.newFixedThreadPool(16);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 36; i++) {
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                Random random = new Random();
                GraknClient.Transaction remoteTx = remoteSession.transaction().write();
                for (int j = 0; j < 200; j++) {
                    remoteTx.execute(Graql.parse("insert $x isa person, has name \"" + getName(random.nextInt(10)) + "\";").asInsert());
                    remoteTx.execute(Graql.parse("insert $x isa person, has surname \"" + getSurname(random.nextInt(10)) + "\";").asInsert());
                }
                remoteTx.commit();

                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }

        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();
        GraknClient.Transaction remoteTx = remoteSession.transaction().write();
        List<ConceptMap> conceptMaps = remoteTx.execute(Graql.parse("match $x isa person; get;").asGet());
        conceptMaps.forEach(map -> {
            Collection<Concept> concepts = map.concepts();
            concepts.forEach(concept -> {
                Set<Attribute<?>> collect = concept.asThing().attributes().collect(toSet());
                collect.forEach(attribute -> System.out.println(attribute.value()));
            });
        });
        remoteTx.close();
        long end = System.currentTimeMillis();


//        int sizeNames = tx.execute(Graql.parse("match $x isa name; get $x; count;").asGetAggregate()).get(0).number().intValue();
//        int sizeFriendships = tx.execute(Graql.parse("match $x isa friendship; get $x; count;").asGetAggregate()).get(0).number().intValue();
//
//        System.out.println(size);
//        System.out.println(sizeNames);
//        System.out.println(sizeFriendships);
        System.out.println((end - start));
    }

    private String getName(int index){
        String[] namez = new String[] {"giulio", "pino", "ano", "navarro", "edemeclezio", "mortacci tua", "chissa' qual'e' il cazzo di problema", "otto", "nove", "dieci"};
        return  namez[index];
    }

    private String getSurname(int index){
        String[] namez = new String[] {"rossi", "bianchi", "fumagalli", "decilli", "boccadifuoco", "fuocodibocca", "undicimila", "trentatretrentini", "entrarono", "tuttitrotterellando"};
        return  namez[index];
    }
}
