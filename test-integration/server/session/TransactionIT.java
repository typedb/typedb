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

package grakn.core.server.session;

import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.impl.TypeImpl;
import grakn.core.core.Schema;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import grakn.core.util.ConceptDowncasting;
import grakn.core.util.GraqlTestUtil;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.statement.Statement;
import junit.framework.TestCase;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static graql.lang.Graql.define;
import static graql.lang.Graql.insert;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class TransactionIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private Transaction tx;
    private Session session;

    @Before
    public void setUp() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        tx = session.writeTransaction();
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }

    @Test
    public void whenGettingConceptById_ReturnTheConcept() {
        EntityType entityType = tx.putEntityType("test-name");
        assertEquals(entityType, tx.getConcept(entityType.id()));
    }

    @Test
    public void whenGettingConceptWithInvalidId_Null() {
        assertNull(tx.getConcept(ConceptId.of("not_valid_id")));
    }

    @Test
    public void whenAttemptingToMutateViaTraversal_Throw() {
        expectedException.expect(VerificationException.class);
        expectedException.expectMessage("not read only");
        ((TestTransactionProvider.TestTransaction)tx).janusTraversalSourceProvider().getTinkerTraversal().V().drop().iterate();
    }

    @Test
    public void whenGettingAttributesByValue_ReturnTheMatchingAttributes() {
        String targetValue = "Geralt";
        assertTrue(tx.getAttributesByValue(targetValue).isEmpty());

        AttributeType<String> t1 = tx.putAttributeType("Parent 1", AttributeType.DataType.STRING);
        AttributeType<String> t2 = tx.putAttributeType("Parent 2", AttributeType.DataType.STRING);

        Attribute<String> r1 = t1.create(targetValue);
        Attribute<String> r2 = t2.create(targetValue);
        t2.create("Dragon");

        assertThat(tx.getAttributesByValue(targetValue), containsInAnyOrder(r1, r2));
    }

    @Test
    public void whenGettingTypesByName_ReturnTypes() {
        String entityTypeLabel = "My Entity Type";
        String relationTypeLabel = "My Relation Type";
        String roleTypeLabel = "My Role Type";
        String resourceTypeLabel = "My Attribute Type";
        String ruleTypeLabel = "My Rule Type";

        assertNull(tx.getEntityType(entityTypeLabel));
        assertNull(tx.getRelationType(relationTypeLabel));
        assertNull(tx.getRole(roleTypeLabel));
        assertNull(tx.getAttributeType(resourceTypeLabel));
        assertNull(tx.getRule(ruleTypeLabel));

        EntityType entityType = tx.putEntityType(entityTypeLabel);
        RelationType relationType = tx.putRelationType(relationTypeLabel);
        Role role = tx.putRole(roleTypeLabel);
        AttributeType attributeType = tx.putAttributeType(resourceTypeLabel, AttributeType.DataType.STRING);

        assertEquals(entityType, tx.getEntityType(entityTypeLabel));
        assertEquals(relationType, tx.getRelationType(relationTypeLabel));
        assertEquals(role, tx.getRole(roleTypeLabel));
        assertEquals(attributeType, tx.getAttributeType(resourceTypeLabel));
    }

    @Test
    public void whenGettingSubTypesFromRootMeta_IncludeAllTypes() {
        EntityType sampleEntityType = tx.putEntityType("Sample Entity Type");
        RelationType sampleRelationType = tx.putRelationType("Sample Relation Type");

        assertThat(tx.getMetaConcept().subs().collect(toSet()), containsInAnyOrder(
                tx.getMetaConcept(),
                tx.getMetaRelationType(),
                tx.getMetaEntityType(),
                tx.getMetaAttributeType(),
                sampleEntityType,
                sampleRelationType
        ));
    }

    @Test
    public void whenGettingTheShardingThreshold_TheCorrectValueIsReturned() {
        final long threshold = 333333L;
        Config mockServerConfig = storage.createCompatibleServerConfig();
        try(Session session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig, threshold)) {
            try (Transaction tx = session.readTransaction()) {
                assertEquals(threshold, ((TestTransactionProvider.TestTransaction)tx).shardingThreshold());
            }
        }
    }

    @Test
    public void whenBuildingAConceptFromAVertex_ReturnConcept() {
        EntityType et = tx.putEntityType("Sample Entity Type");
        assertEquals(et, ((TestTransactionProvider.TestTransaction)tx).conceptManager().buildConcept(ConceptDowncasting.concept(et).vertex()));
    }

    @Test
    public void whenPassingTxToAnotherThreadWithoutOpening_Throw() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newSingleThreadExecutor();

        expectedException.expectCause(IsInstanceOf.instanceOf(TransactionException.class));
        expectedException.expectMessage(TransactionException.notInOriginatingThread().getMessage());

        Future future = pool.submit(() -> {
            tx.putEntityType("A Thing");
        });
        future.get();
    }

    @Test
    public void attemptingToUseClosedTxFailsThenOpeningNewTx_EnsureTxIsUsable() throws InvalidKBException {
        tx.close();

        boolean errorThrown = false;
        try {
            tx.putEntityType("A Thing");
        } catch (TransactionException e) {
            if (e.getMessage().equals("The transaction for keyspace [" + tx.keyspace() + "] is closed. Use the session to get a new transaction for the graph.")) {
                errorThrown = true;
            }
        }
        assertTrue("Graph not correctly closed", errorThrown);

        tx = session.writeTransaction();
        tx.putEntityType("A Thing");
    }


    @Test
    public void whenClosingATxWhichWasJustCommitted_DoNothing() {
        tx.commit();
        assertTrue("Graph is still open after commit", !tx.isOpen());
        tx.close();
        assertTrue("Graph is somehow open after close", !tx.isOpen());
    }

    @Test
    public void whenCommittingATxWhichWasJustCommitted_DoNothing() {
        tx.commit();
        assertTrue("Graph is still open after commit", !tx.isOpen());
        tx.commit();
        assertTrue("Graph is somehow open after 2nd commit", !tx.isOpen());
    }

    @Test
    public void whenAttemptingToMutateSchemaWithReadOnlyTransaction_ThrowOnCommit() {
        tx.close();
        String entityType = "My Entity Type";
        String roleType1 = "My Role Type 1";
        String relationType1 = "My Relation Type 1";

        //Fail Some Mutations
        tx = session.readTransaction();
        tx.putEntityType(entityType);
        expectedException.expectMessage(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
        tx.commit();

        tx = session.readTransaction();
        tx.putRole(roleType1);
        expectedException.expectMessage(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
        tx.commit();

        tx = session.readTransaction();
        tx.putRelationType(relationType1);
        expectedException.expectMessage(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
        tx.commit();

    }

    @Test
    public void whenAttemptingToMutateInstancesWithReadOnlyTransaction_ThrowOnCommit() {
        tx.close();
        String entityType = "person";

        tx = session.writeTransaction();
        tx.putEntityType(entityType);
        tx.commit();

        tx = session.readTransaction();
        EntityType person = tx.getEntityType("person");
        Entity human = person.create();
        expectedException.expectMessage(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
        tx.commit();
    }

    @Test
    public void whenOpeningDifferentTypesOfTransactionsOnTheSameThread_Throw() {
        String keyspace = tx.keyspace().name();
        failAtOpeningTx(session, true, keyspace);
        tx.close();

        session.writeTransaction();
        failAtOpeningTx(session, false, keyspace);
    }

    private void failAtOpeningTx(Session session, boolean write, String keyspace) {
        Exception exception = null;
        try {
            //noinspection ResultOfMethodCallIgnored
            if (write) {
                session.writeTransaction();
            } else {
                session.readTransaction();
            }
        } catch (TransactionException e) {
            exception = e;
        }
        assertThat(exception, instanceOf(TransactionException.class));
        assertNotNull(exception);
        assertEquals(exception.getMessage(), ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(keyspace));
    }

    @Test
    public void whenShardingSuperNode_EnsureNewInstancesGoToNewShard() {
        EntityType entityType = tx.putEntityType("The Special Type");
        Shard s1 = ConceptDowncasting.type(entityType).currentShard();

        //Add 3 instances to first shard
        Entity s1_e1 = entityType.create();
        Entity s1_e2 = entityType.create();
        Entity s1_e3 = entityType.create();

        ConceptDowncasting.type(entityType).createShard();

        Shard s2 = ConceptDowncasting.type(entityType).currentShard();

        //Add 5 instances to second shard
        Entity s2_e1 = entityType.create();
        Entity s2_e2 = entityType.create();
        Entity s2_e3 = entityType.create();
        Entity s2_e4 = entityType.create();
        Entity s2_e5 = entityType.create();

        ConceptDowncasting.type(entityType).createShard();
        Shard s3 = ConceptDowncasting.type(entityType).currentShard();

        //Add 2 instances to 3rd shard
        Entity s3_e1 = entityType.create();
        Entity s3_e2 = entityType.create();

        //Check Type was sharded correctly
        assertThat(ConceptDowncasting.type(entityType).shards().collect(toSet()), containsInAnyOrder(s1, s2, s3));

        //Check shards have correct instances
        assertThat(s1.links().map(vertexElement -> tx.getConcept(Schema.conceptId(vertexElement.element()))).collect(toSet()),
                containsInAnyOrder(s1_e1, s1_e2, s1_e3));
        assertThat(s2.links().map(vertexElement -> tx.getConcept(Schema.conceptId(vertexElement.element()))).collect(toSet()),
                containsInAnyOrder(s2_e1, s2_e2, s2_e3, s2_e4, s2_e5));
        assertThat(s3.links().map(vertexElement -> tx.getConcept(Schema.conceptId(vertexElement.element()))).collect(toSet()),
                containsInAnyOrder(s3_e1, s3_e2));
    }

    @Test
    public void whenThresholdIsReachedForAGivenType_EnsureThatNewTypeShardIsCreated() throws IOException {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(mockServerConfig);
        String newKeyspaceName = "a" + UUID.randomUUID().toString().replaceAll("-", "");
        try (Session session = SessionUtil.serverlessSession(mockServerConfig,janusGraphFactory, newKeyspaceName,1L)) {
            try (Transaction tx = session.writeTransaction()) {
                tx.execute(define(type("person").sub("entity")).asDefine());
                tx.commit();
            }
        }
        Set<Vertex> typeShards;
        try (JanusGraph janusGraph = janusGraphFactory.openGraph(newKeyspaceName)) {
            JanusGraphTransaction tx = janusGraph.newTransaction();
            typeShards = tx.traversal().V().has(Schema.VertexProperty.SCHEMA_LABEL.name(), "person").in().hasLabel("SHARD").toSet();
            assertEquals(1, typeShards.size());
            tx.close();
        }
        ConceptId p1;
        try (Session session = SessionUtil.serverlessSession(mockServerConfig,janusGraphFactory, newKeyspaceName,1L)) {
            try (Transaction tx = session.writeTransaction()) {
                p1 = tx.execute(insert(var("p1").isa("person")).asInsert()).get(0).get("p1").id();
                tx.commit();
            }
        }
        Vertex typeShardForP1;
        try (JanusGraph janusGraph = janusGraphFactory.openGraph(newKeyspaceName)) {
            JanusGraphTransaction tx = janusGraph.newTransaction();
            typeShardForP1 = tx.traversal().V(p1.getValue().substring(1)).out(Schema.EdgeLabel.ISA.getLabel()).toList().get(0);
            assertEquals(typeShards.iterator().next(), typeShardForP1);
            typeShards = tx.traversal().V().has(Schema.VertexProperty.SCHEMA_LABEL.name(), "person").in().hasLabel("SHARD").toSet();
            assertEquals(2, typeShards.size());
            tx.close();
        }
        ConceptId p2;
        try (Session session = SessionUtil.serverlessSession(mockServerConfig,janusGraphFactory, newKeyspaceName,1L)) {
            try (Transaction tx = session.writeTransaction()) {
                p2 = tx.execute(insert(var("p2").isa("person")).asInsert()).get(0).get("p2").id();
                tx.commit();
            }
        }
        try (JanusGraph janusGraph = janusGraphFactory.openGraph(newKeyspaceName)) {
            JanusGraphTransaction tx = janusGraph.newTransaction();
            Vertex typeShardForP2 = tx.traversal().V(p2.getValue().substring(1)).out(Schema.EdgeLabel.ISA.getLabel()).toSet().iterator().next();
            assertEquals(Sets.difference(typeShards, Sets.newHashSet(typeShardForP1)).iterator().next(), typeShardForP2);
            typeShards = tx.traversal().V().has(Schema.VertexProperty.SCHEMA_LABEL.name(), "person").in().hasLabel("SHARD").toSet();
            assertEquals(3, typeShards.size());
            tx.close();
        }
    }

    @Test
    public void whenThresholdIsReachedForAGivenType_ensureThatTypeShardIsCreatedForThatTypeOnly() throws IOException {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(mockServerConfig);
        String newKeyspaceName = "a" + UUID.randomUUID().toString().replaceAll("-", "");
        try (Session session = SessionUtil.serverlessSession(mockServerConfig,janusGraphFactory, newKeyspaceName,1L)) {
            try (Transaction tx = session.writeTransaction()) {
                tx.execute(define(type("person").sub("entity")).asDefine());
                tx.execute(define(type("company").sub("entity")).asDefine());
                tx.commit();
            }
        }
        try (Session session = SessionUtil.serverlessSession(mockServerConfig,janusGraphFactory, newKeyspaceName,1L)) {
            try (Transaction tx = session.writeTransaction()) {
                tx.execute(insert(var("p").isa("person")).asInsert());
                tx.commit();
            }
        }
        try (Session session = SessionUtil.serverlessSession(mockServerConfig,janusGraphFactory, newKeyspaceName,1L)) {
            try (Transaction tx = session.writeTransaction()) {
                tx.execute(insert(var("p").isa("person")).asInsert());
                tx.commit();
            }
        }
        try (Session session = SessionUtil.serverlessSession(mockServerConfig,janusGraphFactory, newKeyspaceName,1L)) {
            try (Transaction tx = session.writeTransaction()) {
                tx.execute(insert(var("c").isa("company")).asInsert());
                tx.commit();
            }
        }
        try (JanusGraph janusGraph = janusGraphFactory.openGraph(newKeyspaceName)) {
            JanusGraphTransaction tx = janusGraph.newTransaction();
            Set<Vertex> personTypeShards = tx.traversal().V().has(Schema.VertexProperty.SCHEMA_LABEL.name(), "person").in().hasLabel("SHARD").toSet();
            assertEquals(3, personTypeShards.size());
            Set<Vertex> companyTypeShards = tx.traversal().V().has(Schema.VertexProperty.SCHEMA_LABEL.name(), "company").in().hasLabel("SHARD").toSet();
            assertEquals(2, companyTypeShards.size());
            tx.close();
        }
    }

    private void loadEntitiesConcurrentlyWithSpecificShardingThreshold(long shardingThreshold, int insertsPerCommit, long noOfEntities, int threads, double tol) throws ExecutionException, InterruptedException {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        Session session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig, shardingThreshold);

        String entityLabel = "someEntity";
        try(Transaction tx = session.writeTransaction()){
            tx.putEntityType(entityLabel);
            tx.commit();
        }
        List<Statement> statements = new ArrayList<>();
        for (int i = 0 ; i < noOfEntities ; i++){
            statements.add(Graql.var().isa(entityLabel));
        }
        GraqlTestUtil.insertStatementsConcurrently(session, statements, threads, insertsPerCommit);
        try(Transaction tx = session.writeTransaction()) {
            final long noOfConcepts = tx.execute(Graql.parse("compute count in someEntity;").asComputeStatistics()).get(0).number().longValue();
            TestCase.assertEquals(noOfEntities, noOfConcepts);
            //NB one extra shard comes from the fact that if we have <shardThreshold> number of instances we will have 2 shards (instance count equal to thresh triggers sharding)
            long expectedShards = noOfEntities/shardingThreshold + 1;
            TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction) tx);
            long createdShards = testTx.getShardCount(tx.getType(Label.of(entityLabel)));
            System.out.println("expected shards: " + expectedShards);
            System.out.println("created shards: " + createdShards);
            assertEquals(expectedShards, createdShards, tol*expectedShards);
        }
        assertFalse(session.shardManager().lockCandidatesPresent());
        assertFalse(session.shardManager().shardRequestsPresent());
        session.close();
    }

    @Test
    public void whenMultipleTXsCreateShards_shardingThresholdEqualToInsertSize_currentShardsDontGoOutOfSyncAndShardManagerIsEmptyAfterLoading() throws ExecutionException, InterruptedException {
        //NB: in this configuration each tx should be creating a shard on commit provided it no other tx creates a shard at the same time
        int threads = 16;
        loadEntitiesConcurrentlyWithSpecificShardingThreshold(200L,200, 80000, 16, 1.0/threads);
    }

    @Test
    public void whenMultipleTXsCreateShards_shardingThresholdMultipleOfInsertSize_currentShardsDontGoOutOfSyncAndShardManagerIsEmptyAfterLoading() throws ExecutionException, InterruptedException {
        //NB: in this configuration each thread creates a shard every 4 txs provided it no other tx creates a shard at the same time
        int threads = 16;
        loadEntitiesConcurrentlyWithSpecificShardingThreshold(400L,100, 80000, threads, 1.0/threads);
    }

    @Test
    public void whenMultipleTXsCreateShards_insertSizeMultipleOfShardingThreshold_currentShardsDontGoOutOfSyncAndShardManagerIsEmptyAfterLoading() throws ExecutionException, InterruptedException {
        int threads = 16;
        //NB: here within a single tx, we exceed the sharding threshold multiple times. In such scenario we don't create multiple shards - we create a single one.
        //Hence the final number of shards will be ~<shardingThreshold>/<insertsPerCommit> * <noOfEntities>/<shardingThreshold>
        long threshold = 100L;
        int insertsPerCommit = 400;
        double tol = 1.0 - (double )threshold/insertsPerCommit + 1.0/threads;
        loadEntitiesConcurrentlyWithSpecificShardingThreshold(100L, 400, 80000, threads, tol);
    }

    @Test
    public void whenMultipleTxsInsertAttributes_noGhostVerticesAreCreatedAndAttributeManagerIsEmptyAfterLoading() throws ExecutionException, InterruptedException {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        Session session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String entityLabel = "someEntity";
        String attributeLabel = "someAttribute";
        try(Transaction tx = session.writeTransaction()){
            AttributeType<Long> someAtttribute = tx.putAttributeType(attributeLabel, AttributeType.DataType.LONG);
            tx.putEntityType(entityLabel).has(someAtttribute);
            tx.commit();
        }
        final int insertsPerCommit = 200;
        final int noOfEntities = 100000;
        final int threads = 8;

        List<Statement> statements = new ArrayList<>();
        Random rand = new Random();
        Set<Integer> values = new HashSet<>();
        for (int i = 0 ; i < noOfEntities ; i++){
            int value = rand.nextInt(10);
            values.add(value);
            statements.add(Graql.var("x" + i).isa(attributeLabel).val(value));
        }
        GraqlTestUtil.insertStatementsConcurrently(session, statements, threads, insertsPerCommit);
        try(Transaction tx = session.writeTransaction()) {
            final long noOfAttributes = tx.execute(Graql.parse("compute count in someAttribute;").asComputeStatistics()).get(0).number().longValue();
            TestCase.assertEquals(values.size(), noOfAttributes);
        }

        assertFalse(session.attributeManager().lockCandidatesPresent());
        assertFalse(session.attributeManager().ephemeralAttributesPresent());
        session.close();
    }

    @Test
    public void whenCreatingAValidSchemaInSeparateThreads_EnsureValidationRulesHold() throws ExecutionException, InterruptedException {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        Session localSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            //Resources
            try (Transaction tx = localSession.writeTransaction()) {
                AttributeType<Long> int_ = tx.putAttributeType("int", AttributeType.DataType.LONG);
                AttributeType<Long> foo = tx.putAttributeType("foo", AttributeType.DataType.LONG).sup(int_);
                tx.putAttributeType("bar", AttributeType.DataType.LONG).sup(int_);
                tx.putEntityType("FOO").has(foo);

                tx.commit();
            }
        }).get();

        //Relation Which Has Resources
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("BAR").has(tx.getAttributeType("bar"));
            tx.commit();
        }
        localSession.close();
    }

    @Test
    public void whenShardingConcepts_EnsureCountsAreUpdated() {
        TypeImpl entity = ConceptDowncasting.type(tx.putEntityType("my amazing entity type"));
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction) tx);

        assertEquals(1L, testTx.getShardCount(entity));

        entity.createShard();
        assertEquals(2L, testTx.getShardCount(entity));
    }

    @Test
    public void whenGettingSupsOfASchemaConcept_ResultIncludesMetaThing() {
        EntityType yes = tx.putEntityType("yes");
        EntityType entity = tx.getMetaEntityType();
        Type thing = tx.getMetaConcept();
        Set<SchemaConcept> no = tx.sups(yes).collect(toSet());
        assertThat(no, containsInAnyOrder(yes, entity, thing));
        assertThat(tx.sups(entity).collect(toSet()), containsInAnyOrder(entity, thing));
        assertThat(tx.sups(thing).collect(toSet()), containsInAnyOrder(thing));
    }


    @Test
    public void insertAndDeleteRelationInSameTransaction_relationIsCorrectlyDeletedAndRolePlayersAreInserted(){
        tx.execute(Graql.parse("define person sub entity, plays friend; friendship sub relation, relates friend;").asDefine());
        tx.commit();
        tx = session.writeTransaction();
        String relId = tx.execute(Graql.parse("insert $x isa person; $y isa person; $r (friend: $x, friend: $y) isa friendship;").asInsert()).get(0).get("r").id().getValue();
        tx.execute(Graql.parse("match $r id " + relId + "; delete $r;").asDelete());
        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> rolePlayersResult = tx.execute(Graql.parse("match $x isa person; get;").asGet());
        assertEquals(2, rolePlayersResult.size());
        List<ConceptMap> relationResult = tx.execute(Graql.parse("match $r id " + relId + "; get;").asGet());
        assertEquals(0, relationResult.size());
    }

    @Test
    public void insertAndDeleteSameRelationInDifferentTransactions_relationIsCorrectlyDeletedAndRolePlayersAreInserted(){
        tx.execute(Graql.parse("define person sub entity, plays friend; friendship sub relation, relates friend;").asDefine());
        tx.commit();
        tx = session.writeTransaction();
        String relId = tx.execute(Graql.parse("insert $x isa person; $y isa person; $r (friend: $x, friend: $y) isa friendship;").asInsert()).get(0).get("r").id().getValue();
        tx.commit();
        tx = session.writeTransaction();
        tx.execute(Graql.parse("match $r id " + relId + "; delete $r;").asDelete());
        tx.commit();


        tx = session.writeTransaction();
        List<ConceptMap> rolePlayersResult = tx.execute(Graql.parse("match $x isa person; get;").asGet());
        assertEquals(2, rolePlayersResult.size());
        List<ConceptMap> relationResult = tx.execute(Graql.parse("match $r id " + relId + "; get;").asGet());
        assertEquals(0, relationResult.size());
    }

    @Test
    public void whenCommitingInferredConcepts_InferredConceptsAreNotPersisted(){
        tx.execute(Graql.<GraqlDefine>parse(
                    "define " +
                            "name sub attribute, datatype string;" +
                            "score sub attribute, datatype double;" +
                            "person sub entity, has name, has score;" +
                            "infer-attr sub rule," +
                            "when {" +
                            "  $p isa person, has score $s;" +
                            "  $s > 0.0;" +
                            "}, then {" +
                            "  $p has name 'Ganesh';" +
                            "};"
            ));
        tx.commit();

        tx = session.writeTransaction();
        tx.execute(Graql.<GraqlInsert>parse("insert $p isa person, has score 10.0;"));
        tx.commit();

        tx = session.writeTransaction();
        tx.execute(Graql.<GraqlGet>parse("match $p isa person, has name $n; get;"));
        tx.commit();

        tx = session.readTransaction();
        List<ConceptMap> answers = tx.execute(Graql.<GraqlGet>parse("match $p isa person, has name $n; get;"), false);
        assertTrue(answers.isEmpty());
    }

    @Test
    public void whenCommitingInferredAttributeEdge_EdgeIsNotPersisted(){
        tx.execute(Graql.<GraqlDefine>parse(
                "define " +
                        "score sub attribute, datatype double;" +
                        "person sub entity, has score;" +
                        "infer-attr sub rule," +
                        "when {" +
                        "  $p isa person, has score $s;" +
                        "  $q isa person; $q != $p;" +
                        "}, then {" +
                        "  $q has score $s;" +
                        "};"
        ));
        tx.commit();

        tx = session.writeTransaction();
        tx.execute(Graql.<GraqlInsert>parse("insert $p isa person, has score 10.0;"));
        tx.execute(Graql.<GraqlInsert>parse("insert $q isa person;"));
        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> answers = tx.execute(Graql.<GraqlGet>parse("match $p isa person, has score $score; get;"), true);
        assertEquals(2, answers.size());
        tx.commit();

        tx = session.readTransaction();
        answers = tx.execute(Graql.<GraqlGet>parse("match $p isa person, has score $score; get;"), false);
        assertEquals(1, answers.size());
    }

    @Test
    public void whenCommittingConceptsDependentOnInferredConcepts_conceptsAndDependantsArePersisted(){
        String inferrableSchema = "define " +
                "baseEntity sub entity, has inferrableAttribute, has nonInferrableAttribute, plays someRole, plays anotherRole;" +
                "someEntity sub baseEntity;" +
                "nonInferrableAttribute sub attribute, datatype string;" +
                "inferrableAttribute sub attribute, datatype string, plays anotherRole;" +
                "inferrableRelation sub relation, has nonInferrableAttribute, relates someRole, relates anotherRole;" +

                "infer-attr sub rule," +
                "when { $p isa someEntity; not{$p has nonInferrableAttribute 'nonInferred';};}, " +
                "then { $p has inferrableAttribute 'inferred';};" +

                "infer-relation sub rule," +
                "when { $p isa someEntity; $q isa someEntity, has inferrableAttribute $r; $r 'inferred';}, " +
                "then { (someRole: $p, anotherRole: $r) isa inferrableRelation;};";

        tx.execute(Graql.<GraqlDefine>parse(inferrableSchema));

        tx.execute(Graql.<GraqlInsert>parse(
                "insert " +
                        "$p isa someEntity, has nonInferrableAttribute 'nonInferred';" +
                        "$q isa someEntity;"
        ));
        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> relationsWithInferredRolePlayer = tx.execute(Graql.<GraqlInsert>parse(
                "match " +
                        "$p isa someEntity;" +
                        "$q isa someEntity, has inferrableAttribute $r; $r 'inferred';" +
                        "insert " +
                        "$rel (someRole: $p, anotherRole: $r) isa inferrableRelation;" +
                        "$rel has nonInferrableAttribute 'relation with inferred roleplayer';"
        ));

        List<ConceptMap> inferredRelationWithAttributeAttached = tx.execute(Graql.<GraqlInsert>parse(
                "match " +
                        "$rel (someRole: $p, anotherRole: $r) isa inferrableRelation;" +
                        "insert " +
                        "$rel has nonInferrableAttribute 'inferred relation label';"
        ));
        tx.commit();

        tx = session.readTransaction();
        List<ConceptMap> relationsWithInferredRolePlayerPostCommit = tx.execute(Graql.parse(
                "match " +
                        "$p isa someEntity;" +
                        "$q isa someEntity, has inferrableAttribute $r; $r 'inferred';" +
                        "$rel (someRole: $p, anotherRole: $r) isa inferrableRelation;" +
                        "$rel has nonInferrableAttribute 'relation with inferred roleplayer';" +
                        "get $rel, $p, $r;")
                .asGet(), false);

        List<ConceptMap> inferredRelationWithAttributeAttachedPostCommit = tx.execute(Graql.parse(
                "match " +
                "$rel (someRole: $p, anotherRole: $r) isa inferrableRelation;" +
                "$rel has nonInferrableAttribute 'inferred relation label'; get $rel;")
                .asGet(), false);
        tx.close();

        assertCollectionsNonTriviallyEqual(relationsWithInferredRolePlayer, relationsWithInferredRolePlayerPostCommit);
        assertCollectionsNonTriviallyEqual(inferredRelationWithAttributeAttached, inferredRelationWithAttributeAttachedPostCommit);
    }

    @Test
    public void whenInsertedConceptsDependOnAChainOfInferredConcepts_conceptsAndDependantsArePersisted(){
        String inferrableSchema = "define " +
                "someEntity sub entity, plays someRole, plays anotherRole;" +
                "anotherEntity sub entity, plays someRole, plays anotherRole;" +
                "nonInferredRelation sub relation, relates someRole, relates anotherRole;" +
                "inferredRelation sub relation, relates someRole, relates anotherRole, plays someRole;" +
                "anotherInferredRelation sub relation, relates someRole, relates anotherRole, plays someRole;" +
                "yetAnotherInferredRelation sub relation, relates someRole, relates anotherRole, plays someRole;" +

                "infer-relation sub rule," +
                "when { $p isa someEntity; $q isa anotherEntity;}, " +
                "then { (someRole: $p, anotherRole: $q) isa inferredRelation;};" +

                "infer-anotherRelation sub rule," +
                "when { $p isa inferredRelation; $q isa anotherEntity;}, " +
                "then { (someRole: $p, anotherRole: $q) isa anotherInferredRelation;};" +

                "infer-yetAnotherRelation sub rule," +
                "when { $p isa anotherInferredRelation; $q isa anotherEntity;}, " +
                "then { (someRole: $p, anotherRole: $q) isa yetAnotherInferredRelation;};";

        tx.execute(Graql.<GraqlDefine>parse(inferrableSchema));

        tx.execute(Graql.<GraqlInsert>parse(
                "insert " +
                        "$p isa someEntity;" +
                        "$q isa anotherEntity;"
        ));
        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> relationsWithInferredRolePlayer = tx.execute(Graql.<GraqlInsert>parse(
                "match " +
                        "$finalRel (someRole: $p, anotherRole: $q) isa yetAnotherInferredRelation;" +
                        "insert " +
                        "(someRole: $finalRel, anotherRole: $q) isa nonInferredRelation;"
        ));
        tx.commit();

        tx = session.readTransaction();
        List<ConceptMap> relationsWithInferredRolePlayerPostCommit = tx.execute(Graql.parse(
                "match " +
                        "$rel  (someRole: $p, anotherRole: $q) isa inferredRelation;" +
                        "$anotherRel (someRole: $rel, anotherRole: $q) isa anotherInferredRelation;" +
                        "$finalRel (someRole: $anotherRel, anotherRole: $q) isa yetAnotherInferredRelation;" +
                        "(someRole: $finalRel, anotherRole: $q) isa nonInferredRelation;" +
                        "get $finalRel, $q;")
                .asGet(), false);
        tx.close();

        assertCollectionsNonTriviallyEqual(relationsWithInferredRolePlayer, relationsWithInferredRolePlayerPostCommit);
    }

    @Test
    public void whenPersistingInferredConcepts_theyHaveInferredFlagSetToFalse(){
        String inferrableSchema = "define " +
                "baseEntity sub entity, has inferrableAttribute, has nonInferrableAttribute, plays someRole, plays anotherRole;" +
                "someEntity sub baseEntity;" +
                "nonInferrableAttribute sub attribute, datatype string;" +
                "inferrableAttribute sub attribute, datatype string, plays anotherRole;" +
                "someRelation sub relation, has nonInferrableAttribute, relates someRole, relates anotherRole;" +

                "infer-attr sub rule," +
                "when { $p isa someEntity; not{$p has nonInferrableAttribute 'nonInferred';};}, " +
                "then { $p has inferrableAttribute 'inferred';};";

        tx.execute(Graql.<GraqlDefine>parse(inferrableSchema));

        tx.execute(Graql.<GraqlInsert>parse(
                "insert " +
                        "$p isa someEntity, has nonInferrableAttribute 'nonInferred';" +
                        "$q isa someEntity;"
        ));
        tx.commit();

        tx = session.writeTransaction();
        tx.execute(Graql.<GraqlInsert>parse(
                "match " +
                        "$p isa someEntity;" +
                        "$q isa someEntity, has inferrableAttribute $r; $r 'inferred';" +
                        "insert " +
                        "$rel (someRole: $p, anotherRole: $r) isa someRelation;" +
                        "$rel has nonInferrableAttribute 'relation with inferred roleplayer';"
        ));
        tx.commit();

        tx = session.readTransaction();
        tx.execute(Graql.<GraqlGet>parse(
                "match " +
                        "$rel (someRole: $p, anotherRole: $r) isa someRelation;" +
                        "$rel has nonInferrableAttribute 'relation with inferred roleplayer';" +
                "get;"
        ))
                .forEach(ans -> ans.concepts().stream()
                        .filter(Concept::isThing)
                        .map(Concept::asThing)
                        .forEach(c -> assertFalse(c.isInferred())));
        tx.close();
    }

    @Test
    public void whenInferredConceptsAreCommitted_ifWeRequeryThemInAnotherTxTheyWontBeDeleted(){
        String inferrableSchema = "define " +
                "baseEntity sub entity, has inferrableAttribute, has nonInferrableAttribute, plays someRole, plays anotherRole;" +
                "someEntity sub baseEntity;" +
                "nonInferrableAttribute sub attribute, datatype string;" +
                "inferrableAttribute sub attribute, datatype string, plays anotherRole;" +
                "someRelation sub relation, has nonInferrableAttribute, relates someRole, relates anotherRole;" +

                "infer-attr sub rule," +
                "when { $p isa someEntity; not{$p has nonInferrableAttribute 'nonInferred';};}, " +
                "then { $p has inferrableAttribute 'inferred';};";

        tx.execute(Graql.<GraqlDefine>parse(inferrableSchema));

        tx.execute(Graql.<GraqlInsert>parse(
                "insert " +
                        "$p isa someEntity, has nonInferrableAttribute 'nonInferred';" +
                        "$q isa someEntity;"
        ));
        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> relationsWithInferredRolePlayer = tx.execute(Graql.<GraqlInsert>parse(
                "match " +
                        "$p isa someEntity;" +
                        "$q isa someEntity, has inferrableAttribute $r; $r 'inferred';" +
                        "insert " +
                        "$rel (someRole: $p, anotherRole: $r) isa someRelation;" +
                        "$rel has nonInferrableAttribute 'relation with inferred roleplayer';"
        ));

        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> relationsWithInferredRolePlayerPostCommitWithoutInference = tx.execute(Graql.parse(
                "match $r isa inferrableAttribute; get;")
                .asGet(), false);
        List<ConceptMap> relationsWithInferredRolePlayerPostCommit = tx.execute(Graql.parse(
                "match $r isa inferrableAttribute; get;")
                .asGet());
        assertCollectionsNonTriviallyEqual(relationsWithInferredRolePlayerPostCommitWithoutInference, relationsWithInferredRolePlayerPostCommit);
        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> relationsWithInferredRolePlayerRequeriedWithoutInference = tx.execute(Graql.parse(
                "match $r isa inferrableAttribute; get;")
                .asGet(), false);
        List<ConceptMap> relationsWithInferredRolePlayerRequeried = tx.execute(Graql.parse(
                "match $r isa inferrableAttribute; get;")
                .asGet());
        assertCollectionsNonTriviallyEqual(relationsWithInferredRolePlayerPostCommit, relationsWithInferredRolePlayerRequeriedWithoutInference);
        assertCollectionsNonTriviallyEqual(relationsWithInferredRolePlayerPostCommit, relationsWithInferredRolePlayerRequeried);
        tx.close();
    }

}
