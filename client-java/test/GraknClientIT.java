/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package grakn.core.client.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.client.GraknClient;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.AttributeType.DataType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.printer.Printer;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.SessionException;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.SessionImpl;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static graql.lang.Graql.Token.Compute.Algorithm.CONNECTED_COMPONENT;
import static graql.lang.Graql.Token.Compute.Algorithm.DEGREE;
import static graql.lang.Graql.Token.Compute.Algorithm.K_CORE;
import static graql.lang.Graql.and;
import static graql.lang.Graql.rel;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Integration Tests for Grakn Client and Server through RPC communication
 */
@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class GraknClientIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl localSession;
    private static GraknClient.Session remoteSession;

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private GraknClient graknClient;

    @Before
    public void setUp() {
        localSession = server.sessionWithNewKeyspace();
        graknClient = new GraknClient(server.grpcUri().toString());
        remoteSession = graknClient.session(localSession.keyspace().name());
    }

    @After
    public void tearDown() {
        localSession.close();
        remoteSession.close();
        graknClient.close();
    }

    @Test
    public void testOpeningASession_ReturnARemoteGraknSession() {
        try (Session session = graknClient.session(localSession.keyspace().name())) {
            assertTrue(GraknClient.Session.class.isAssignableFrom(session.getClass()));
        }
    }

    @Test
    public void testOpeningASessionWithAGivenUriAndKeyspace_TheUriAndKeyspaceAreSet() {
        try (Session session = graknClient.session(localSession.keyspace().name())) {
            assertEquals(localSession.keyspace(), session.keyspace());
        }
    }

    @Test
    public void testOpeningATransactionFromASession_ReturnATransactionWithParametersSet() {
        try (Session session = graknClient.session(localSession.keyspace().name())) {
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                assertEquals(session, tx.session());
                assertEquals(localSession.keyspace(), tx.keyspace());
                assertEquals(Transaction.Type.READ, tx.type());
            }
        }
    }

    @Test
    public void testPuttingEntityType_EnsureItIsAdded() {
        String label = "Oliver";
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType(label);
            tx.commit();
        }

        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            assertNotNull(tx.getEntityType(label));
        }
    }

    @Test
    public void testGettingEntityType_EnsureItIsReturned() {
        String label = "Oliver";
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType(label);
            tx.commit();
        }

        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            assertNotNull(tx.getEntityType(label));
        }
    }

    @Test
    public void testExecutingAndCommittingAQuery_TheQueryIsCommitted() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(type("person").sub("entity")));
            tx.commit();
        }

        try (Transaction tx = localSession.transaction(Transaction.Type.READ)) {
            assertNotNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void testExecutingAQueryAndNotCommitting_TheQueryIsNotCommitted() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(type("flibflab").sub("entity")));
        }

        try (Transaction tx = localSession.transaction(Transaction.Type.READ)) {
            assertNull(tx.getEntityType("flibflab"));
        }
    }

    @Test
    public void testExecutingAQuery_ResultsAreReturned() {
        List<ConceptMap> answers;

        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.READ)) {
            answers = tx.execute(Graql.match(var("x").sub("thing")).get());
        }

        int size;
        try (Transaction tx = localSession.transaction(Transaction.Type.READ)) {
            size = tx.execute(Graql.match(var("x").sub("thing")).get()).size();
        }

        assertThat(answers, hasSize(size));

        try (Transaction tx = localSession.transaction(Transaction.Type.READ)) {
            for (ConceptMap answer : answers) {
                assertThat(answer.vars(), contains(new Variable("x")));
                assertNotNull(tx.getConcept(answer.get("x").id()));
            }
        }
    }

    @Test
    @Ignore // TODO: complete with richer relation structures
    public void testGetQueryForRelation() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            List<ConceptMap> directorships = tx.execute(Graql.match(var("x").isa("directed-by")).get());

            for (ConceptMap directorship : directorships) {
                System.out.println(Printer.stringPrinter(true).toString(directorship.get("x")));
            }
        }
    }

    @Test
    public void testExecutingAQuery_ExplanationsAreReturned() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(
                    type("name").sub("attribute").datatype("string"),
                    type("content").sub("entity").has("name").plays("contained").plays("container"),
                    type("contains").sub("relation").relates("contained").relates("container"),
                    type("transitive-location").sub("rule")
                    .when(and(
                            rel("contained", "x").rel("container", "y").isa("contains"),
                            rel("contained", "y").rel("container", "z").isa("contains")
                    ))
                    .then(rel("contained", "x").rel("container", "z").isa("contains"))
            ));
            tx.execute(Graql.insert(
                    var("x").isa("content").has("name", "x"),
                    var("y").isa("content").has("name", "y"),
                    var("z").isa("content").has("name", "z"),
                    rel("contained", "x").rel("container", "y").isa("contains"),
                    rel("contained", "y").rel("container", "z").isa("contains")
            ));
            tx.commit();
        }
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            List<Pattern> patterns = Lists.newArrayList(
                    Graql.var("x").isa("content").has("name", "x"),
                    var("z").isa("content").has("name", "z"),
                    var("infer").rel("x").rel("z").isa("contains")
            );
            ConceptMap answer = Iterables.getOnlyElement(tx.execute(Graql.match(patterns).get()));
            final int ruleStatements = tx.getRule("transitive-location").when().statements().size();

            assertEquals(patterns.size() + ruleStatements, answer.explanation().deductions().size());
            assertEquals(patterns.size(), answer.explanation().getAnswers().size());
            answer.explanation().getAnswers().stream()
                    .filter(a -> a.containsVar(var("infer").var()))
                    .forEach(a -> assertEquals(ruleStatements, a.explanation().getAnswers().size()));
            testExplanation(answer);
        }
    }

    private void testExplanation(ConceptMap answer) {
        answerHasConsistentExplanations(answer);
        checkAnswerConnectedness(answer);
    }

    private void checkAnswerConnectedness(ConceptMap answer) {
        List<ConceptMap> answers = answer.explanation().getAnswers();
        answers.forEach(a -> {
            TestCase.assertTrue("Disconnected answer in explanation",
                                answers.stream()
                                        .filter(a2 -> !a2.equals(a))
                                        .anyMatch(a2 -> !Sets.intersection(a.vars(), a2.vars()).isEmpty())
            );
        });
    }

    private void answerHasConsistentExplanations(ConceptMap answer) {
        Set<ConceptMap> answers = answer.explanation().deductions().stream()
                .filter(a -> a.explanation().getPattern() != null)
                .collect(Collectors.toSet());

        answers.forEach(a -> TestCase.assertTrue("Answer has inconsistent explanations", explanationConsistentWithAnswer(a)));
    }

    private boolean explanationConsistentWithAnswer(ConceptMap ans){
        Pattern queryPattern = ans.explanation().getPattern();
        Set<Variable> vars = new HashSet<>();
        if (queryPattern != null){
            queryPattern.statements().forEach(s -> vars.addAll(s.variables()));
        }
        return vars.containsAll(ans.map().keySet());
    }

    @Test
    public void testExecutingTwoSequentialQueries_ResultsAreTheSame() {
        Set<ConceptMap> answers1;
        Set<ConceptMap> answers2;

        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.READ)) {
            answers1 = tx.stream(Graql.match(var("x").sub("thing")).get()).collect(toSet());
            answers2 = tx.stream(Graql.match(var("x").sub("thing")).get()).collect(toSet());
        }

        assertEquals(answers1, answers2);
    }

    @Test
    public void testExecutingTwoParallelQueries_GetBothResults() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.READ)) {
            GraqlGet query = Graql.match(var("x").sub("thing")).get();

            Iterator<ConceptMap> iterator1 = tx.stream(query).iterator();
            Iterator<ConceptMap> iterator2 = tx.stream(query).iterator();

            while (iterator1.hasNext() || iterator2.hasNext()) {
                assertEquals(iterator1.next(), iterator2.next());
                assertEquals(iterator1.hasNext(), iterator2.hasNext());
            }
        }
    }


    @Test
    public void testGettingAConcept_TheInformationOnTheConceptIsCorrect() {
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").isa("thing")).get();

            remoteTx.stream(query).forEach(answer -> {
                Concept remoteConcept = answer.get("x");
                Concept localConcept = localTx.getConcept(remoteConcept.id());

                assertEquals(localConcept.isAttribute(), remoteConcept.isAttribute());
                assertEquals(localConcept.isAttributeType(), remoteConcept.isAttributeType());
                assertEquals(localConcept.isEntity(), remoteConcept.isEntity());
                assertEquals(localConcept.isEntityType(), remoteConcept.isEntityType());
                assertEquals(localConcept.isRelation(), remoteConcept.isRelation());
                assertEquals(localConcept.isRelationType(), remoteConcept.isRelationType());
                assertEquals(localConcept.isRole(), remoteConcept.isRole());
                assertEquals(localConcept.isRule(), remoteConcept.isRule());
                assertEquals(localConcept.isSchemaConcept(), remoteConcept.isSchemaConcept());
                assertEquals(localConcept.isThing(), remoteConcept.isThing());
                assertEquals(localConcept.isType(), remoteConcept.isType());
                assertEquals(localConcept.id(), remoteConcept.id());
                assertEquals(localConcept.isDeleted(), remoteConcept.isDeleted());
            });
        }
    }

    @Test
    public void testExecutingDeleteQueries_ConceptsAreDeleted() {
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType name = tx.putAttributeType("name", DataType.STRING);
            AttributeType email = tx.putAttributeType("email", DataType.STRING);
            Role actor = tx.putRole("actor");
            Role characterBeingPlayed = tx.putRole("character-being-played");
            RelationType hasCast = tx.putRelationType("has-cast").relates(actor).relates(characterBeingPlayed);
            person.key(email).has(name);
            person.plays(actor).plays(characterBeingPlayed);

            Entity marco = person.create().has(name.create("marco")).has(email.create("marco@yolo.com"));
            Entity luca = person.create().has(name.create("luca")).has(email.create("luca@yolo.com"));
            hasCast.create().assign(actor, marco).assign(characterBeingPlayed, luca);
            tx.commit();
        }
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            GraqlDelete deleteQuery = Graql.match(var("g").rel("x").rel("y").isa("has-cast")).delete("x", "y");
            tx.execute(deleteQuery);
            assertTrue(tx.execute(Graql.match(var().rel("x").rel("y").isa("has-cast")).get("x", "y")).isEmpty());

            deleteQuery = Graql.match(var("x").isa("person")).delete();
            tx.execute(deleteQuery);
            assertTrue(tx.execute(Graql.match(var("x").isa("person")).get()).isEmpty());
        }
    }


    @Test
    public void testGettingARelation_TheInformationOnTheRelationIsCorrect() {
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType name = tx.putAttributeType("name", DataType.STRING);
            AttributeType email = tx.putAttributeType("email", DataType.STRING);
            Role actor = tx.putRole("actor");
            Role characterBeingPlayed = tx.putRole("character-being-played");
            RelationType hasCast = tx.putRelationType("has-cast").relates(actor).relates(characterBeingPlayed);
            person.key(email).has(name);
            person.plays(actor).plays(characterBeingPlayed);

            Entity marco = person.create().has(name.create("marco")).has(email.create("marco@yolo.com"));
            Entity luca = person.create().has(name.create("luca")).has(email.create("luca@yolo.com"));
            hasCast.create().assign(actor, marco).assign(characterBeingPlayed, luca);
            tx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").isa("has-cast")).get();
            Relation remoteConcept = remoteTx.stream(query).findAny().get().get("x").asRelation();
            Relation localConcept = localTx.getConcept(remoteConcept.id()).asRelation();

            assertEqualConcepts(localConcept, remoteConcept, Relation::rolePlayers);

            ImmutableMultimap.Builder<ConceptId, ConceptId> localRolePlayers = ImmutableMultimap.builder();
            localConcept.rolePlayersMap().forEach((role, players) -> {
                for (Thing player : players) {
                    localRolePlayers.put(role.id(), player.id());
                }
            });

            ImmutableMultimap.Builder<ConceptId, ConceptId> remoteRolePlayers = ImmutableMultimap.builder();
            remoteConcept.rolePlayersMap().forEach((role, players) -> {
                for (Thing player : players) {
                    remoteRolePlayers.put(role.id(), player.id());
                }
            });

            assertEquals(localRolePlayers.build(), remoteRolePlayers.build());
        }
    }


    @Test
    public void testGettingASchemaConcept_TheInformationOnTheSchemaConceptIsCorrect() {
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            EntityType human = tx.putEntityType("human");
            EntityType man = tx.putEntityType("man").sup(human);
            tx.putEntityType("child").sup(man);
            tx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").type("man")).get();
            SchemaConcept remoteConcept = remoteTx.stream(query).findAny().get().get("x").asSchemaConcept();
            SchemaConcept localConcept = localTx.getConcept(remoteConcept.id()).asSchemaConcept();

            assertEquals(localConcept.isImplicit(), remoteConcept.isImplicit());
            assertEquals(localConcept.label(), remoteConcept.label());
            assertEquals(localConcept.sup().id(), remoteConcept.sup().id());
            assertEqualConcepts(localConcept, remoteConcept, SchemaConcept::sups);
            assertEqualConcepts(localConcept, remoteConcept, SchemaConcept::subs);
        }
    }

    @Test
    public void testGettingAThing_TheInformationOnTheThingIsCorrect() {
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType name = tx.putAttributeType("name", DataType.STRING);
            AttributeType email = tx.putAttributeType("email", DataType.STRING);
            Role actor = tx.putRole("actor");
            Role characterBeingPlayed = tx.putRole("character-being-played");
            RelationType hasCast = tx.putRelationType("has-cast").relates(actor).relates(characterBeingPlayed);
            person.key(email).has(name);
            person.plays(actor).plays(characterBeingPlayed);

            Entity marco = person.create().has(name.create("marco")).has(email.create("marco@yolo.com"));
            Entity luca = person.create().has(name.create("luca")).has(email.create("luca@yolo.com"));
            hasCast.create().assign(actor, marco).assign(characterBeingPlayed, luca);
            tx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").isa("person")).get();
            Thing remoteConcept = remoteTx.stream(query).findAny().get().get("x").asThing();
            Thing localConcept = localTx.getConcept(remoteConcept.id()).asThing();

            assertEquals(localConcept.isInferred(), remoteConcept.isInferred());
            assertEquals(localConcept.type().id(), remoteConcept.type().id());
            assertEqualConcepts(localConcept, remoteConcept, Thing::attributes);
            assertEqualConcepts(localConcept, remoteConcept, Thing::keys);
//            assertEqualConcepts(localConcept, remoteConcept, Thing::plays); // TODO: re-enable when #19630 is fixed
            assertEqualConcepts(localConcept, remoteConcept, Thing::relations);
        }
    }

    @Test
    public void testGettingAType_TheInformationOnTheTypeIsCorrect() {
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            Role productionWithCast = tx.putRole("production-with-cast");
            Role actor = tx.putRole("actor");
            Role characterBeingPlayed = tx.putRole("character-being-played");
            tx.putRelationType("has-cast").relates(productionWithCast).relates(actor).relates(characterBeingPlayed);
            EntityType person = tx.putEntityType("person").plays(actor).plays(characterBeingPlayed);

            person.has(tx.putAttributeType("gender", DataType.STRING));
            person.has(tx.putAttributeType("name", DataType.STRING));

            person.create();
            person.create();
            tx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").type("person")).get();
            Type remoteConcept = remoteTx.stream(query).findAny().get().get("x").asType();
            Type localConcept = localTx.getConcept(remoteConcept.id()).asType();

            assertEquals(localConcept.isAbstract(), remoteConcept.isAbstract());
            assertEqualConcepts(localConcept, remoteConcept, Type::playing);
            assertEqualConcepts(localConcept, remoteConcept, Type::instances);
            assertEqualConcepts(localConcept, remoteConcept, Type::attributes);
            assertEqualConcepts(localConcept, remoteConcept, Type::keys);
        }
    }

    @Test
    public void testGettingARole_TheInformationOnTheRoleIsCorrect() {
        try (Transaction localTx = localSession.transaction(Transaction.Type.WRITE)) {
            Role productionWithCast = localTx.putRole("production-with-cast");
            Role actor = localTx.putRole("actor");
            Role characterBeingPlayed = localTx.putRole("character-being-played");
            localTx.putRelationType("has-cast")
                    .relates(productionWithCast).relates(actor).relates(characterBeingPlayed);
            localTx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").type("actor")).get();
            Role remoteConcept = remoteTx.stream(query).findAny().get().get("x").asRole();
            Role localConcept = localTx.getConcept(remoteConcept.id()).asRole();

            assertEqualConcepts(localConcept, remoteConcept, Role::players);
            assertEqualConcepts(localConcept, remoteConcept, Role::relations);
        }
    }

    @Test
    public void testGettingARule_TheInformationOnTheRuleIsCorrect() {
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putAttributeType("name", DataType.STRING);
            Pattern when = Graql.parsePattern("$x has name 'expectation-when';");
            Pattern then = Graql.parsePattern("$x has name 'expectation-then';");

            tx.putRule("expectation-rule", when, then);

            when = Graql.parsePattern("$x has name 'materialize-when';");
            then = Graql.parsePattern("$x has name 'materialize-then';");
            tx.putRule("materialize-rule", when, then);
            tx.commit();
        }

        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").type("expectation-rule")).get();
            grakn.core.concept.type.Rule remoteConcept = remoteTx.stream(query).findAny().get().get("x").asRule();
            grakn.core.concept.type.Rule localConcept = localTx.getConcept(remoteConcept.id()).asRule();

            assertEquals(localConcept.when(), remoteConcept.when());
            assertEquals(localConcept.then(), remoteConcept.then());
        }
    }

    @Test
    public void testGettingAnEntityType_TheInformationOnTheEntityTypeIsCorrect() {
        try (Transaction localTx = localSession.transaction(Transaction.Type.WRITE)) {
            localTx.putEntityType("person");
            localTx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").type("person")).get();
            EntityType remoteConcept = remoteTx.stream(query).findAny().get().get("x").asEntityType();
            EntityType localConcept = localTx.getConcept(remoteConcept.id()).asEntityType();

            // There actually aren't any new methods on EntityType, but we should still check we can get them
            assertEquals(localConcept.id(), remoteConcept.id());
        }
    }

    @Test
    public void testGettingARelationType_TheInformationOnTheRelationTypeIsCorrect() {
        try (Transaction localTx = localSession.transaction(Transaction.Type.WRITE)) {
            Role productionWithCast = localTx.putRole("production-with-cast");
            Role actor = localTx.putRole("actor");
            Role characterBeingPlayed = localTx.putRole("character-being-played");
            localTx.putRelationType("has-cast")
                    .relates(productionWithCast).relates(actor).relates(characterBeingPlayed);
            localTx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").type("has-cast")).get();
            RelationType remoteConcept = remoteTx.stream(query).findAny().get().get("x").asRelationType();
            RelationType localConcept = localTx.getConcept(remoteConcept.id()).asRelationType();

            assertEqualConcepts(localConcept, remoteConcept, RelationType::roles);
        }
    }


    @Test
    public void testGettingAnAttributeType_TheInformationOnTheAttributeTypeIsCorrect() {
        try (Transaction localTx = localSession.transaction(Transaction.Type.WRITE)) {
            AttributeType title = localTx.putAttributeType("title", DataType.STRING);
            title.create("The Muppets");
            localTx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").type("title")).get();
            AttributeType<String> remoteConcept = remoteTx.stream(query).findAny().get().get("x").asAttributeType();
            AttributeType<String> localConcept = localTx.getConcept(remoteConcept.id()).asAttributeType();

            assertEquals(localConcept.dataType(), remoteConcept.dataType());
            assertEquals(localConcept.regex(), remoteConcept.regex());
            assertEquals(
                    localConcept.attribute("The Muppets").id(),
                    remoteConcept.attribute("The Muppets").id()
            );
        }
    }

    @Test
    public void testGettingAnEntity_TheInformationOnTheEntityIsCorrect() {
        try (Transaction localTx = localSession.transaction(Transaction.Type.WRITE)) {
            EntityType movie = localTx.putEntityType("movie");
            movie.create();
            localTx.commit();
        }
        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").isa("movie")).get();
            Entity remoteConcept = remoteTx.stream(query).findAny().get().get("x").asEntity();
            Entity localConcept = localTx.getConcept(remoteConcept.id()).asEntity();

            // There actually aren't any new methods on Entity, but we should still check we can get them
            assertEquals(localConcept.id(), remoteConcept.id());
        }
    }

    @Test
    public void testGettingAnAttribute_TheInformationOnTheAttributeIsCorrect() {
        try (Transaction localTx = localSession.transaction(Transaction.Type.WRITE)) {
            EntityType person = localTx.putEntityType("person");
            AttributeType name = localTx.putAttributeType("name", DataType.STRING);
            person.has(name);
            Attribute alice = name.create("Alice");
            person.create().has(alice);
            localTx.commit();
        }

        try (Transaction remoteTx = remoteSession.transaction(Transaction.Type.READ);
             Transaction localTx = localSession.transaction(Transaction.Type.READ)
        ) {
            GraqlGet query = Graql.match(var("x").isa("name")).get();
            Attribute<?> remoteConcept = remoteTx.stream(query).findAny().get().get("x").asAttribute();
            Attribute<?> localConcept = localTx.getConcept(remoteConcept.id()).asAttribute();

            assertEquals(localConcept.dataType(), remoteConcept.dataType());
            assertEquals(localConcept.value(), remoteConcept.value());
            assertEquals(localConcept.owner().id(), remoteConcept.owner().id());
            assertEqualConcepts(localConcept, remoteConcept, Attribute::owners);
        }
    }


    @Test
    public void testExecutingComputeQueryies_ResultsAreCorrect() {
        ConceptId idCoco, idMike, idCocoAndMike;
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            Role pet = tx.putRole("pet");
            Role owner = tx.putRole("owner");
            EntityType animal = tx.putEntityType("animal").plays(pet);
            EntityType human = tx.putEntityType("human").plays(owner);
            RelationType petOwnership = tx.putRelationType("pet-ownership").relates(pet).relates(owner);
            AttributeType<Long> age = tx.putAttributeType("age", DataType.LONG);
            human.has(age);

            Entity coco = animal.create();
            Entity mike = human.create();
            Relation cocoAndMike = petOwnership.create().assign(pet, coco).assign(owner, mike);
            mike.has(age.create(10L));

            idCoco = coco.id();
            idMike = mike.id();
            idCocoAndMike = cocoAndMike.id();

            tx.commit();
        }

        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.READ)) {
            // count
            assertEquals(1, tx.execute(Graql.compute().count().in("animal")).get(0).number().intValue());

            // statistics
            assertEquals(10, tx.execute(Graql.compute().min().of("age").in("human")).get(0).number().intValue());
            assertEquals(10, tx.execute(Graql.compute().max().of("age").in("human")).get(0).number().intValue());
            assertEquals(10, tx.execute(Graql.compute().mean().of("age").in("human")).get(0).number().intValue());


            List<Numeric> answer = tx.execute(Graql.compute().std().of("age").in("human"));
            assertEquals(0, answer.get(0).number().intValue());


            assertEquals(10, tx.execute(Graql.compute().sum().of("age").in("human")).get(0).number().intValue());
            assertEquals(10, tx.execute(Graql.compute().median().of("age").in("human")).get(0).number().intValue());

            // degree
            List<ConceptSetMeasure> centrality = tx.execute(Graql.compute().centrality().using(DEGREE)
                                                                    .of("animal").in("human", "animal", "pet-ownership"));
            assertEquals(1, centrality.size());
            assertEquals(idCoco, centrality.get(0).set().iterator().next());
            assertEquals(1, centrality.get(0).measurement().intValue());

            // coreness
            assertTrue(tx.execute(Graql.compute().centrality().using(K_CORE).of("animal")).isEmpty());

            // path
            List<ConceptList> paths = tx.execute(Graql.compute().path().to(idCoco.getValue()).from(idMike.getValue()));
            assertEquals(1, paths.size());
            assertEquals(idCoco, paths.get(0).list().get(2));
            assertEquals(idMike, paths.get(0).list().get(0));

            // connected component
            List<ConceptSet> clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT)
                                                              .in("human", "animal", "pet-ownership"));
            assertEquals(1, clusterList.size());
            assertEquals(3, clusterList.get(0).set().size());
            assertEquals(Sets.newHashSet(idCoco, idMike, idCocoAndMike), clusterList.get(0).set());

            // k-core
            assertTrue(tx.execute(Graql.compute().cluster().using(K_CORE).in("human", "animal", "pet-ownership")).isEmpty());
        }
    }

    @Test
    public void testExecutingAggregateQueries_theResultsAreCorrect() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType name = tx.putAttributeType("name", DataType.STRING);
            AttributeType age = tx.putAttributeType("age", DataType.INTEGER);
            AttributeType rating = tx.putAttributeType("rating", DataType.DOUBLE);

            person.has(name).has(age).has(rating);

            person.create().has(name.create("Alice")).has(age.create(20));
            person.create().has(name.create("Bob")).has(age.create(22));

            GraqlGet.Aggregate nullQuery =
                    Graql.match(var("x").isa("person").has("rating", var("y"))).get().sum("y");
            assertTrue(tx.execute(nullQuery).isEmpty());

            GraqlGet.Aggregate countQuery =
                    Graql.match(var("x").isa("person").has("age", var("y"))).get("y").count();
            assertEquals(2L, tx.execute(countQuery).get(0).number().longValue());

            GraqlGet.Aggregate sumAgeQuery =
                    Graql.match(var("x").isa("person").has("age", var("y"))).get().sum("y");
            assertEquals(42, tx.execute(sumAgeQuery).get(0).number().intValue());

            GraqlGet.Aggregate minAgeQuery =
                    Graql.match(var("x").isa("person").has("age", var("y"))).get().min("y");
            assertEquals(20, tx.execute(minAgeQuery).get(0).number().intValue());

            GraqlGet.Aggregate maxAgeQuery =
                    Graql.match(var("x").isa("person").has("age", var("y"))).get().max("y");
            assertEquals(22, tx.execute(maxAgeQuery).get(0).number().intValue());

            GraqlGet.Aggregate meanAgeQuery =
                    Graql.match(var("x").isa("person").has("age", var("y"))).get().mean("y");
            assertEquals(21.0d, tx.execute(meanAgeQuery).get(0).number().doubleValue(), 0.01d);

            GraqlGet.Aggregate medianAgeQuery =
                    Graql.match(var("x").isa("person").has("age", var("y"))).get().median("y");
            assertEquals(21.0d, tx.execute(medianAgeQuery).get(0).number().doubleValue(), 0.01d);

            GraqlGet.Aggregate stdAgeQuery =
                    Graql.match(var("x").isa("person").has("age", var("y"))).get().std("y");
            int n = 2;
            double mean = (20 + 22) / n;
            double var = (Math.pow(20 - mean, 2) + Math.pow(22 - mean, 2)) / (n - 1);
            double std = Math.sqrt(var);
            assertEquals(std, tx.execute(stdAgeQuery).get(0).number().doubleValue(), 0.0001d);

            List<AnswerGroup<ConceptMap>> groups = tx.execute(
                    Graql.match(var("x").isa("person").has("name", var("y"))).get().group("y")
            );

            assertEquals(2, groups.size());
            groups.forEach(group -> {
                group.answers().forEach(answer -> {
                    assertTrue(answer.get("x").asEntity().attributes(name).collect(toSet()).contains(group.owner()));
                });
            });

            List<AnswerGroup<Numeric>> counts = tx.execute(
                    Graql.match(var("x").isa("person").has("name", var("y"))).get()
                            .group("y").count()
            );

            assertEquals(2, counts.size());
            counts.forEach(group -> {
                group.answers().forEach(answer -> {
                    assertEquals(1, answer.number().intValue());
                });
            });
        }
    }

    @Test
    public void testDeletingAConcept_TheConceptIsDeleted() {
        Label label = Label.of("hello");

        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType(label);
            tx.commit();
        }

        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            SchemaConcept schemaConcept = tx.getSchemaConcept(label);
            assertFalse(schemaConcept.isDeleted());
            schemaConcept.delete();
            assertTrue(schemaConcept.isDeleted());
            tx.commit();
        }

        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            assertNull(tx.getSchemaConcept(label));
        }
    }

    @Test
    public void testDefiningASchema_TheSchemaIsDefined() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            EntityType animal = tx.putEntityType("animal");
            EntityType dog = tx.putEntityType("dog").sup(animal);
            EntityType cat = tx.putEntityType("cat");
            cat.sup(animal);

            cat.label(Label.of("feline"));
            dog.isAbstract(true).isAbstract(false);
            cat.isAbstract(true);

            RelationType chases = tx.putRelationType("chases");
            Role chased = tx.putRole("chased");
            Role chaser = tx.putRole("chaser");
            chases.relates(chased).relates(chaser);

            Role pointlessRole = tx.putRole("pointless-role");
            tx.putRelationType("pointless").relates(pointlessRole);

            chases.relates(pointlessRole).unrelate(pointlessRole);

            dog.plays(chaser);
            cat.plays(chased);

            AttributeType<String> name = tx.putAttributeType("name", DataType.STRING);
            AttributeType<String> id = tx.putAttributeType("id", DataType.STRING).regex("(good|bad)-dog");
            AttributeType<Long> age = tx.putAttributeType("age", DataType.LONG);

            animal.has(name);
            animal.key(id);

            dog.has(age).unhas(age);
            cat.key(age).unkey(age);
            cat.plays(chaser).unplay(chaser);

            Entity dunstan = dog.create();
            Attribute<String> dunstanId = id.create("good-dog");
            assertNotNull(dunstan.relhas(dunstanId));

            Attribute<String> dunstanName = name.create("Dunstan");
            dunstan.has(dunstanName).unhas(dunstanName);

            chases.create().assign(chaser, dunstan);

            Set<Attribute> set = dunstan.keys(name).collect(toSet());
            assertEquals(0, set.size());

            tx.commit();
        }

        try (Transaction tx = localSession.transaction(Transaction.Type.READ)) {
            EntityType animal = tx.getEntityType("animal");
            EntityType dog = tx.getEntityType("dog");
            EntityType cat = tx.getEntityType("feline");
            RelationType chases = tx.getRelationType("chases");
            Role chased = tx.getRole("chased");
            Role chaser = tx.getRole("chaser");
            AttributeType<String> name = tx.getAttributeType("name");
            AttributeType<String> id = tx.getAttributeType("id");
            Entity dunstan = Iterators.getOnlyElement(dog.instances().iterator());
            Relation aChase = Iterators.getOnlyElement(chases.instances().iterator());

            assertEquals(animal, dog.sup());
            assertEquals(animal, cat.sup());

            assertEquals(ImmutableSet.of(chased, chaser), chases.roles().collect(toSet()));
            assertEquals(ImmutableSet.of(chaser), dog.playing().filter(role -> !role.isImplicit()).collect(toSet()));
            assertEquals(ImmutableSet.of(chased), cat.playing().filter(role -> !role.isImplicit()).collect(toSet()));

            assertEquals(ImmutableSet.of(name, id), animal.attributes().collect(toSet()));
            assertEquals(ImmutableSet.of(id), animal.keys().collect(toSet()));

            assertEquals(ImmutableSet.of(name, id), dog.attributes().collect(toSet()));
            assertEquals(ImmutableSet.of(id), dog.keys().collect(toSet()));

            assertEquals(ImmutableSet.of(name, id), cat.attributes().collect(toSet()));
            assertEquals(ImmutableSet.of(id), cat.keys().collect(toSet()));

            assertEquals("good-dog", Iterables.getOnlyElement(dunstan.keys(id).collect(toSet())).value());

            ImmutableMap<Role, ImmutableSet<?>> expectedRolePlayers =
                    ImmutableMap.of(chaser, ImmutableSet.of(dunstan), chased, ImmutableSet.of());

            assertEquals(expectedRolePlayers, aChase.rolePlayersMap());

            assertEquals("(good|bad)-dog", id.regex());

            assertFalse(dog.isAbstract());
            assertTrue(cat.isAbstract());
        }
    }


    @Ignore("(waiting for keyspaces.retrieve())")
    @Test
    public void testDeletingAKeyspace_TheKeyspaceIsDeleted() {
        GraknClient client = graknClient;
        Session localSession = server.sessionWithNewKeyspace();
        String keyspace = localSession.keyspace().name();
        GraknClient.Session remoteSession = client.session(keyspace);

        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("easter");
            tx.commit();
        }
        localSession.close();

        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            assertNotNull(tx.getEntityType("easter"));
            client.keyspaces().delete(tx.keyspace().name());
        }
        remoteSession.close();

//        assertTrue(client.keyspaces().retrieve().contains(keyspace));
    }

    @Test
    public void testDeletingAKeyspace_TheKeyspaceIsRecreatedInNewSession() {
        GraknClient client = graknClient;
        SessionImpl localSession = server.sessionWithNewKeyspace();
        String keyspace = localSession.keyspace().name();
        GraknClient.Session remoteSession = client.session(keyspace);

        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("easter");
            tx.commit();
        }
        localSession.close();

        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            assertNotNull(tx.getEntityType("easter"));

            client.keyspaces().delete(tx.keyspace().name());
        }

        // Opening a new session will re-create the keyspace
        SessionImpl newLocalSession = server.sessionFactory().session(localSession.keyspace());
        try (Transaction tx = newLocalSession.transaction(Transaction.Type.READ)) {
            assertNull(tx.getEntityType("easter"));
            assertNotNull(tx.getEntityType("entity"));
        }
        newLocalSession.close();
    }


    @Test
    public void whenDeletingKeyspace_OpenTransactionFails() {
        // get open session
        KeyspaceImpl keyspace = localSession.keyspace();

        // Hold on to an open tx
        Transaction tx = localSession.transaction(Transaction.Type.READ);

        // delete keyspace
        graknClient.keyspaces().delete(keyspace.name());

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Graph has been closed");

        // try to operate on an open tx
        tx.getEntityType("entity");

    }

    @Test
    public void whenDeletingKeyspace_OpenSessionFails() {
        // get open session
        KeyspaceImpl keyspace = localSession.keyspace();

        // Hold on to an open tx
        Transaction tx = localSession.transaction(Transaction.Type.READ);

        // delete keyspace
        graknClient.keyspaces().delete(keyspace.name());

        exception.expect(SessionException.class);
        exception.expectMessage("session for graph");
        exception.expectMessage("is closed");

        // try to open a new tx
        Transaction tx2 = localSession.transaction(Transaction.Type.READ);
    }

    @Test
    public void whenDeletingSameKeyspaceTwice_NoErrorThrown() {
        // open a session
        KeyspaceImpl keyspace = localSession.keyspace();

        // delete keyspace twice
        graknClient.keyspaces().delete(keyspace.name());
        graknClient.keyspaces().delete(keyspace.name());

    }


    private <T extends Concept> void assertEqualConcepts(
            T concept1, T concept2, Function<T, Stream<? extends Concept>> function
    ) {
        assertEquals(
                function.apply(concept1).map(Concept::id).collect(toSet()),
                function.apply(concept2).map(Concept::id).collect(toSet())
        );
    }

    @Test
    public void testExecutingAnInvalidQuery_Throw() throws Throwable {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.READ)) {
            GraqlGet query = Graql.match(var("x").isa("not-a-thing")).get();

            exception.expect(RuntimeException.class);

            tx.execute(query);
        }
    }

    @Test
    public void testPerformingAMatchGetQuery_TheResultsAreCorrect() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            //Graql.match(var("x").isa("company")).get(var("x"), var("y"));

            EntityType company = tx.putEntityType("company-123");
            company.create();
            company.create();

            EntityType person = tx.putEntityType("person-123");
            person.create();
            person.create();
            person.create();

            Statement x = var("x");
            Statement y = var("y");

            Collection<ConceptMap> result = tx.execute(Graql.match(x.isa("company-123"), y.isa("person-123")).get(x.var(), y.var()));
            assertEquals(6, result.size());

            result = tx.execute(Graql.match(x.isa("company-123")).get(x.var()));
            assertEquals(2, result.size());
        }
    }

    @Test
    public void testCreatingBasicMultipleTransaction_ThreadsDoNotConflict() {
        GraknClient.Transaction tx1 = remoteSession.transaction(Transaction.Type.WRITE);
        GraknClient.Transaction tx2 = remoteSession.transaction(Transaction.Type.WRITE);

        EntityType company = tx1.putEntityType("company");
        EntityType person = tx2.putEntityType("person");

        AttributeType<String> name1 = tx1.putAttributeType(Label.of("name"), DataType.STRING);
        AttributeType<String> name2 = tx2.putAttributeType(Label.of("name"), DataType.STRING);

        company.has(name1);
        person.has(name2);

        Entity google = company.create();
        Entity alice = person.create();

        google.has(name1.create("Google"));
        alice.has(name2.create("Alice"));

        assertTrue(company.attributes().anyMatch(a -> a.equals(name1)));
        assertTrue(person.attributes().anyMatch(a -> a.equals(name2)));

        assertTrue(google.attributes(name1).allMatch(n -> n.value().equals("Google")));
        assertTrue(alice.attributes(name2).allMatch(n -> n.value().equals("Alice")));

        tx1.close();

        Entity bob = person.create();
        bob.has(name2.create("Bob"));

        assertTrue(bob.attributes(name2).allMatch(n -> n.value().equals("Bob")));

        tx2.close();
    }

    @Test
    public void setAttributeValueWithDatatypeDate() {
        try (GraknClient.Transaction tx = remoteSession.transaction(Transaction.Type.WRITE)) {
            AttributeType<LocalDateTime> birthDateType = tx.putAttributeType(Label.of("birth-date"), DataType.DATE);
            LocalDateTime date = LocalDateTime.now();
            Attribute<LocalDateTime> dateAttribute = birthDateType.create(date);
            assertEquals(date, dateAttribute.value());
        }
    }
}