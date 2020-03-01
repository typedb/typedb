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

package grakn.core.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.client.GraknClient;
import grakn.client.answer.AnswerGroup;
import grakn.client.answer.ConceptList;
import grakn.client.answer.ConceptMap;
import grakn.client.answer.ConceptSet;
import grakn.client.answer.ConceptSetMeasure;
import grakn.client.answer.Numeric;
import grakn.client.concept.Attribute;
import grakn.client.concept.AttributeType;
import grakn.client.concept.Concept;
import grakn.client.concept.ConceptId;
import grakn.client.concept.Entity;
import grakn.client.concept.EntityType;
import grakn.client.concept.Label;
import grakn.client.concept.Relation;
import grakn.client.concept.RelationType;
import grakn.client.concept.Role;
import grakn.client.concept.SchemaConcept;
import grakn.client.concept.Thing;
import grakn.client.concept.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.SessionException;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.rule.GraknTestServer;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Paths;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
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
    public static final GraknTestServer server = new GraknTestServer(
            Paths.get("server/conf/grakn.properties"),
            Paths.get("test-integration/resources/cassandra-embedded.yaml")
    );

    private static Session localSession;
    private static GraknClient.Session remoteSession;

    @Rule
    public final ExpectedException exception = ExpectedException.none();
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
        if (graknClient.keyspaces().retrieve().contains(localSession.keyspace().name())) {
            graknClient.keyspaces().delete(localSession.keyspace().name());
        }
        graknClient.close();
    }

    @Test
    public void testOpeningASession_ReturnARemoteGraknSession() {
        try (GraknClient.Session session = graknClient.session(localSession.keyspace().name())) {
            assertTrue(GraknClient.Session.class.isAssignableFrom(session.getClass()));
        }
    }

    @Test
    public void testOpeningASessionWithAGivenUriAndKeyspace_TheUriAndKeyspaceAreSet() {
        try (GraknClient.Session session = graknClient.session(localSession.keyspace().name())) {
            assertEquals(localSession.keyspace().name(), session.keyspace().name());
        }
    }

    @Test
    public void testOpeningATransactionFromASession_ReturnATransactionWithParametersSet() {
        try (GraknClient.Session session = graknClient.session(localSession.keyspace().name())) {
            try (GraknClient.Transaction tx = session.transaction().read()) {
                assertEquals(session, tx.session());
                assertEquals(localSession.keyspace().name(), tx.keyspace().name());
                assertEquals(GraknClient.Transaction.Type.READ, tx.type());
            }
        }
    }

    @Test
    public void testPuttingEntityType_EnsureItIsAdded() {
        String label = "Oliver";
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            tx.putEntityType(label);
            tx.commit();
        }

        try (Transaction tx = localSession.writeTransaction()) {
            assertNotNull(tx.getEntityType(label));
        }
    }

    @Test
    public void testGettingEntityType_EnsureItIsReturned() {
        String label = "Oliver";
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType(label);
            tx.commit();
        }

        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            assertNotNull(tx.getEntityType(label));
        }
    }

    @Test
    public void testExecutingAndCommittingAQuery_TheQueryIsCommitted() {
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            tx.execute(Graql.define(type("person").sub("entity")));
            tx.commit();
        }

        try (Transaction tx = localSession.readTransaction()) {
            assertNotNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void testExecutingAQueryAndNotCommitting_TheQueryIsNotCommitted() {
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            tx.execute(Graql.define(type("flibflab").sub("entity")));
        }

        try (Transaction tx = localSession.readTransaction()) {
            assertNull(tx.getEntityType("flibflab"));
        }
    }

    @Test
    public void testExecutingAQuery_ResultsAreReturned() {
        List<ConceptMap> answers;

        try (GraknClient.Transaction tx = remoteSession.transaction().read()) {
            answers = tx.execute(Graql.match(var("x").sub("thing")).get());
        }

        int size;
        try (Transaction tx = localSession.readTransaction()) {
            size = tx.execute(Graql.match(var("x").sub("thing")).get()).size();
        }

        assertThat(answers, hasSize(size));

        try (Transaction tx = localSession.readTransaction()) {
            for (ConceptMap answer : answers) {
                assertThat(answer.map().keySet(), contains(new Variable("x")));
                assertNotNull(tx.getConcept(grakn.core.kb.concept.api.ConceptId.of(answer.get("x").id().getValue())));
            }
        }
    }

    @Test
    public void testExecutingAQuery_ExplanationsAreReturned() {
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
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
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            List<Pattern> patterns = Lists.newArrayList(
                    Graql.var("x").isa("content").has("name", "x"),
                    var("z").isa("content").has("name", "z"),
                    var("infer").rel("contained","x").rel("container","z").isa("contains")
            );
            ConceptMap answer = Iterables.getOnlyElement(tx.execute(Graql.match(patterns).get()));

            final int ruleStatements = tx.getRule("transitive-location").when().statements().size();

            Set<ConceptMap> deductions = deductions(answer);

            assertEquals(patterns.size() + ruleStatements, deductions.size());
            assertEquals(patterns.size(), answer.explanation().getAnswers().size());
            answer.explanation().getAnswers().stream()
                    .filter(a -> a.map().containsKey(var("infer").var()))
                    .forEach(a -> assertEquals(ruleStatements, a.explanation().getAnswers().size()));
            testExplanation(answer);
        }
    }

    @Test
    public void testExecutingAlphaEquivalentQueries_CorrectPatternsAreReturned() {
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
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
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            List<Pattern> patterns = Lists.newArrayList(
                    Graql.var("x").isa("content").has("name", "x"),
                    var("z").isa("content").has("name", "z"),
                    var("infer").rel("contained","x").rel("container","z").isa("contains")
            );
            ConceptMap answer = Iterables.getOnlyElement(tx.execute(Graql.match(patterns).get()));


            List<Pattern> patterns2 = Lists.newArrayList(
                    Graql.var("x2").isa("content").has("name", "x"),
                    var("z2").isa("content").has("name", "z"),
                    var().rel("contained","x2").rel("container","z2").isa("contains")
            );
            ConceptMap answer2 = Iterables.getOnlyElement(tx.execute(Graql.match(patterns2).get()));
            testExplanation(answer2);
        }
    }

    private Set<ConceptMap> deductions(ConceptMap answer) {
        if (answer.hasExplanation()) {
            List<ConceptMap> answers = answer.explanation().getAnswers();
            Set<ConceptMap> deductions = new HashSet<>(answers);
            for (ConceptMap explanationAnswer : answers) {
                deductions.addAll(deductions(explanationAnswer));
            }
            return deductions;
        } else {
            return new HashSet<>();
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
                            .anyMatch(a2 -> !Sets.intersection(a.map().keySet(), a2.map().keySet()).isEmpty())
            );
        });
    }

    private void answerHasConsistentExplanations(ConceptMap answer) {
        Set<ConceptMap> answers = deductions(answer).stream()
                .filter(a -> a.queryPattern() != null)
                .collect(Collectors.toSet());

        answers.forEach(a -> TestCase.assertTrue("Answer has inconsistent explanations", explanationConsistentWithAnswer(a)));
    }

    private boolean explanationConsistentWithAnswer(ConceptMap ans) {
        Pattern queryPattern = ans.queryPattern();
        Set<Variable> vars = new HashSet<>();
        if (queryPattern != null) {
            queryPattern.statements().forEach(s -> vars.addAll(s.variables()));
        }
        return vars.containsAll(ans.map().keySet());
    }

    @Test
    public void testExecutingTwoSequentialQueries_ResultsAreTheSame() {
        Set<ConceptMap> answers1;
        Set<ConceptMap> answers2;

        try (GraknClient.Transaction tx = remoteSession.transaction().read()) {
            answers1 = tx.stream(Graql.match(var("x").sub("thing")).get()).collect(toSet());
            answers2 = tx.stream(Graql.match(var("x").sub("thing")).get()).collect(toSet());
        }

        assertEquals(answers1, answers2);
    }

    @Test
    public void testExecutingTwoParallelQueries_GetBothResults() {
        try (GraknClient.Transaction tx = remoteSession.transaction().read()) {
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
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").isa("thing")).get();

            remoteTx.stream(query).forEach(answer -> {
                Concept remoteConcept = answer.get("x");
                grakn.core.kb.concept.api.Concept localConcept = localTx.getConcept(grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue()));

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
        try (Transaction tx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.EntityType person = tx.putEntityType("person");
            grakn.core.kb.concept.api.AttributeType name = tx.putAttributeType("name", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            grakn.core.kb.concept.api.AttributeType email = tx.putAttributeType("email", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            grakn.core.kb.concept.api.Role actor = tx.putRole("actor");
            grakn.core.kb.concept.api.Role characterBeingPlayed = tx.putRole("character-being-played");
            grakn.core.kb.concept.api.RelationType hasCast = tx.putRelationType("has-cast").relates(actor).relates(characterBeingPlayed);
            person.key(email).has(name);
            person.plays(actor).plays(characterBeingPlayed);

            grakn.core.kb.concept.api.Entity marco = person.create().has(name.create("marco")).has(email.create("marco@yolo.com"));
            grakn.core.kb.concept.api.Entity luca = person.create().has(name.create("luca")).has(email.create("luca@yolo.com"));
            hasCast.create().assign(actor, marco).assign(characterBeingPlayed, luca);
            tx.commit();
        }
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
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
        try (Transaction tx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.EntityType person = tx.putEntityType("person");
            grakn.core.kb.concept.api.AttributeType name = tx.putAttributeType("name", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            grakn.core.kb.concept.api.AttributeType email = tx.putAttributeType("email", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            grakn.core.kb.concept.api.Role actor = tx.putRole("actor");
            grakn.core.kb.concept.api.Role characterBeingPlayed = tx.putRole("character-being-played");
            grakn.core.kb.concept.api.RelationType hasCast = tx.putRelationType("has-cast").relates(actor).relates(characterBeingPlayed);
            person.key(email).has(name);
            person.plays(actor).plays(characterBeingPlayed);

            grakn.core.kb.concept.api.Entity marco = person.create().has(name.create("marco")).has(email.create("marco@yolo.com"));
            grakn.core.kb.concept.api.Entity luca = person.create().has(name.create("luca")).has(email.create("luca@yolo.com"));
            hasCast.create().assign(actor, marco).assign(characterBeingPlayed, luca);
            tx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").isa("has-cast")).get();
            Relation remoteConcept = remoteTx.stream(query).findAny().get().get("x").asRelation();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.Relation localConcept = localTx.getConcept(localId).asRelation();

            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Relation::rolePlayers,
                    grakn.client.concept.Relation::rolePlayers);

                    ImmutableMultimap.Builder<String, String> localRolePlayers = ImmutableMultimap.builder();
            localConcept.rolePlayersMap().forEach((role, players) -> {
                for (grakn.core.kb.concept.api.Thing player : players) {
                    localRolePlayers.put(role.id().toString(), player.id().toString());
                }
            });

            ImmutableMultimap.Builder<String, String> remoteRolePlayers = ImmutableMultimap.builder();
            remoteConcept.rolePlayersMap().forEach((role, players) -> {
                for (Thing player : players) {
                    remoteRolePlayers.put(role.id().toString(), player.id().toString());
                }
            });

            assertEquals(localRolePlayers.build(), remoteRolePlayers.build());
        }
    }


    @Test
    public void testGettingASchemaConcept_TheInformationOnTheSchemaConceptIsCorrect() {
        try (Transaction tx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.EntityType human = tx.putEntityType("human");
            grakn.core.kb.concept.api.EntityType man = tx.putEntityType("man").sup(human);
            tx.putEntityType("child").sup(man);
            tx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").type("man")).get();
            SchemaConcept remoteConcept = remoteTx.stream(query).findAny().get().get("x").asSchemaConcept();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.SchemaConcept localConcept = localTx.getConcept(localId).asSchemaConcept();

            assertEquals(localConcept.isImplicit(), remoteConcept.isImplicit());
            assertEquals(localConcept.label().toString(), remoteConcept.label().toString());
            assertEquals(localConcept.sup().id().toString(), remoteConcept.sup().id().toString());
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.SchemaConcept::sups,
                    grakn.client.concept.SchemaConcept::sups);
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.SchemaConcept::subs,
                    grakn.client.concept.SchemaConcept::subs);
        }
    }

    @Test
    public void testGettingAThing_TheInformationOnTheThingIsCorrect() {
        try (Transaction tx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.EntityType person = tx.putEntityType("person");
            grakn.core.kb.concept.api.AttributeType name = tx.putAttributeType("name", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            grakn.core.kb.concept.api.AttributeType email = tx.putAttributeType("email", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            grakn.core.kb.concept.api.Role actor = tx.putRole("actor");
            grakn.core.kb.concept.api.Role characterBeingPlayed = tx.putRole("character-being-played");
            grakn.core.kb.concept.api.RelationType hasCast = tx.putRelationType("has-cast").relates(actor).relates(characterBeingPlayed);
            person.key(email).has(name);
            person.plays(actor).plays(characterBeingPlayed);

            grakn.core.kb.concept.api.Entity marco = person.create().has(name.create("marco")).has(email.create("marco@yolo.com"));
            grakn.core.kb.concept.api.Entity luca = person.create().has(name.create("luca")).has(email.create("luca@yolo.com"));
            hasCast.create().assign(actor, marco).assign(characterBeingPlayed, luca);
            tx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").isa("person")).get();
            Thing remoteConcept = remoteTx.stream(query).findAny().get().get("x").asThing();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.Thing localConcept = localTx.getConcept(localId).asThing();

            assertEquals(localConcept.isInferred(), remoteConcept.isInferred());
            assertEquals(localConcept.type().id().toString(), remoteConcept.type().id().toString());
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Thing::attributes,
                    grakn.client.concept.Thing::attributes);
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Thing::keys,
                    grakn.client.concept.Thing::keys);
//            assertEqualConcepts(localConcept, remoteConcept,  //TODO: re-enable when #19630 is fixed
//                  grakn.core.kb.concept.api.Thing::plays);
//                  grakn.client.concept.Thing::plays);
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Thing::relations,
                    grakn.client.concept.Thing::relations);
        }
    }

    @Test
    public void testGettingAType_TheInformationOnTheTypeIsCorrect() {
        try (Transaction tx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.Role productionWithCast = tx.putRole("production-with-cast");
            grakn.core.kb.concept.api.Role actor = tx.putRole("actor");
            grakn.core.kb.concept.api.Role characterBeingPlayed = tx.putRole("character-being-played");
            tx.putRelationType("has-cast").relates(productionWithCast).relates(actor).relates(characterBeingPlayed);
            grakn.core.kb.concept.api.EntityType person = tx.putEntityType("person").plays(actor).plays(characterBeingPlayed);

            person.has(tx.putAttributeType("gender", grakn.core.kb.concept.api.AttributeType.DataType.STRING));
            person.has(tx.putAttributeType("name", grakn.core.kb.concept.api.AttributeType.DataType.STRING));

            person.create();
            person.create();
            tx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").type("person")).get();
            Type remoteConcept = remoteTx.stream(query).findAny().get().get("x").asType();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.Type localConcept = localTx.getConcept(localId).asType();

            assertEquals(localConcept.isAbstract(), remoteConcept.isAbstract());
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Type::playing,
                    grakn.client.concept.Type::playing);
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Type::instances,
                    grakn.client.concept.Type::instances);
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Type::attributes,
                    grakn.client.concept.Type::attributes);
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Type::keys,
                    grakn.client.concept.Type::keys);
        }
    }

    @Test
    public void testGettingARole_TheInformationOnTheRoleIsCorrect() {
        try (Transaction localTx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.Role productionWithCast = localTx.putRole("production-with-cast");
            grakn.core.kb.concept.api.Role actor = localTx.putRole("actor");
            grakn.core.kb.concept.api.Role characterBeingPlayed = localTx.putRole("character-being-played");
            localTx.putRelationType("has-cast")
                    .relates(productionWithCast).relates(actor).relates(characterBeingPlayed);
            localTx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").type("actor")).get();
            Role remoteConcept = remoteTx.stream(query).findAny().get().get("x").asRole();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.Role localConcept = localTx.getConcept(localId).asRole();

            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Role::players,
                    grakn.client.concept.Role::players);
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Role::relations,
                    grakn.client.concept.Role::relations);
        }
    }

    @Test
    public void testGettingARule_TheInformationOnTheRuleIsCorrect() {
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putAttributeType("name", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            Pattern when = Graql.parsePattern("$x has name 'expectation-when';");
            Pattern then = Graql.parsePattern("$x has name 'expectation-then';");

            tx.putRule("expectation-rule", when, then);

            when = Graql.parsePattern("$x has name 'materialize-when';");
            then = Graql.parsePattern("$x has name 'materialize-then';");
            tx.putRule("materialize-rule", when, then);
            tx.commit();
        }

        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").type("expectation-rule")).get();
            grakn.client.concept.Rule remoteConcept = remoteTx.stream(query).findAny().get().get("x").asRule();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.Rule localConcept = localTx.getConcept(localId).asRule();

            assertEquals(localConcept.when(), remoteConcept.when());
            assertEquals(localConcept.then(), remoteConcept.then());
        }
    }

    @Test
    public void testGettingAnEntityType_TheInformationOnTheEntityTypeIsCorrect() {
        try (Transaction localTx = localSession.writeTransaction()) {
            localTx.putEntityType("person");
            localTx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").type("person")).get();
            EntityType remoteConcept = remoteTx.stream(query).findAny().get().get("x").asEntityType();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.EntityType localConcept = localTx.getConcept(localId).asEntityType();

            // There actually aren't any new methods on EntityType, but we should still check we can get them
            assertEquals(localConcept.id().toString(), remoteConcept.id().toString());
        }
    }

    @Test
    public void testGettingARelationType_TheInformationOnTheRelationTypeIsCorrect() {
        try (Transaction localTx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.Role productionWithCast = localTx.putRole("production-with-cast");
            grakn.core.kb.concept.api.Role actor = localTx.putRole("actor");
            grakn.core.kb.concept.api.Role characterBeingPlayed = localTx.putRole("character-being-played");
            localTx.putRelationType("has-cast")
                    .relates(productionWithCast).relates(actor).relates(characterBeingPlayed);
            localTx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").type("has-cast")).get();
            RelationType remoteConcept = remoteTx.stream(query).findAny().get().get("x").asRelationType();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.RelationType localConcept = localTx.getConcept(localId).asRelationType();

            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.RelationType::roles,
                    grakn.client.concept.RelationType::roles);
        }
    }


    @Test
    public void testGettingAnAttributeType_TheInformationOnTheAttributeTypeIsCorrect() {
        try (Transaction localTx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.AttributeType title = localTx.putAttributeType("title", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            title.create("The Muppets");
            localTx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").type("title")).get();
            AttributeType<String> remoteConcept = remoteTx.stream(query).findAny().get().get("x").asAttributeType();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.AttributeType<String> localConcept = localTx.getConcept(localId).asAttributeType();

            assertEquals(localConcept.dataType().dataClass(), remoteConcept.dataType().dataClass());
            assertEquals(localConcept.regex(), remoteConcept.regex());
            assertEquals(
                    localConcept.attribute("The Muppets").id().toString(),
                    remoteConcept.attribute("The Muppets").id().toString()
            );
        }
    }

    @Test
    public void testGettingAnEntity_TheInformationOnTheEntityIsCorrect() {
        try (Transaction localTx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.EntityType movie = localTx.putEntityType("movie");
            movie.create();
            localTx.commit();
        }
        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").isa("movie")).get();
            Entity remoteConcept = remoteTx.stream(query).findAny().get().get("x").asEntity();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.Entity localConcept = localTx.getConcept(localId).asEntity();

            // There actually aren't any new methods on Entity, but we should still check we can get them
            assertEquals(localConcept.id().toString(), remoteConcept.id().toString());
        }
    }

    @Test
    public void testGettingAnAttribute_TheInformationOnTheAttributeIsCorrect() {
        try (Transaction localTx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.EntityType person = localTx.putEntityType("person");
            grakn.core.kb.concept.api.AttributeType name = localTx.putAttributeType("name", grakn.core.kb.concept.api.AttributeType.DataType.STRING);
            person.has(name);
            grakn.core.kb.concept.api.Attribute alice = name.create("Alice");
            person.create().has(alice);
            localTx.commit();
        }

        try (GraknClient.Transaction remoteTx = remoteSession.transaction().read();
             Transaction localTx = localSession.readTransaction()
        ) {
            GraqlGet query = Graql.match(var("x").isa("name")).get();
            Attribute<?> remoteConcept = remoteTx.stream(query).findAny().get().get("x").asAttribute();
            grakn.core.kb.concept.api.ConceptId localId = grakn.core.kb.concept.api.ConceptId.of(remoteConcept.id().getValue());
            grakn.core.kb.concept.api.Attribute<?> localConcept = localTx.getConcept(localId).asAttribute();

            assertEquals(localConcept.dataType().dataClass(), remoteConcept.dataType().dataClass());
            assertEquals(localConcept.value(), remoteConcept.value());
            assertEquals(localConcept.owner().id().toString(), remoteConcept.owners().findFirst().get().id().toString());
            assertEqualConcepts(localConcept, remoteConcept,
                    grakn.core.kb.concept.api.Attribute::owners,
                    grakn.client.concept.Attribute::owners);
        }
    }


    @Test
    public void testExecutingComputeQueries_ResultsAreCorrect() {
        grakn.core.kb.concept.api.ConceptId idCoco, idMike, idCocoAndMike;
        try (Transaction tx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.Role pet = tx.putRole("pet");
            grakn.core.kb.concept.api.Role owner = tx.putRole("owner");
            grakn.core.kb.concept.api.EntityType animal = tx.putEntityType("animal").plays(pet);
            grakn.core.kb.concept.api.EntityType human = tx.putEntityType("human").plays(owner);
            grakn.core.kb.concept.api.RelationType petOwnership = tx.putRelationType("pet-ownership").relates(pet).relates(owner);
            grakn.core.kb.concept.api.AttributeType<Long> age = tx.putAttributeType("age", grakn.core.kb.concept.api.AttributeType.DataType.LONG);
            human.has(age);

            grakn.core.kb.concept.api.Entity coco = animal.create();
            grakn.core.kb.concept.api.Entity mike = human.create();
            grakn.core.kb.concept.api.Relation cocoAndMike = petOwnership.create().assign(pet, coco).assign(owner, mike);
            mike.has(age.create(10L));

            idCoco = coco.id();
            idMike = mike.id();
            idCocoAndMike = cocoAndMike.id();

            tx.commit();
        }

        try (GraknClient.Transaction tx = remoteSession.transaction().read()) {
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
            assertEquals(idCoco.toString(), centrality.get(0).set().iterator().next().toString());
            assertEquals(1, centrality.get(0).measurement().intValue());

            // coreness
            assertTrue(tx.execute(Graql.compute().centrality().using(K_CORE).of("animal")).isEmpty());

            // path
            List<ConceptList> paths = tx.execute(Graql.compute().path().to(idCoco.getValue()).from(idMike.getValue()));
            assertEquals(1, paths.size());
            assertEquals(idCoco.toString(), paths.get(0).list().get(2).toString());
            assertEquals(idMike.toString(), paths.get(0).list().get(0).toString());

            // connected component
            List<ConceptSet> clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT)
                    .in("human", "animal", "pet-ownership"));
            assertEquals(1, clusterList.size());
            assertEquals(3, clusterList.get(0).set().size());
            assertThat(Sets.newHashSet(idCoco.toString(), idMike.toString(), idCocoAndMike.toString()),
                    containsInAnyOrder(clusterList.get(0).set().stream().map(ConceptId::toString).toArray()));

            // k-core
            assertTrue(tx.execute(Graql.compute().cluster().using(K_CORE).in("human", "animal", "pet-ownership")).isEmpty());
        }
    }

    @Test
    public void testExecutingAggregateQueries_theResultsAreCorrect() {
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            EntityType person = tx.putEntityType("person");
            AttributeType name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            AttributeType age = tx.putAttributeType("age", AttributeType.DataType.INTEGER);
            AttributeType rating = tx.putAttributeType("rating", AttributeType.DataType.DOUBLE);

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
            double mean = (double) (20 + 22) / n;
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

        try (Transaction tx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.Label localLabel = grakn.core.kb.concept.api.Label.of(label.getValue());
            tx.putEntityType(localLabel);
            tx.commit();
        }

        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            SchemaConcept schemaConcept = tx.getSchemaConcept(label);
            assertFalse(schemaConcept.isDeleted());
            schemaConcept.delete();
            assertTrue(schemaConcept.isDeleted());
            tx.commit();
        }

        try (Transaction tx = localSession.writeTransaction()) {
            grakn.core.kb.concept.api.Label localLabel = grakn.core.kb.concept.api.Label.of(label.getValue());
            assertNull(tx.getSchemaConcept(localLabel));
        }
    }

    @Test
    public void testDefiningASchema_TheSchemaIsDefined() {
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
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

            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            AttributeType<String> id = tx.putAttributeType("id", AttributeType.DataType.STRING).regex("(good|bad)-dog");
            AttributeType<Long> age = tx.putAttributeType("age", AttributeType.DataType.LONG);

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

        try (Transaction tx = localSession.readTransaction()) {
            grakn.core.kb.concept.api.EntityType animal = tx.getEntityType("animal");
            grakn.core.kb.concept.api.EntityType dog = tx.getEntityType("dog");
            grakn.core.kb.concept.api.EntityType cat = tx.getEntityType("feline");
            grakn.core.kb.concept.api.RelationType chases = tx.getRelationType("chases");
            grakn.core.kb.concept.api.Role chased = tx.getRole("chased");
            grakn.core.kb.concept.api.Role chaser = tx.getRole("chaser");
            grakn.core.kb.concept.api.AttributeType<String> name = tx.getAttributeType("name");
            grakn.core.kb.concept.api.AttributeType<String> id = tx.getAttributeType("id");
            grakn.core.kb.concept.api.Entity dunstan = Iterators.getOnlyElement(dog.instances().iterator());
            grakn.core.kb.concept.api.Relation aChase = Iterators.getOnlyElement(chases.instances().iterator());

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

            ImmutableMap<grakn.core.kb.concept.api.Role, ImmutableSet<?>> expectedRolePlayers =
                    ImmutableMap.of(chaser, ImmutableSet.of(dunstan), chased, ImmutableSet.of());

            assertEquals(expectedRolePlayers, aChase.rolePlayersMap());

            assertEquals("(good|bad)-dog", id.regex());

            assertFalse(dog.isAbstract());
            assertTrue(cat.isAbstract());
        }
    }


    @Test
    public void testDeletingAKeyspace_TheKeyspaceIsRecreatedInNewSession() {
        GraknClient client = graknClient;
        Session localSession = server.sessionWithNewKeyspace();
        String keyspace = localSession.keyspace().name();
        GraknClient.Session remoteSession = client.session(keyspace);

        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("easter");
            tx.commit();
        }
        localSession.close();

        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            assertNotNull(tx.getEntityType("easter"));

            client.keyspaces().delete(tx.keyspace().name());
        }

        // Opening a new session will re-create the keyspace
        Session newLocalSession = server.sessionFactory().session(localSession.keyspace());
        try (Transaction tx = newLocalSession.readTransaction()) {
            assertNull(tx.getEntityType("easter"));
            assertNotNull(tx.getEntityType("entity"));
        }
        newLocalSession.close();
    }


    @Test
    public void whenDeletingKeyspace_OpenTransactionFails() {
        // get open session
        Keyspace keyspace = localSession.keyspace();

        // Hold on to an open tx
        Transaction tx = localSession.readTransaction();

        // delete keyspace
        graknClient.keyspaces().delete(keyspace.name());

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operation cannot be executed because the enclosing transaction is closed");

        // try to operate on an open tx
        tx.getEntityType("entity");
    }

    @Test
    public void whenDeletingKeyspace_OpenSessionFails() {
        // get open session
        Keyspace keyspace = localSession.keyspace();

        // Hold on to an open tx
        Transaction tx = localSession.readTransaction();

        // delete keyspace
        graknClient.keyspaces().delete(keyspace.name());

        exception.expect(SessionException.class);
        exception.expectMessage("session for graph");
        exception.expectMessage("is closed");

        // try to open a new tx
        Transaction tx2 = localSession.readTransaction();
    }

    @Test
    public void whenDeletingNonExistingKeyspace_exceptionThrown() {
        exception.expectMessage("It is not possible to delete keyspace [nonexistingkeyspace] as it does not exist");
        graknClient.keyspaces().delete("nonexistingkeyspace");
    }


    private <T extends grakn.core.kb.concept.api.Concept, S extends Concept> void assertEqualConcepts(
            T conceptLocal, S conceptRemote, Function<T,Stream<?extends grakn.core.kb.concept.api.Concept>> functionLocal,
            Function<S, Stream<? extends Concept>> functionRemote
    ) {
        assertEquals(
                functionLocal.apply(conceptLocal).map(concept -> concept.id().toString()).collect(toSet()),
                functionRemote.apply(conceptRemote).map(concept -> concept.id().toString()).collect(toSet())
        );
    }

    @Test
    public void testExecutingAnInvalidQuery_Throw() throws Throwable {
        try (GraknClient.Transaction tx = remoteSession.transaction().read()) {
            GraqlGet query = Graql.match(var("x").isa("not-a-thing")).get();

            exception.expect(RuntimeException.class);

            tx.execute(query);
        }
    }

    @Test
    public void testPerformingAMatchGetQuery_TheResultsAreCorrect() {
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
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
        GraknClient.Transaction tx1 = remoteSession.transaction().write();
        GraknClient.Transaction tx2 = remoteSession.transaction().write();

        EntityType company = tx1.putEntityType("company");
        EntityType person = tx2.putEntityType("person");

        AttributeType<String> name1 = tx1.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<String> name2 = tx2.putAttributeType("name", AttributeType.DataType.STRING);

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
        try (GraknClient.Transaction tx = remoteSession.transaction().write()) {
            AttributeType<LocalDateTime> birthDateType = tx.putAttributeType("birth-date", AttributeType.DataType.DATE);
            LocalDateTime date = LocalDateTime.now();
            Attribute<LocalDateTime> dateAttribute = birthDateType.create(date);
            assertEquals(date, dateAttribute.value());
        }
    }

    @Test
    public void retrievingExistingKeyspaces_onlyRemoteSessionKeyspaceIsReturned() {
        List<String> keyspaces = graknClient.keyspaces().retrieve();
        assertTrue(keyspaces.contains(remoteSession.keyspace().name()));
    }

    @Test
    public void whenCreatingNewKeyspace_itIsVisibileInListOfExistingKeyspaces() {
        graknClient.session("newkeyspace").transaction().write().close();
        List<String> keyspaces = graknClient.keyspaces().retrieve();

        assertTrue(keyspaces.contains("newkeyspace"));
    }

    @Test
    public void whenDeletingKeyspace_notListedInExistingKeyspaces() {
        graknClient.session("newkeyspace").transaction().write().close();
        List<String> keyspaces = graknClient.keyspaces().retrieve();

        assertTrue(keyspaces.contains("newkeyspace"));

        graknClient.keyspaces().delete("newkeyspace");
        List<String> keyspacesNoNew = graknClient.keyspaces().retrieve();

        assertFalse(keyspacesNoNew.contains("newkeyspace"));
    }
}