/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.engine;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.AttributeType.DataType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.EngineContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.DEGREE;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.K_CORE;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.members;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CENTRALITY;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CLUSTER;
import static ai.grakn.util.GraqlSyntax.Compute.Method.COUNT;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MAX;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MEAN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MEDIAN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MIN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.PATH;
import static ai.grakn.util.GraqlSyntax.Compute.Method.STD;
import static ai.grakn.util.GraqlSyntax.Compute.Method.SUM;
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
 * @author Felix Chapman
 */
public class GrpcServerIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.create();

    private static GraknSession localSession;
    private static GraknSession remoteSession;

    @Before
    public void setUp() {
        localSession = engine.sessionWithNewKeyspace();

        try (GraknTx tx = localSession.open(GraknTxType.WRITE)) {
            MovieKB.get().accept(tx);
            tx.commit();
        }

        remoteSession = RemoteGrakn.session(engine.grpcUri(), localSession.keyspace());
    }

    @After
    public void tearDown() {
        remoteSession.close();
    }

    @Test
    public void whenPuttingEntityType_EnsureItIsAdded() {
        String label = "Oliver";
        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            tx.putEntityType(label);
            tx.commit();
        }

        try (GraknTx tx = localSession.open(GraknTxType.WRITE)) {
            assertNotNull(tx.getEntityType(label));
        }
    }

    @Test
    public void whenGettingEntityType_EnsureItIsReturned() {
        String label = "Oliver";
        try (GraknTx tx = localSession.open(GraknTxType.WRITE)) {
            tx.putEntityType(label);
            tx.commit();
        }

        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            assertNotNull(tx.getEntityType(label));
        }
    }

    @Test
    public void whenExecutingAndCommittingAQuery_TheQueryIsCommitted() throws InterruptedException {
        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            tx.graql().define(label("person").sub("entity")).execute();
            tx.commit();
        }

        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            assertNotNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void whenExecutingAQueryAndNotCommitting_TheQueryIsNotCommitted() throws InterruptedException {
        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            tx.graql().define(label("flibflab").sub("entity")).execute();
        }

        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            assertNull(tx.getEntityType("flibflab"));
        }
    }

    @Test
    public void whenExecutingAQuery_ResultsAreReturned() throws InterruptedException {
        List<Answer> answers;

        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            answers = tx.graql().match(var("x").sub("thing")).get().execute();
        }

        int size;
        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            size = tx.graql().match(var("x").sub("thing")).get().execute().size();
        }

        assertThat(answers.toString(), answers, hasSize(size));
        assertThat(Sets.newHashSet(answers), hasSize(size));

        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            for (Answer answer : answers) {
                assertThat(answer.vars(), contains(var("x")));
                assertNotNull(tx.getConcept(answer.get("x").getId()));
            }
        }
    }

    @Test
    public void whenExecutingTwoSequentialQueries_ResultsAreTheSame() throws InterruptedException {
        Set<Answer> answers1;
        Set<Answer> answers2;

        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            answers1 = tx.graql().match(var("x").sub("thing")).get().stream().collect(toSet());
            answers2 = tx.graql().match(var("x").sub("thing")).get().stream().collect(toSet());
        }

        assertEquals(answers1, answers2);
    }

    @Test
    public void whenExecutingTwoParallelQueries_GetBothResults() throws Throwable {
        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            GetQuery query = tx.graql().match(var("x").sub("thing")).get();

            Iterator<Answer> iterator1 = query.iterator();
            Iterator<Answer> iterator2 = query.iterator();

            while (iterator1.hasNext() || iterator2.hasNext()) {
                assertEquals(iterator1.next(), iterator2.next());
                assertEquals(iterator1.hasNext(), iterator2.hasNext());
            }
        }
    }

    @Test
    public void whenExecutingComputeQueryies_ResultsAreCorrect() {
        ConceptId idCoco, idMike, idCocoAndMike;
        try (GraknTx tx = localSession.open(GraknTxType.WRITE)) {
            Role pet = tx.putRole("pet");
            Role owner = tx.putRole("owner");
            EntityType animal = tx.putEntityType("animal").plays(pet);
            EntityType human = tx.putEntityType("human").plays(owner);
            RelationshipType petOwnership = tx.putRelationshipType("pet-ownership").relates(pet).relates(owner);
            AttributeType<Long> age = tx.putAttributeType("age", DataType.LONG);
            human.attribute(age);

            Entity coco = animal.addEntity();
            Entity mike = human.addEntity();
            Relationship cocoAndMike = petOwnership.addRelationship().addRolePlayer(pet, coco).addRolePlayer(owner, mike);
            mike.attribute(age.putAttribute(10L));

            idCoco = coco.getId();
            idMike = mike.getId();
            idCocoAndMike = cocoAndMike.getId();

            tx.commit();
        }

        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            // count
            assertEquals(1L, tx.graql().compute(COUNT).in("animal").execute().getNumber().get());

            // statistics
            assertEquals(10L, tx.graql().compute(MIN).of("age").in("human").execute().getNumber().get());
            assertEquals(10L, tx.graql().compute(MAX).of("age").in("human").execute().getNumber().get());
            assertEquals(10L, tx.graql().compute(MEAN).of("age").in("human").execute().getNumber().get());


            ComputeQuery.Answer answer = tx.graql().compute(STD).of("age").in("human").execute();
            assertEquals(0L, answer.getNumber().get());


            assertEquals(10L, tx.graql().compute(SUM).of("age").in("human").execute().getNumber().get());
            assertEquals(10L, tx.graql().compute(MEDIAN).of("age").in("human").execute().getNumber().get());

            // degree
            Map<Long, Set<ConceptId>> centrality = tx.graql().compute(CENTRALITY).using(DEGREE)
                    .of("animal").in("human", "animal", "pet-ownership").execute().getCentrality().get();
            assertEquals(1L, centrality.size());
            assertEquals(idCoco, centrality.get(1L).iterator().next());

            // coreness
            assertTrue(tx.graql().compute(CENTRALITY).using(K_CORE).of("animal").execute().getCentrality().get().isEmpty());

            // path
            List<List<ConceptId>> paths = tx.graql().compute(PATH).to(idCoco).from(idMike).execute().getPaths().get();
            assertEquals(1, paths.size());
            assertEquals(idCoco, paths.get(0).get(2));
            assertEquals(idMike, paths.get(0).get(0));

            // connected component size
            List<Long> sizeList = tx.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .in("human", "animal", "pet-ownership").execute().getClusterSizes().get();
            assertEquals(1, sizeList.size());
            assertTrue(sizeList.contains(3L));

            // connected component member
            Set<Set<ConceptId>> membersList = tx.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .in("human", "animal", "pet-ownership").where(members(true)).execute().getClusters().get();
            assertEquals(1, membersList.size());
            Set<ConceptId> memberSet = membersList.iterator().next();
            assertEquals(3, memberSet.size());
            assertEquals(Sets.newHashSet(idCoco, idMike, idCocoAndMike), memberSet);

            // k-core
            assertEquals(
                    0,
                    tx.graql().compute(CLUSTER).using(K_CORE)
                    .in("human", "animal", "pet-ownership").execute().getClusters().get().size());
        }
    }

    @Test
    public void whenGettingAConcept_TheInformationOnTheConceptIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x")).get();

            for (Answer answer : query) {
                Concept remoteConcept = answer.get("x");
                Concept localConcept = localTx.getConcept(remoteConcept.getId());

                assertEquals(localConcept.isAttribute(), remoteConcept.isAttribute());
                assertEquals(localConcept.isAttributeType(), remoteConcept.isAttributeType());
                assertEquals(localConcept.isEntity(), remoteConcept.isEntity());
                assertEquals(localConcept.isEntityType(), remoteConcept.isEntityType());
                assertEquals(localConcept.isRelationship(), remoteConcept.isRelationship());
                assertEquals(localConcept.isRelationshipType(), remoteConcept.isRelationshipType());
                assertEquals(localConcept.isRole(), remoteConcept.isRole());
                assertEquals(localConcept.isRule(), remoteConcept.isRule());
                assertEquals(localConcept.isSchemaConcept(), remoteConcept.isSchemaConcept());
                assertEquals(localConcept.isThing(), remoteConcept.isThing());
                assertEquals(localConcept.isType(), remoteConcept.isType());
                assertEquals(localConcept.getId(), remoteConcept.getId());
                assertEquals(localConcept.isDeleted(), remoteConcept.isDeleted());
                assertEquals(localConcept.keyspace(), remoteConcept.keyspace());
            }
        }
    }

    @Test
    public void whenGettingASchemaConcept_TheInformationOnTheSchemaConceptIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").label("actor")).get();
            SchemaConcept remoteConcept = query.stream().findAny().get().get("x").asSchemaConcept();
            SchemaConcept localConcept = localTx.getConcept(remoteConcept.getId()).asSchemaConcept();

            assertEquals(localConcept.isImplicit(), remoteConcept.isImplicit());
            assertEquals(localConcept.getLabel(), remoteConcept.getLabel());
            assertEquals(localConcept.sup().getId(), remoteConcept.sup().getId());
            assertEqualConcepts(localConcept, remoteConcept, SchemaConcept::sups);
            assertEqualConcepts(localConcept, remoteConcept, SchemaConcept::subs);
        }
    }

    @Test
    public void whenGettingAThing_TheInformationOnTheThingIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").has("name", "crime")).get();
            Thing remoteConcept = query.stream().findAny().get().get("x").asThing();
            Thing localConcept = localTx.getConcept(remoteConcept.getId()).asThing();

            assertEquals(localConcept.isInferred(), remoteConcept.isInferred());
            assertEquals(localConcept.type().getId(), remoteConcept.type().getId());
            assertEqualConcepts(localConcept, remoteConcept, Thing::attributes);
            assertEqualConcepts(localConcept, remoteConcept, Thing::keys);
//            assertEqualConcepts(localConcept, remoteConcept, Thing::plays); // TODO: re-enable when #19630 is fixed
            assertEqualConcepts(localConcept, remoteConcept, Thing::relationships);
        }
    }

    @Test
    public void whenGettingAType_TheInformationOnTheTypeIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").label("person")).get();
            Type remoteConcept = query.stream().findAny().get().get("x").asType();
            Type localConcept = localTx.getConcept(remoteConcept.getId()).asType();

            assertEquals(localConcept.isAbstract(), remoteConcept.isAbstract());
            assertEqualConcepts(localConcept, remoteConcept, Type::plays);
            assertEqualConcepts(localConcept, remoteConcept, Type::instances);
            assertEqualConcepts(localConcept, remoteConcept, Type::attributes);
            assertEqualConcepts(localConcept, remoteConcept, Type::keys);
        }
    }

    @Test
    public void whenGettingARole_TheInformationOnTheRoleIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").label("actor")).get();
            Role remoteConcept = query.stream().findAny().get().get("x").asRole();
            Role localConcept = localTx.getConcept(remoteConcept.getId()).asRole();

            assertEqualConcepts(localConcept, remoteConcept, Role::playedByTypes);
            assertEqualConcepts(localConcept, remoteConcept, Role::relationshipTypes);
        }
    }

    @Test
    public void whenGettingARule_TheInformationOnTheRuleIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").label("expectation-rule")).get();
            ai.grakn.concept.Rule remoteConcept = query.stream().findAny().get().get("x").asRule();
            ai.grakn.concept.Rule localConcept = localTx.getConcept(remoteConcept.getId()).asRule();

            assertEquals(localConcept.getWhen(), remoteConcept.getWhen());
            assertEquals(localConcept.getThen(), remoteConcept.getThen());
        }
    }

    @Test
    public void whenGettingAnEntityType_TheInformationOnTheEntityTypeIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").label("person")).get();
            EntityType remoteConcept = query.stream().findAny().get().get("x").asEntityType();
            EntityType localConcept = localTx.getConcept(remoteConcept.getId()).asEntityType();

            // There actually aren't any new methods on EntityType, but we should still check we can get them
            assertEquals(localConcept.getId(), remoteConcept.getId());
        }
    }

    @Test
    public void whenGettingARelationshipType_TheInformationOnTheRelationshipTypeIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").label("has-cast")).get();
            RelationshipType remoteConcept = query.stream().findAny().get().get("x").asRelationshipType();
            RelationshipType localConcept = localTx.getConcept(remoteConcept.getId()).asRelationshipType();

            assertEqualConcepts(localConcept, remoteConcept, RelationshipType::relates);
        }
    }

    @Test
    public void whenGettingAnAttributeType_TheInformationOnTheAttributeTypeIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").label("title")).get();
            AttributeType<String> remoteConcept = query.stream().findAny().get().get("x").asAttributeType();
            AttributeType<String> localConcept = localTx.getConcept(remoteConcept.getId()).asAttributeType();

            assertEquals(localConcept.getDataType(), remoteConcept.getDataType());
            assertEquals(localConcept.getRegex(), remoteConcept.getRegex());
            assertEquals(
                    localConcept.getAttribute("The Muppets").getId(),
                    remoteConcept.getAttribute("The Muppets").getId()
            );
        }
    }

    @Test
    public void whenGettingAnEntity_TheInformationOnTheEntityIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").isa("movie")).get();
            Entity remoteConcept = query.stream().findAny().get().get("x").asEntity();
            Entity localConcept = localTx.getConcept(remoteConcept.getId()).asEntity();

            // There actually aren't any new methods on Entity, but we should still check we can get them
            assertEquals(localConcept.getId(), remoteConcept.getId());
        }
    }

    @Test
    public void whenGettingARelationship_TheInformationOnTheRelationshipIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").isa("has-cast")).get();
            Relationship remoteConcept = query.stream().findAny().get().get("x").asRelationship();
            Relationship localConcept = localTx.getConcept(remoteConcept.getId()).asRelationship();

            assertEqualConcepts(localConcept, remoteConcept, Relationship::rolePlayers);

            ImmutableMultimap.Builder<ConceptId, ConceptId> localRolePlayers = ImmutableMultimap.builder();
            localConcept.allRolePlayers().forEach((role, players) -> {
                for (Thing player : players) {
                    localRolePlayers.put(role.getId(), player.getId());
                }
            });

            ImmutableMultimap.Builder<ConceptId, ConceptId> remoteRolePlayers = ImmutableMultimap.builder();
            remoteConcept.allRolePlayers().forEach((role, players) -> {
                for (Thing player : players) {
                    remoteRolePlayers.put(role.getId(), player.getId());
                }
            });

            assertEquals(localRolePlayers.build(), remoteRolePlayers.build());
        }
    }

    @Test
    public void whenGettingAnAttribute_TheInformationOnTheAttributeIsCorrect() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").isa("title")).get();
            Attribute<?> remoteConcept = query.stream().findAny().get().get("x").asAttribute();
            Attribute<?> localConcept = localTx.getConcept(remoteConcept.getId()).asAttribute();

            assertEquals(localConcept.dataType(), remoteConcept.dataType());
            assertEquals(localConcept.getValue(), remoteConcept.getValue());
            assertEquals(localConcept.owner().getId(), remoteConcept.owner().getId());
            assertEqualConcepts(localConcept, remoteConcept, Attribute::ownerInstances);
        }
    }

    @Test
    public void whenDeletingAConcept_TheConceptIsDeleted() {
        Label label = Label.of("hello");

        try (GraknTx tx = localSession.open(GraknTxType.WRITE)) {
            tx.putEntityType(label);
            tx.commit();
        }

        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            SchemaConcept schemaConcept = tx.getSchemaConcept(label);
            assertFalse(schemaConcept.isDeleted());
            schemaConcept.delete();
            assertTrue(schemaConcept.isDeleted());
            tx.commit();
        }

        try (GraknTx tx = localSession.open(GraknTxType.WRITE)) {
            assertNull(tx.getSchemaConcept(label));
        }
    }

    @Test
    public void whenDefiningASchema_TheSchemaIsDefined() {
        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            EntityType animal = tx.putEntityType("animal");
            EntityType dog = tx.putEntityType("dog").sup(animal);
            EntityType cat = tx.putEntityType("cat");
            animal.sub(cat);

            cat.setLabel(Label.of("feline"));
            dog.setAbstract(true).setAbstract(false);
            cat.setAbstract(true);

            RelationshipType chases = tx.putRelationshipType("chases");
            Role chased = tx.putRole("chased");
            Role chaser = tx.putRole("chaser");
            chases.relates(chased).relates(chaser);

            Role pointlessRole = tx.putRole("pointless-role");
            tx.putRelationshipType("pointless").relates(pointlessRole);

            chases.relates(pointlessRole).deleteRelates(pointlessRole);

            dog.plays(chaser);
            cat.plays(chased);

            AttributeType<String> name = tx.putAttributeType("name", DataType.STRING);
            AttributeType<String> id = tx.putAttributeType("id", DataType.STRING).setRegex("(good|bad)-dog");
            AttributeType<Long> age = tx.putAttributeType("age", DataType.LONG);

            animal.attribute(name);
            animal.key(id);

            dog.attribute(age).deleteAttribute(age);
            cat.key(age).deleteKey(age);
            cat.plays(chaser).deletePlays(chaser);

            Entity dunstan = dog.addEntity();
            Attribute<String> dunstanId = id.putAttribute("good-dog");
            assertNotNull(dunstan.attributeRelationship(dunstanId));

            Attribute<String> dunstanName = name.putAttribute("Dunstan");
            dunstan.attribute(dunstanName).deleteAttribute(dunstanName);

            chases.addRelationship().addRolePlayer(chaser, dunstan);

            tx.commit();
        }

        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            EntityType animal = tx.getEntityType("animal");
            EntityType dog = tx.getEntityType("dog");
            EntityType cat = tx.getEntityType("feline");
            RelationshipType chases = tx.getRelationshipType("chases");
            Role chased = tx.getRole("chased");
            Role chaser = tx.getRole("chaser");
            AttributeType<String> name = tx.getAttributeType("name");
            AttributeType<String> id = tx.getAttributeType("id");
            Entity dunstan = Iterators.getOnlyElement(dog.instances().iterator());
            Relationship aChase = Iterators.getOnlyElement(chases.instances().iterator());

            assertEquals(animal, dog.sup());
            assertEquals(animal, cat.sup());

            assertEquals(ImmutableSet.of(chased, chaser), chases.relates().collect(toSet()));
            assertEquals(ImmutableSet.of(chaser), dog.plays().filter(role -> !role.isImplicit()).collect(toSet()));
            assertEquals(ImmutableSet.of(chased), cat.plays().filter(role -> !role.isImplicit()).collect(toSet()));

            assertEquals(ImmutableSet.of(name, id), animal.attributes().collect(toSet()));
            assertEquals(ImmutableSet.of(id), animal.keys().collect(toSet()));

            assertEquals(ImmutableSet.of(name, id), dog.attributes().collect(toSet()));
            assertEquals(ImmutableSet.of(id), dog.keys().collect(toSet()));

            assertEquals(ImmutableSet.of(name, id), cat.attributes().collect(toSet()));
            assertEquals(ImmutableSet.of(id), cat.keys().collect(toSet()));

            assertEquals("good-dog", Iterables.getOnlyElement(dunstan.keys(id).collect(toSet())).getValue());

            ImmutableMap<Role, ImmutableSet<?>> expectedRolePlayers =
                    ImmutableMap.of(chaser, ImmutableSet.of(dunstan), chased, ImmutableSet.of());

            assertEquals(expectedRolePlayers, aChase.allRolePlayers());

            assertEquals("(good|bad)-dog", id.getRegex());

            assertFalse(dog.isAbstract());
            assertTrue(cat.isAbstract());
        }
    }

    @Test
    public void whenDeletingAKeyspace_TheKeyspaceIsDeleted() {
        try (GraknTx tx = localSession.open(GraknTxType.WRITE)) {
            tx.putEntityType("easter");
            tx.commit();
        }

        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            assertNotNull(tx.getEntityType("easter"));

            tx.admin().delete();

            assertTrue(tx.isClosed());
        }

        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            assertNull(tx.getEntityType("easter"));
        }
    }

    private <T extends Concept> void assertEqualConcepts(
            T concept1, T concept2, Function<T, Stream<? extends Concept>> function
    ) {
        assertEquals(
                function.apply(concept1).map(Concept::getId).collect(toSet()),
                function.apply(concept2).map(Concept::getId).collect(toSet())
        );
    }

    @Test
    public void whenExecutingAnInvalidQuery_Throw() throws Throwable {
        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            GetQuery query = tx.graql().match(var("x").isa("not-a-thing")).get();

            exception.expect(GraqlQueryException.class);
            exception.expectMessage(GraqlQueryException.labelNotFound(Label.of("not-a-thing")).getMessage());

            query.execute();
        }
    }

    @Test
    public void whenPerformingAMatchGetQuery_TheResultsAreCorrect(){
        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            //Graql.match(var("x").isa("company")).get(var("x"), var("y"));

            EntityType company = tx.putEntityType("company-123");
            company.addEntity();
            company.addEntity();

            EntityType person = tx.putEntityType("person-123");
            person.addEntity();
            person.addEntity();
            person.addEntity();

            QueryBuilder qb = tx.graql();
            Var x = var("x");
            Var y = var("y");

            Collection<Answer> result = qb.match(x.isa("company-123"), y.isa("person-123")).get(x, y).execute();
            assertEquals(6, result.size());

            result = qb.match(x.isa("company-123")).get(x).execute();
            assertEquals(2, result.size());
        }
    }
}
