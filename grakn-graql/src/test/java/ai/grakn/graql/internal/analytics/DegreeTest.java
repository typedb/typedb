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

package ai.grakn.graql.internal.analytics;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknTestUtil;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.DEGREE;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CENTRALITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DegreeTest {

    public GraknSession session;
    private GraknTx tx;

    @ClassRule
    public final static SessionContext sessionContext = SessionContext.create();

    @Before
    public void setUp() {
        session = sessionContext.newSession();
        tx = session.open(GraknTxType.WRITE);
    }

    @Test
    public void testDegreesSimple() {
        // create instances
        EntityType thingy = tx.putEntityType("thingy");
        EntityType anotherThing = tx.putEntityType("another");

        ConceptId entity1 = thingy.addEntity().getId();
        ConceptId entity2 = thingy.addEntity().getId();
        ConceptId entity3 = thingy.addEntity().getId();
        ConceptId entity4 = anotherThing.addEntity().getId();

        Role role1 = tx.putRole("role1");
        Role role2 = tx.putRole("role2");
        thingy.plays(role1).plays(role2);
        anotherThing.plays(role1).plays(role2);
        RelationshipType related = tx.putRelationshipType("related").relates(role1).relates(role2);

        // relate them
        related.addRelationship()
                .addRolePlayer(role1, tx.getConcept(entity1))
                .addRolePlayer(role2, tx.getConcept(entity2));
        related.addRelationship()
                .addRolePlayer(role1, tx.getConcept(entity2))
                .addRolePlayer(role2, tx.getConcept(entity3));
        related.addRelationship()
                .addRolePlayer(role1, tx.getConcept(entity2))
                .addRolePlayer(role2, tx.getConcept(entity4));
        tx.commit();

        tx = session.open(GraknTxType.READ);

        Map<ConceptId, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1L);
        correctDegrees.put(entity2, 3L);
        correctDegrees.put(entity3, 1L);
        correctDegrees.put(entity4, 1L);

        // compute degrees
        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        if (GraknTestUtil.usingTinker()) workerNumber = 1L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }
        tx.close();

        Set<Map<Long, Set<ConceptId>>> result = list.parallelStream().map(i -> {
            try (GraknTx graph = session.open(GraknTxType.READ)) {
                return graph.graql().compute(CENTRALITY).using(DEGREE).execute().getCentralityCount().get();
            }
        }).collect(Collectors.toSet());
        assertEquals(1, result.size());
        Map<Long, Set<ConceptId>> degrees0 = result.iterator().next();
        assertEquals(2, degrees0.size());
        degrees0.forEach((key, value) -> value.forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id), key);
                }
        ));

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            Map<Long, Set<ConceptId>> degrees1 =
                    graph.graql().compute(CENTRALITY).using(DEGREE).of("thingy").execute().getCentralityCount().get();

            assertEquals(2, degrees1.size());
            assertEquals(2, degrees1.get(1L).size());
            assertEquals(1, degrees1.get(3L).size());
            degrees1.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(id));
                        assertEquals(correctDegrees.get(id), key);
                    }
            ));

            Map<Long, Set<ConceptId>> degrees2 =
                    graph.graql().compute(CENTRALITY).using(DEGREE).of("thingy", "related").execute().getCentralityCount().get();
            assertEquals(degrees1, degrees2);

            degrees2 = graph.graql().compute(CENTRALITY).using(DEGREE).execute().getCentralityCount().get();
            assertEquals(degrees0, degrees2);

            // compute degrees on subgraph
            Map<Long, Set<ConceptId>> degrees3 = graph.graql().compute(CENTRALITY).using(DEGREE)
                    .in("thingy", "related").execute().getCentralityCount().get();
            assertEquals(degrees1, degrees3);

            degrees3 = graph.graql().compute(CENTRALITY).using(DEGREE).of("thingy").in("related").execute().getCentralityCount().get();
            assertEquals(degrees1, degrees3);
        }
    }

    @Test
    public void testSubIsAccountedForInSubgraph() {
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");

        Entity person = tx.putEntityType("person").plays(owner).addEntity();

        EntityType animal = tx.putEntityType("animal").plays(pet);
        Entity dog = tx.putEntityType("dog").sup(animal).addEntity();

        tx.putRelationshipType("mans-best-friend").relates(pet).relates(owner)
                .addRelationship().addRolePlayer(pet, dog).addRolePlayer(owner, person);

        Map<Long, Set<ConceptId>> correctDegrees = new HashMap<>();
        correctDegrees.put(1L, Sets.newHashSet(person.getId(), dog.getId()));

        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            // set subgraph, use animal instead of dog
            Set<Label> ct = Sets.newHashSet(Label.of("person"), Label.of("animal"),
                    Label.of("mans-best-friend"));
            Map<Long, Set<ConceptId>> degrees = graph.graql().compute(CENTRALITY).using(DEGREE).in(ct).execute().getCentralityCount().get();
            // check that dog has a degree to confirm sub has been inferred
            assertEquals(correctDegrees, degrees);
        }
    }

    @Test
    public void testDegreeTwoAttributes() throws InvalidKBException {
        // create a simple graph
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        RelationshipType mansBestFriend = tx.putRelationshipType("mans-best-friend").relates(pet).relates(owner);

        EntityType person = tx.putEntityType("person").plays(owner);
        EntityType animal = tx.putEntityType("animal").plays(pet);
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<String> altName =
                tx.putAttributeType("alternate-name", AttributeType.DataType.STRING);

        animal.attribute(name).attribute(altName);

        // add data to the graph
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Attribute coconut = name.putAttribute("coconut");
        Attribute stinky = altName.putAttribute("stinky");
        mansBestFriend.addRelationship().addRolePlayer(owner, dave).addRolePlayer(pet, coco);
        coco.attribute(coconut).attribute(stinky);

        // manually compute the degree for small graph
        Map<Long, Set<ConceptId>> subgraphReferenceDegrees = new HashMap<>();
        subgraphReferenceDegrees.put(1L, Sets.newHashSet(coco.getId(), dave.getId()));

        // manually compute degree for almost full graph
        Map<Long, Set<ConceptId>> almostFullReferenceDegrees = new HashMap<>();
        almostFullReferenceDegrees.put(2L, Sets.newHashSet(coco.getId()));
        almostFullReferenceDegrees.put(1L, Sets.newHashSet(dave.getId(), coconut.getId()));

        // manually compute degrees
        Map<Long, Set<ConceptId>> fullReferenceDegrees = new HashMap<>();
        fullReferenceDegrees.put(3L, Sets.newHashSet(coco.getId()));
        fullReferenceDegrees.put(1L, Sets.newHashSet(dave.getId(), coconut.getId(), stinky.getId()));

        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {

            // create a subgraph excluding attributes and their relationship
            HashSet<Label> subGraphTypes = Sets.newHashSet(Label.of("animal"), Label.of("person"),
                    Label.of("mans-best-friend"));
            Map<Long, Set<ConceptId>> degrees = graph.graql().compute(CENTRALITY).using(DEGREE)
                    .in(subGraphTypes).execute().getCentralityCount().get();
            assertEquals(subgraphReferenceDegrees, degrees);

            // create a subgraph excluding one attribute type only
            HashSet<Label> almostFullTypes = Sets.newHashSet(Label.of("animal"), Label.of("person"),
                    Label.of("mans-best-friend"), Label.of("@has-name"), Label.of("name"));
            degrees = graph.graql().compute(CENTRALITY).using(DEGREE).in(almostFullTypes).execute().getCentralityCount().get();
            assertEquals(almostFullReferenceDegrees, degrees);

            // full graph
            degrees = graph.graql().compute(CENTRALITY).using(DEGREE).execute().getCentralityCount().get();
            assertEquals(fullReferenceDegrees, degrees);
        }
    }

    @Test
    public void testDegreeMissingRolePlayer() {
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        Role breeder = tx.putRole("breeder");
        RelationshipType mansBestFriend = tx.putRelationshipType("mans-best-friend")
                .relates(pet).relates(owner).relates(breeder);
        EntityType person = tx.putEntityType("person").plays(owner).plays(breeder);
        EntityType animal = tx.putEntityType("animal").plays(pet);

        // make one person breeder and owner
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        mansBestFriend.addRelationship().addRolePlayer(pet, coco).addRolePlayer(owner, dave);

        // manual degrees
        Map<Long, Set<ConceptId>> referenceDegrees = new HashMap<>();
        referenceDegrees.put(1L, Sets.newHashSet(coco.getId(), dave.getId()));

        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            Map<Long, Set<ConceptId>> degrees = graph.graql().compute(CENTRALITY).using(DEGREE).execute().getCentralityCount().get();
            assertEquals(referenceDegrees, degrees);
        }
    }

    @Test
    public void testRelationshipPlaysARole() throws InvalidKBException {

        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        RelationshipType mansBestFriend = tx.putRelationshipType("mans-best-friend").relates(pet).relates(owner);

        EntityType person = tx.putEntityType("person").plays(owner);
        EntityType animal = tx.putEntityType("animal").plays(pet);

        Role ownership = tx.putRole("ownership");
        Role ownershipResource = tx.putRole("ownership-resource");
        RelationshipType hasOwnershipResource = tx.putRelationshipType("has-ownership-resource")
                .relates(ownership).relates(ownershipResource);

        AttributeType<String> startDate = tx.putAttributeType("start-date", AttributeType.DataType.STRING);
        startDate.plays(ownershipResource);
        mansBestFriend.plays(ownership);

        // add instances
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Relationship daveOwnsCoco = mansBestFriend.addRelationship()
                .addRolePlayer(owner, dave).addRolePlayer(pet, coco);
        Attribute aStartDate = startDate.putAttribute("01/01/01");
        hasOwnershipResource.addRelationship()
                .addRolePlayer(ownershipResource, aStartDate).addRolePlayer(ownership, daveOwnsCoco);

        Map<Long, Set<ConceptId>> referenceDegrees = new HashMap<>();
        referenceDegrees.put(1L, Sets.newHashSet(coco.getId(), dave.getId(), aStartDate.getId(), daveOwnsCoco.getId()));

        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            Map<Long, Set<ConceptId>> degrees = graph.graql().compute(CENTRALITY).using(DEGREE).execute().getCentralityCount().get();
            assertEquals(referenceDegrees, degrees);
        }
    }

    @Test
    public void testDegreeTernaryRelationships() throws InvalidKBException {
        // make relationship
        Role productionWithCast = tx.putRole("production-with-cast");
        Role actor = tx.putRole("actor");
        Role characterBeingPlayed = tx.putRole("character-being-played");
        RelationshipType hasCast = tx.putRelationshipType("has-cast")
                .relates(productionWithCast)
                .relates(actor)
                .relates(characterBeingPlayed);

        EntityType movie = tx.putEntityType("movie").plays(productionWithCast);
        EntityType person = tx.putEntityType("person").plays(actor);
        EntityType character = tx.putEntityType("character").plays(characterBeingPlayed);

        Entity godfather = movie.addEntity();
        Entity marlonBrando = person.addEntity();
        Entity donVitoCorleone = character.addEntity();

        hasCast.addRelationship()
                .addRolePlayer(productionWithCast, godfather)
                .addRolePlayer(actor, marlonBrando)
                .addRolePlayer(characterBeingPlayed, donVitoCorleone);

        Map<Long, Set<ConceptId>> referenceDegrees = new HashMap<>();
        referenceDegrees.put(1L, Sets.newHashSet(godfather.getId(), marlonBrando.getId(),
                donVitoCorleone.getId()));

        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            Map<Long, Set<ConceptId>> degrees = graph.graql().compute(CENTRALITY).using(DEGREE).execute().getCentralityCount().get();
            assertEquals(referenceDegrees, degrees);
        }
    }

    @Test
    public void testOneRolePlayerMultipleRoles() throws InvalidKBException {

        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        Role breeder = tx.putRole("breeder");
        RelationshipType mansBestFriend = tx.putRelationshipType("mans-best-friend")
                .relates(pet).relates(owner).relates(breeder);
        EntityType person = tx.putEntityType("person").plays(owner).plays(breeder);
        EntityType animal = tx.putEntityType("animal").plays(pet);

        // make one person breeder and owner
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();

        mansBestFriend.addRelationship()
                .addRolePlayer(pet, coco)
                .addRolePlayer(owner, dave)
                .addRolePlayer(breeder, dave);

        Map<Long, Set<ConceptId>> referenceDegrees = new HashMap<>();
        referenceDegrees.put(1L, Sets.newHashSet(coco.getId()));
        referenceDegrees.put(2L, Collections.singleton(dave.getId()));

        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            Map<Long, Set<ConceptId>> degrees = graph.graql().compute(CENTRALITY).using(DEGREE).execute().getCentralityCount().get();
            assertEquals(referenceDegrees, degrees);
        }
    }
}