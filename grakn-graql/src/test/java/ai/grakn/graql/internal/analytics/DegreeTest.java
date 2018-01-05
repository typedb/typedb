/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    public void testDegreesSimple() throws Exception {
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
        ConceptId id1 = related.addRelationship()
                .addRolePlayer(role1, tx.getConcept(entity1))
                .addRolePlayer(role2, tx.getConcept(entity2))
                .getId();
        ConceptId id2 = related.addRelationship()
                .addRolePlayer(role1, tx.getConcept(entity2))
                .addRolePlayer(role2, tx.getConcept(entity3))
                .getId();
        ConceptId id3 = related.addRelationship()
                .addRolePlayer(role1, tx.getConcept(entity2))
                .addRolePlayer(role2, tx.getConcept(entity4))
                .getId();
        tx.commit();
        tx = session.open(GraknTxType.READ);

        Map<ConceptId, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1L);
        correctDegrees.put(entity2, 3L);
        correctDegrees.put(entity3, 1L);
        correctDegrees.put(entity4, 1L);
        correctDegrees.put(id1, 2L);
        correctDegrees.put(id2, 2L);
        correctDegrees.put(id3, 2L);

        // compute degrees
        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        if (GraknTestUtil.usingTinker()) workerNumber = 1L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }
        tx.close();

        Set<Map<Long, Set<String>>> result = list.parallelStream().map(i -> {
            try (GraknTx graph = session.open(GraknTxType.READ)) {
                return graph.graql().compute().degree().execute();
            }
        }).collect(Collectors.toSet());
        result.forEach(degrees -> {
            assertEquals(3, degrees.size());
            degrees.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(correctDegrees.get(ConceptId.of(id)), key);
                    }
            ));
        });

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            Map<Long, Set<String>> degrees2 = graph.graql().compute().degree().of("thingy").execute();

            assertEquals(2, degrees2.size());
            assertEquals(2, degrees2.get(1L).size());
            assertEquals(1, degrees2.get(3L).size());
            degrees2.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(correctDegrees.get(ConceptId.of(id)), key);
                    }
            ));

            degrees2 = graph.graql().compute().degree().of("thingy", "related").execute();
            assertEquals(3, degrees2.size());
            assertEquals(2, degrees2.get(1L).size());
            assertEquals(3, degrees2.get(2L).size());
            assertEquals(1, degrees2.get(3L).size());
            degrees2.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(correctDegrees.get(ConceptId.of(id)), key);
                    }
            ));

            degrees2 = graph.graql().compute().degree().of().execute();

            assertEquals(3, degrees2.size());
            assertEquals(3, degrees2.get(1L).size());
            assertEquals(3, degrees2.get(2L).size());
            assertEquals(1, degrees2.get(3L).size());
            degrees2.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(correctDegrees.get(ConceptId.of(id)), key);
                    }
            ));

            // compute degrees on subgraph
            Map<Long, Set<String>> degrees3 = graph.graql().compute().degree().in("thingy", "related").execute();
            correctDegrees.put(id3, 1L);
            assertTrue(!degrees3.isEmpty());
            degrees3.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(correctDegrees.get(ConceptId.of(id)), key);
                    }
            ));

            degrees3 = graph.graql().compute().degree().of("thingy").in("related").execute();
            assertEquals(2, degrees3.size());
            assertEquals(2, degrees3.get(1L).size());
            assertEquals(1, degrees3.get(3L).size());
            degrees3.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(correctDegrees.get(ConceptId.of(id)), key);
                    }
            ));
        }
    }

    @Test
    public void testSubIsAccountedForInSubgraph() throws Exception {
        // create a simple tx
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        tx.putRelationshipType("mans-best-friend").relates(pet).relates(owner);
        tx.putEntityType("person").plays(owner);
        EntityType animal = tx.putEntityType("animal").plays(pet);
        EntityType dog = tx.putEntityType("dog").sup(animal);
        dog.addEntity();
        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            // set subgraph
            HashSet<Label> ct = Sets.newHashSet(Label.of("person"), Label.of("animal"),
                    Label.of("mans-best-friend"));
            Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(ct).execute();

            // check that dog has a degree to confirm sub has been inferred
            assertTrue(degrees.keySet().iterator().next().equals(0L));
        }
    }

    @Test
    public void testDegreeTwoAttributes() throws InvalidKBException, ExecutionException, InterruptedException {
        // create a simple tx
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        RelationshipType mansBestFriend = tx.putRelationshipType("mans-best-friend").relates(pet).relates(owner);

        EntityType person = tx.putEntityType("person").plays(owner);
        EntityType animal = tx.putEntityType("animal").plays(pet);
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<String> altName =
                tx.putAttributeType("alternate-name", AttributeType.DataType.STRING);

        animal.attribute(name).attribute(altName);

        // add data to the tx
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Attribute coconut = name.putAttribute("coconut");
        Attribute stinky = altName.putAttribute("stinky");
        Relationship daveOwnsCoco = mansBestFriend.addRelationship()
                .addRolePlayer(owner, dave)
                .addRolePlayer(pet, coco);
        coco.attribute(coconut).attribute(stinky);

        // manually compute the degree for small tx
        Map<ConceptId, Long> subGraphReferenceDegrees = new HashMap<>();
        subGraphReferenceDegrees.put(coco.getId(), 1L);
        subGraphReferenceDegrees.put(dave.getId(), 1L);
        subGraphReferenceDegrees.put(daveOwnsCoco.getId(), 2L);

        // manually compute degree for almost full tx
        Map<ConceptId, Long> almostFullReferenceDegrees = new HashMap<>();
        almostFullReferenceDegrees.put(coco.getId(), 2L);
        almostFullReferenceDegrees.put(dave.getId(), 1L);
        almostFullReferenceDegrees.put(daveOwnsCoco.getId(), 2L);
        almostFullReferenceDegrees.put(coconut.getId(), 1L);

        // manually compute degrees
        Map<ConceptId, Long> fullReferenceDegrees = new HashMap<>();
        fullReferenceDegrees.put(coco.getId(), 3L);
        fullReferenceDegrees.put(dave.getId(), 1L);
        fullReferenceDegrees.put(coconut.getId(), 1L);
        fullReferenceDegrees.put(stinky.getId(), 1L);
        fullReferenceDegrees.put(daveOwnsCoco.getId(), 2L);

        tx.commit();
        try (GraknTx graph = session.open(GraknTxType.READ)) {

            // create a subgraph excluding attributes and their relationship
            HashSet<Label> subGraphTypes = Sets.newHashSet(Label.of("animal"), Label.of("person"),
                    Label.of("mans-best-friend"));
            Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(subGraphTypes).execute();
            assertEquals(2, degrees.size());
            System.out.println("degrees = " + degrees);

            degrees.forEach((key, value1) -> value1.forEach(
                    id -> {
                        assertTrue(subGraphReferenceDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(subGraphReferenceDegrees.get(ConceptId.of(id)), key);
                    }
            ));

            degrees = graph.graql().compute().degree().execute();
            assertEquals(2, degrees.size());
            System.out.println("degrees = " + degrees);

            degrees.forEach((key, value1) -> value1.forEach(
                    id -> {
                        assertTrue(subGraphReferenceDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(subGraphReferenceDegrees.get(ConceptId.of(id)), key);
                    }
            ));

            // create a subgraph excluding one attribute type only
            HashSet<Label> almostFullTypes = Sets.newHashSet(Label.of("animal"), Label.of("person"),
                    Label.of("mans-best-friend"), Label.of("@has-name"), Label.of("name"));
            degrees = graph.graql().compute().degree().in(almostFullTypes).execute();
            System.out.println("degrees = " + degrees);

            assertEquals(2, degrees.size());
            degrees.forEach((key, value1) -> value1.forEach(
                    id -> {
                        assertTrue(almostFullReferenceDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(almostFullReferenceDegrees.get(ConceptId.of(id)), key);
                    }
            ));

            // full tx
            degrees = graph.graql().compute().degree().includeAttribute().of().execute();
            assertEquals(3, degrees.size());
            System.out.println("degrees = " + degrees);
            degrees.forEach((key, value1) -> value1.forEach(
                    id -> {
                        assertTrue(fullReferenceDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(fullReferenceDegrees.get(ConceptId.of(id)), key);
                    }
            ));
        }
    }

    @Test
    public void testDegreeMissingRolePlayer() throws Exception {
        // create a simple tx
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
        Relationship daveBreedsAndOwnsCoco = mansBestFriend.addRelationship()
                .addRolePlayer(pet, coco).addRolePlayer(owner, dave);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        tx.commit();
        try (GraknTx graph = session.open(GraknTxType.READ)) {

            // compute and persist degrees
            Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();

            // check degrees are correct
            referenceDegrees.forEach((key, value) -> assertTrue(degrees.get(value).contains(key.getValue())));
            degrees.forEach((key, value) -> value.forEach(id ->
                    assertEquals(key, referenceDegrees.get(ConceptId.of(id)))));
        }
    }

    @Test
    public void testDegreeAssertionAboutAssertion()
            throws InvalidKBException, ExecutionException, InterruptedException {
        // create a simple tx
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        RelationshipType mansBestFriend = tx.putRelationshipType("mans-best-friend").relates(pet).relates(owner);
        Role target = tx.putRole("target");
        Role value = tx.putRole("value");
        RelationshipType hasName = tx.putRelationshipType("has-name").relates(value).relates(target);
        EntityType person = tx.putEntityType("person").plays(owner);
        EntityType animal = tx.putEntityType("animal").plays(pet).plays(target);
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING).plays(value);
        AttributeType<String> altName =
                tx.putAttributeType("alternate-name", AttributeType.DataType.STRING).plays(value);
        Role ownership = tx.putRole("ownership");
        Role ownershipResource = tx.putRole("ownership-resource");
        RelationshipType hasOwnershipResource =
                tx.putRelationshipType("has-ownership-resource").relates(ownership).relates(ownershipResource);
        AttributeType<String> startDate =
                tx.putAttributeType("start-date", AttributeType.DataType.STRING).plays(ownershipResource);
        mansBestFriend.plays(ownership);

        // add data to the tx
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Attribute coconut = name.putAttribute("coconut");
        Attribute stinky = altName.putAttribute("stinky");
        Relationship daveOwnsCoco = mansBestFriend.addRelationship()
                .addRolePlayer(owner, dave).addRolePlayer(pet, coco);
        hasName.addRelationship().addRolePlayer(target, coco).addRolePlayer(value, coconut);
        hasName.addRelationship().addRolePlayer(target, coco).addRolePlayer(value, stinky);
        Attribute sd = startDate.putAttribute("01/01/01");
        Relationship ownsFrom = hasOwnershipResource.addRelationship()
                .addRolePlayer(ownershipResource, sd).addRolePlayer(ownership, daveOwnsCoco);

        // manually compute the degree
        Map<ConceptId, Long> referenceDegrees1 = new HashMap<>();
        referenceDegrees1.put(coco.getId(), 1L);
        referenceDegrees1.put(dave.getId(), 1L);
        referenceDegrees1.put(daveOwnsCoco.getId(), 3L);
        referenceDegrees1.put(sd.getId(), 1L);
        referenceDegrees1.put(ownsFrom.getId(), 2L);

        // manually compute degrees
        Map<ConceptId, Long> referenceDegrees2 = new HashMap<>();
        referenceDegrees2.put(coco.getId(), 1L);
        referenceDegrees2.put(dave.getId(), 1L);
        referenceDegrees2.put(daveOwnsCoco.getId(), 2L);

        tx.commit();
        try (GraknTx graph = session.open(GraknTxType.READ)) {

            // create a subgraph with assertion on assertion
            HashSet<Label> ct =
                    Sets.newHashSet(Label.of("animal"),
                            Label.of("person"),
                            Label.of("mans-best-friend"),
                            Label.of("start-date"),
                            Label.of("has-ownership-resource"));
            Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(ct).execute();
            assertTrue(!degrees.isEmpty());
            degrees.forEach((key1, value2) -> value2.forEach(
                    id -> {
                        assertTrue(referenceDegrees1.containsKey(ConceptId.of(id)));
                        assertEquals(referenceDegrees1.get(ConceptId.of(id)), key1);
                    }
            ));

            // create subgraph without assertion on assertion
            ct.clear();
            ct.add(Label.of("animal"));
            ct.add(Label.of("person"));
            ct.add(Label.of("mans-best-friend"));
            degrees = graph.graql().compute().degree().in(ct).execute();
            assertFalse(degrees.isEmpty());
            degrees.forEach((key, value1) -> value1.forEach(
                    id -> {
                        assertTrue(referenceDegrees2.containsKey(ConceptId.of(id)));
                        assertEquals(referenceDegrees2.get(ConceptId.of(id)), key);
                    }
            ));
        }
    }

    @Test
    public void testDegreeTernaryRelationships()
            throws InvalidKBException, ExecutionException, InterruptedException {
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
        ConceptId marlonId = marlonBrando.getId();
        Entity donVitoCorleone = character.addEntity();

        Relationship relationship = hasCast.addRelationship()
                .addRolePlayer(productionWithCast, godfather)
                .addRolePlayer(actor, marlonBrando)
                .addRolePlayer(characterBeingPlayed, donVitoCorleone);
        ConceptId relationId = relationship.getId();

        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();
            assertTrue(degrees.get(3L).contains(relationId.getValue()));
            assertTrue(degrees.get(1L).contains(marlonId.getValue()));
        }
    }

    @Test
    public void testDegreeOneRolePlayerMultipleRoles()
            throws InvalidKBException, ExecutionException, InterruptedException {
        // create a simple tx
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

        Relationship daveBreedsAndOwnsCoco = mansBestFriend.addRelationship()
                .addRolePlayer(pet, coco)
                .addRolePlayer(owner, dave)
                .addRolePlayer(breeder, dave);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 2L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);

        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();
            assertFalse(degrees.isEmpty());
            degrees.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(referenceDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(referenceDegrees.get(ConceptId.of(id)), key);
                    }
            ));
        }
    }

    @Test
    public void testDegreeRolePlayerWrongType()
            throws InvalidKBException, ExecutionException, InterruptedException {
        // create a simple tx
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        Role breeder = tx.putRole("breeder");
        RelationshipType mansBestFriend = tx.putRelationshipType("mans-best-friend")
                .relates(pet).relates(owner).relates(breeder);
        EntityType person = tx.putEntityType("person").plays(owner).plays(breeder);
        EntityType dog = tx.putEntityType("dog").plays(pet);
        EntityType cat = tx.putEntityType("cat").plays(pet);

        // make one person breeder and owner of a dog and a cat
        Entity beast = dog.addEntity();
        Entity coco = cat.addEntity();
        Entity dave = person.addEntity();
        Relationship daveBreedsAndOwnsCoco = mansBestFriend.addRelationship()
                .addRolePlayer(owner, dave).addRolePlayer(breeder, dave).addRolePlayer(pet, coco);
        Relationship daveBreedsAndOwnsBeast = mansBestFriend.addRelationship()
                .addRolePlayer(owner, dave).addRolePlayer(breeder, dave).addRolePlayer(pet, beast);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 4L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);
        referenceDegrees.put(daveBreedsAndOwnsBeast.getId(), 2L);

        // validate
        tx.commit();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            // check degree for dave owning cats
            //TODO: should we count the relationship even if there is no cat attached?
            HashSet<Label> ct = Sets.newHashSet(Label.of("mans-best-friend"), Label.of("cat"),
                    Label.of("person"));
            Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(ct).execute();
            assertFalse(degrees.isEmpty());
            degrees.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(referenceDegrees.containsKey(ConceptId.of(id)));
                        assertEquals(referenceDegrees.get(ConceptId.of(id)), key);
                    }
            ));
        }
    }
}