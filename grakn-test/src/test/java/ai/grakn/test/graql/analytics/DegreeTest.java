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

package ai.grakn.test.graql.analytics;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.computer.GraknSparkComputer;
import ai.grakn.test.EngineContext;
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

import static ai.grakn.test.GraknTestEnv.usingOrientDB;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class DegreeTest {

    @ClassRule
    public static final EngineContext context = EngineContext.startInMemoryServer();
    private GraknSession factory;
    private GraknGraph graph;

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());

        factory = context.factoryWithNewKeyspace();
        graph = factory.open(GraknTxType.WRITE);
    }

    @Test
    public void testDegrees() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");

        ConceptId entity1 = thing.addEntity().getId();
        ConceptId entity2 = thing.addEntity().getId();
        ConceptId entity3 = thing.addEntity().getId();
        ConceptId entity4 = anotherThing.addEntity().getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        thing.plays(role1).plays(role2);
        anotherThing.plays(role1).plays(role2);
        RelationType related = graph.putRelationType("related").relates(role1).relates(role2);

        // relate them
        ConceptId id1 = related.addRelation()
                .addRolePlayer(role1, graph.getConcept(entity1))
                .addRolePlayer(role2, graph.getConcept(entity2))
                .getId();
        ConceptId id2 = related.addRelation()
                .addRolePlayer(role1, graph.getConcept(entity2))
                .addRolePlayer(role2, graph.getConcept(entity3))
                .getId();
        ConceptId id3 = related.addRelation()
                .addRolePlayer(role1, graph.getConcept(entity2))
                .addRolePlayer(role2, graph.getConcept(entity4))
                .getId();
        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

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
        for (long i = 0L; i < 4L; i++) {
            list.add(i);
        }
        GraknSparkComputer.clear();
        graph.close();
        list.parallelStream().forEach(i -> {
            try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
                Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();

                assertEquals(3, degrees.size());
                degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                        id -> {
                            assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                            assertEquals(correctDegrees.get(ConceptId.of(id)), entry.getKey());
                        }
                ));
            }
        });

        graph = factory.open(GraknTxType.WRITE);
        Map<Long, Set<String>> degrees2 = graph.graql().compute().degree().of("thing").execute();

        assertEquals(2, degrees2.size());
        assertEquals(2, degrees2.get(1L).size());
        assertEquals(1, degrees2.get(3L).size());
        degrees2.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(correctDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        degrees2 = graph.graql().compute().degree().of("thing", "related").execute();
        assertEquals(3, degrees2.size());
        assertEquals(2, degrees2.get(1L).size());
        assertEquals(3, degrees2.get(2L).size());
        assertEquals(1, degrees2.get(3L).size());
        degrees2.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(correctDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        degrees2 = graph.graql().compute().degree().of().execute();

        assertEquals(3, degrees2.size());
        assertEquals(3, degrees2.get(1L).size());
        assertEquals(3, degrees2.get(2L).size());
        assertEquals(1, degrees2.get(3L).size());
        degrees2.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(correctDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        // compute degrees on subgraph
        Map<Long, Set<String>> degrees3 = graph.graql().compute().degree().in("thing", "related").execute();
        correctDegrees.put(id3, 1L);
        assertTrue(!degrees3.isEmpty());
        degrees3.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(correctDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        degrees3 = graph.graql().compute().degree().of("thing").in("thing", "related").execute();
        assertEquals(2, degrees3.size());
        assertEquals(2, degrees3.get(1L).size());
        assertEquals(1, degrees3.get(3L).size());
        degrees3.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(correctDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));
    }

    @Test
    public void testSubIsAccountedForInSubgraph() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        graph.putRelationType("mans-best-friend").relates(pet).relates(owner);
        graph.putEntityType("person").plays(owner);
        EntityType animal = graph.putEntityType("animal").plays(pet);
        EntityType dog = graph.putEntityType("dog").superType(animal);
        ConceptId foofoo = dog.addEntity().getId();
        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        // set subgraph
        HashSet<TypeLabel> ct = Sets.newHashSet(TypeLabel.of("person"), TypeLabel.of("animal"), TypeLabel.of("mans-best-friend"));
        Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(ct).execute();

        // check that dog has a degree to confirm sub has been inferred
        assertTrue(degrees.keySet().iterator().next().equals(0L));
    }

    @Test
    public void testDegreeIsCorrect() throws GraknValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").relates(pet).relates(owner);
        RoleType target = graph.putRoleType("target");
        RoleType value = graph.putRoleType("value");
        RelationType hasName = graph.putRelationType("has-name").relates(value).relates(target);
        EntityType person = graph.putEntityType("person").plays(owner);
        EntityType animal = graph.putEntityType("animal").plays(pet).plays(target);
        ResourceType<String> name = graph.putResourceType("name", ResourceType.DataType.STRING).plays(value);
        ResourceType<String> altName =
                graph.putResourceType("alternate-name", ResourceType.DataType.STRING).plays(value);

        // add data to the graph
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Resource coconut = name.putResource("coconut");
        Resource stinky = altName.putResource("stinky");
        Relation daveOwnsCoco = mansBestFriend.addRelation().addRolePlayer(owner, dave).addRolePlayer(pet, coco);
        Relation cocoName = hasName.addRelation().addRolePlayer(target, coco).addRolePlayer(value, coconut);
        Relation cocoAltName = hasName.addRelation().addRolePlayer(target, coco).addRolePlayer(value, stinky);

        // manually compute the degree for small graph
        Map<ConceptId, Long> subGraphReferenceDegrees = new HashMap<>();
        subGraphReferenceDegrees.put(coco.getId(), 1L);
        subGraphReferenceDegrees.put(dave.getId(), 1L);
        subGraphReferenceDegrees.put(daveOwnsCoco.getId(), 2L);

        // manually compute degree for almost full graph
        Map<ConceptId, Long> almostFullReferenceDegrees = new HashMap<>();
        almostFullReferenceDegrees.put(coco.getId(), 3L);
        almostFullReferenceDegrees.put(dave.getId(), 1L);
        almostFullReferenceDegrees.put(daveOwnsCoco.getId(), 2L);
        almostFullReferenceDegrees.put(cocoName.getId(), 2L);
        almostFullReferenceDegrees.put(cocoAltName.getId(), 1L);
        almostFullReferenceDegrees.put(coconut.getId(), 1L);

        // manually compute degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 3L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(coconut.getId(), 1L);
        referenceDegrees.put(stinky.getId(), 1L);
        referenceDegrees.put(daveOwnsCoco.getId(), 2L);
        referenceDegrees.put(cocoName.getId(), 2L);
        referenceDegrees.put(cocoAltName.getId(), 2L);

        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        // create a subgraph excluding resources and the relationship
        HashSet<TypeLabel> subGraphTypes = Sets.newHashSet(TypeLabel.of("animal"), TypeLabel.of("person"), TypeLabel.of("mans-best-friend"));
        Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(subGraphTypes).execute();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(subGraphReferenceDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(subGraphReferenceDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        // create a subgraph excluding resource type only
        HashSet<TypeLabel> almostFullTypes = Sets.newHashSet(TypeLabel.of("animal"), TypeLabel.of("person"), TypeLabel.of("mans-best-friend"), TypeLabel.of("has-name"), TypeLabel.of("name"));
        degrees = graph.graql().compute().degree().in(almostFullTypes).execute();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(almostFullReferenceDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(almostFullReferenceDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        // full graph
        degrees = graph.graql().compute().degree().execute();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(referenceDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));
    }

    @Test
    public void testDegreeMissingRolePlayer() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RoleType breeder = graph.putRoleType("breeder");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend")
                .relates(pet).relates(owner).relates(breeder);
        EntityType person = graph.putEntityType("person").plays(owner).plays(breeder);
        EntityType animal = graph.putEntityType("animal").plays(pet);

        // make one person breeder and owner
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Relation daveBreedsAndOwnsCoco = mansBestFriend.addRelation()
                .addRolePlayer(pet, coco).addRolePlayer(owner, dave);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        // compute and persist degrees
        Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();

        // check degrees are correct
        referenceDegrees.entrySet().forEach(entry ->
                assertTrue(degrees.get(entry.getValue()).contains(entry.getKey().getValue())));
        degrees.entrySet().forEach(entry ->
                entry.getValue().forEach(id -> assertEquals(entry.getKey(), referenceDegrees.get(ConceptId.of(id)))));
    }

    @Test
    public void testDegreeAssertionAboutAssertion()
            throws GraknValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").relates(pet).relates(owner);
        RoleType target = graph.putRoleType("target");
        RoleType value = graph.putRoleType("value");
        RelationType hasName = graph.putRelationType("has-name").relates(value).relates(target);
        EntityType person = graph.putEntityType("person").plays(owner);
        EntityType animal = graph.putEntityType("animal").plays(pet).plays(target);
        ResourceType<String> name = graph.putResourceType("name", ResourceType.DataType.STRING).plays(value);
        ResourceType<String> altName =
                graph.putResourceType("alternate-name", ResourceType.DataType.STRING).plays(value);
        RoleType ownership = graph.putRoleType("ownership");
        RoleType ownershipResource = graph.putRoleType("ownership-resource");
        RelationType hasOwnershipResource =
                graph.putRelationType("has-ownership-resource").relates(ownership).relates(ownershipResource);
        ResourceType<String> startDate =
                graph.putResourceType("start-date", ResourceType.DataType.STRING).plays(ownershipResource);
        mansBestFriend.plays(ownership);

        // add data to the graph
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Resource coconut = name.putResource("coconut");
        Resource stinky = altName.putResource("stinky");
        Relation daveOwnsCoco = mansBestFriend.addRelation()
                .addRolePlayer(owner, dave).addRolePlayer(pet, coco);
        hasName.addRelation().addRolePlayer(target, coco).addRolePlayer(value, coconut);
        hasName.addRelation().addRolePlayer(target, coco).addRolePlayer(value, stinky);
        Resource sd = startDate.putResource("01/01/01");
        Relation ownsFrom = hasOwnershipResource.addRelation()
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

        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        // create a subgraph with assertion on assertion
        HashSet<TypeLabel> ct =
                Sets.newHashSet(TypeLabel.of("animal"), TypeLabel.of("person"), TypeLabel.of("mans-best-friend"), TypeLabel.of("start-date"), TypeLabel.of("has-ownership-resource"));
        Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(ct).execute();
        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees1.containsKey(ConceptId.of(id)));
                    assertEquals(referenceDegrees1.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        // create subgraph without assertion on assertion
        ct.clear();
        ct.add(TypeLabel.of("animal"));
        ct.add(TypeLabel.of("person"));
        ct.add(TypeLabel.of("mans-best-friend"));
        degrees = graph.graql().compute().degree().in(ct).execute();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees2.containsKey(ConceptId.of(id)));
                    assertEquals(referenceDegrees2.get(ConceptId.of(id)), entry.getKey());
                }
        ));
    }

    @Test
    public void testDegreeTernaryRelationships()
            throws GraknValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // make relation
        RoleType productionWithCast = graph.putRoleType("production-with-cast");
        RoleType actor = graph.putRoleType("actor");
        RoleType characterBeingPlayed = graph.putRoleType("character-being-played");
        RelationType hasCast = graph.putRelationType("has-cast")
                .relates(productionWithCast)
                .relates(actor)
                .relates(characterBeingPlayed);

        EntityType movie = graph.putEntityType("movie").plays(productionWithCast);
        EntityType person = graph.putEntityType("person").plays(actor);
        EntityType character = graph.putEntityType("character").plays(characterBeingPlayed);

        Entity godfather = movie.addEntity();
        Entity marlonBrando = person.addEntity();
        ConceptId marlonId = marlonBrando.getId();
        Entity donVitoCorleone = character.addEntity();

        Relation relation = hasCast.addRelation()
                .addRolePlayer(productionWithCast, godfather)
                .addRolePlayer(actor, marlonBrando)
                .addRolePlayer(characterBeingPlayed, donVitoCorleone);
        ConceptId relationId = relation.getId();

        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();
        assertTrue(degrees.get(3L).contains(relationId.getValue()));
        assertTrue(degrees.get(1L).contains(marlonId.getValue()));
    }

    @Test
    public void testDegreeOneRolePlayerMultipleRoles()
            throws GraknValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RoleType breeder = graph.putRoleType("breeder");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend")
                .relates(pet).relates(owner).relates(breeder);
        EntityType person = graph.putEntityType("person").plays(owner).plays(breeder);
        EntityType animal = graph.putEntityType("animal").plays(pet);

        // make one person breeder and owner
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();

        Relation daveBreedsAndOwnsCoco = mansBestFriend.addRelation()
                .addRolePlayer(pet, coco)
                .addRolePlayer(owner, dave)
                .addRolePlayer(breeder, dave);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 2L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);

        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(referenceDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));
    }

    @Test
    public void testDegreeRolePlayerWrongType()
            throws GraknValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RoleType breeder = graph.putRoleType("breeder");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend")
                .relates(pet).relates(owner).relates(breeder);
        EntityType person = graph.putEntityType("person").plays(owner).plays(breeder);
        EntityType dog = graph.putEntityType("dog").plays(pet);
        EntityType cat = graph.putEntityType("cat").plays(pet);

        // make one person breeder and owner of a dog and a cat
        Entity beast = dog.addEntity();
        Entity coco = cat.addEntity();
        Entity dave = person.addEntity();
        Relation daveBreedsAndOwnsCoco = mansBestFriend.addRelation()
                .addRolePlayer(owner, dave).addRolePlayer(breeder, dave).addRolePlayer(pet, coco);
        Relation daveBreedsAndOwnsBeast = mansBestFriend.addRelation()
                .addRolePlayer(owner, dave).addRolePlayer(breeder, dave).addRolePlayer(pet, beast);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 4L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);
        referenceDegrees.put(daveBreedsAndOwnsBeast.getId(), 2L);

        // validate
        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        // check degree for dave owning cats
        //TODO: should we count the relationship even if there is no cat attached?
        HashSet<TypeLabel> ct = Sets.newHashSet(TypeLabel.of("mans-best-friend"), TypeLabel.of("cat"), TypeLabel.of("person"));
        Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(ct).execute();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(referenceDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));
    }
}