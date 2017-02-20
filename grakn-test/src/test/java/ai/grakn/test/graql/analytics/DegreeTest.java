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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.EngineContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
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
    private GraknGraph graph;

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());

        graph = context.factoryWithNewKeyspace().getGraph();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(ComputeQuery.class);
        logger.setLevel(Level.DEBUG);
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
        thing.playsRole(role1).playsRole(role2);
        anotherThing.playsRole(role1).playsRole(role2);
        RelationType related = graph.putRelationType("related").hasRole(role1).hasRole(role2);

        // relate them
        ConceptId id1 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity1))
                .putRolePlayer(role2, graph.getConcept(entity2))
                .getId();
        ConceptId id2 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity2))
                .putRolePlayer(role2, graph.getConcept(entity3))
                .getId();
        ConceptId id3 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity2))
                .putRolePlayer(role2, graph.getConcept(entity4))
                .getId();
        graph.commitOnClose();
        graph.close();

        Map<ConceptId, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1L);
        correctDegrees.put(entity2, 3L);
        correctDegrees.put(entity3, 1L);
        correctDegrees.put(entity4, 1L);
        correctDegrees.put(id1, 2L);
        correctDegrees.put(id2, 2L);
        correctDegrees.put(id3, 2L);

        // compute degrees
        long start = System.currentTimeMillis();
        Map<Long, Set<String>> degrees = graph.graql().compute().degree().execute();
        System.out.println(System.currentTimeMillis() - start + " ms");

        assertEquals(3, degrees.size());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(correctDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        start = System.currentTimeMillis();
        Map<Long, Set<String>> degrees2 = graph.graql().compute().degree().of("thing").execute();
        System.out.println(System.currentTimeMillis() - start + " ms");

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

        start = System.currentTimeMillis();
        degrees2 = graph.graql().compute().degree().of().execute();
        System.out.println(System.currentTimeMillis() - start + " ms");

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
        graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        graph.putEntityType("person").playsRole(owner);
        EntityType animal = graph.putEntityType("animal").playsRole(pet);
        EntityType dog = graph.putEntityType("dog").superType(animal);
        ConceptId foofoo = dog.addEntity().getId();
        graph.commitOnClose();
        graph.close();

        // set subgraph
        HashSet<TypeName> ct = Sets.newHashSet(TypeName.of("person"), TypeName.of("animal"), TypeName.of("mans-best-friend"));
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
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        RoleType target = graph.putRoleType("target");
        RoleType value = graph.putRoleType("value");
        RelationType hasName = graph.putRelationType("has-name").hasRole(value).hasRole(target);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType animal = graph.putEntityType("animal").playsRole(pet).playsRole(target);
        ResourceType<String> name = graph.putResourceType("name", ResourceType.DataType.STRING).playsRole(value);
        ResourceType<String> altName =
                graph.putResourceType("alternate-name", ResourceType.DataType.STRING).playsRole(value);

        // add data to the graph
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Resource coconut = name.putResource("coconut");
        Resource stinky = altName.putResource("stinky");
        Relation daveOwnsCoco = mansBestFriend.addRelation().putRolePlayer(owner, dave).putRolePlayer(pet, coco);
        Relation cocoName = hasName.addRelation().putRolePlayer(target, coco).putRolePlayer(value, coconut);
        Relation cocoAltName = hasName.addRelation().putRolePlayer(target, coco).putRolePlayer(value, stinky);

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

        graph.commitOnClose();
        graph.close();

        // create a subgraph excluding resources and the relationship
        HashSet<TypeName> subGraphTypes = Sets.newHashSet(TypeName.of("animal"), TypeName.of("person"), TypeName.of("mans-best-friend"));
        Map<Long, Set<String>> degrees = graph.graql().compute().degree().in(subGraphTypes).execute();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(subGraphReferenceDegrees.containsKey(ConceptId.of(id)));
                    assertEquals(subGraphReferenceDegrees.get(ConceptId.of(id)), entry.getKey());
                }
        ));

        // create a subgraph excluding resource type only
        HashSet<TypeName> almostFullTypes = Sets.newHashSet(TypeName.of("animal"), TypeName.of("person"), TypeName.of("mans-best-friend"), TypeName.of("has-name"), TypeName.of("name"));
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
                .hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = graph.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = graph.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Relation daveBreedsAndOwnsCoco = mansBestFriend.addRelation()
                .putRolePlayer(pet, coco).putRolePlayer(owner, dave);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        graph.commitOnClose();
        graph.close();

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
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        RoleType target = graph.putRoleType("target");
        RoleType value = graph.putRoleType("value");
        RelationType hasName = graph.putRelationType("has-name").hasRole(value).hasRole(target);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType animal = graph.putEntityType("animal").playsRole(pet).playsRole(target);
        ResourceType<String> name = graph.putResourceType("name", ResourceType.DataType.STRING).playsRole(value);
        ResourceType<String> altName =
                graph.putResourceType("alternate-name", ResourceType.DataType.STRING).playsRole(value);
        RoleType ownership = graph.putRoleType("ownership");
        RoleType ownershipResource = graph.putRoleType("ownership-resource");
        RelationType hasOwnershipResource =
                graph.putRelationType("has-ownership-resource").hasRole(ownership).hasRole(ownershipResource);
        ResourceType<String> startDate =
                graph.putResourceType("start-date", ResourceType.DataType.STRING).playsRole(ownershipResource);
        mansBestFriend.playsRole(ownership);

        // add data to the graph
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();
        Resource coconut = name.putResource("coconut");
        Resource stinky = altName.putResource("stinky");
        Relation daveOwnsCoco = mansBestFriend.addRelation()
                .putRolePlayer(owner, dave).putRolePlayer(pet, coco);
        hasName.addRelation().putRolePlayer(target, coco).putRolePlayer(value, coconut);
        hasName.addRelation().putRolePlayer(target, coco).putRolePlayer(value, stinky);
        Resource sd = startDate.putResource("01/01/01");
        Relation ownsFrom = hasOwnershipResource.addRelation()
                .putRolePlayer(ownershipResource, sd).putRolePlayer(ownership, daveOwnsCoco);

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

        graph.commitOnClose();
        graph.close();

        // create a subgraph with assertion on assertion
        HashSet<TypeName> ct =
                Sets.newHashSet(TypeName.of("animal"), TypeName.of("person"), TypeName.of("mans-best-friend"), TypeName.of("start-date"), TypeName.of("has-ownership-resource"));
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
        ct.add(TypeName.of("animal"));
        ct.add(TypeName.of("person"));
        ct.add(TypeName.of("mans-best-friend"));
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
                .hasRole(productionWithCast)
                .hasRole(actor)
                .hasRole(characterBeingPlayed);

        EntityType movie = graph.putEntityType("movie").playsRole(productionWithCast);
        EntityType person = graph.putEntityType("person").playsRole(actor);
        EntityType character = graph.putEntityType("character").playsRole(characterBeingPlayed);

        Entity godfather = movie.addEntity();
        Entity marlonBrando = person.addEntity();
        ConceptId marlonId = marlonBrando.getId();
        Entity donVitoCorleone = character.addEntity();

        Relation relation = hasCast.addRelation()
                .putRolePlayer(productionWithCast, godfather)
                .putRolePlayer(actor, marlonBrando)
                .putRolePlayer(characterBeingPlayed, donVitoCorleone);
        ConceptId relationId = relation.getId();

        graph.commitOnClose();
        graph.close();

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
                .hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = graph.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = graph.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = animal.addEntity();
        Entity dave = person.addEntity();

        Relation daveBreedsAndOwnsCoco = mansBestFriend.addRelation()
                .putRolePlayer(pet, coco)
                .putRolePlayer(owner, dave)
                .putRolePlayer(breeder, dave);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 2L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);

        graph.commitOnClose();
        graph.close();

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
                .hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = graph.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType dog = graph.putEntityType("dog").playsRole(pet);
        EntityType cat = graph.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity beast = dog.addEntity();
        Entity coco = cat.addEntity();
        Entity dave = person.addEntity();
        Relation daveBreedsAndOwnsCoco = mansBestFriend.addRelation()
                .putRolePlayer(owner, dave).putRolePlayer(breeder, dave).putRolePlayer(pet, coco);
        Relation daveBreedsAndOwnsBeast = mansBestFriend.addRelation()
                .putRolePlayer(owner, dave).putRolePlayer(breeder, dave).putRolePlayer(pet, beast);

        // manual degrees
        Map<ConceptId, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 4L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);
        referenceDegrees.put(daveBreedsAndOwnsBeast.getId(), 2L);

        // validate
        graph.commitOnClose();
        graph.close();

        // check degree for dave owning cats
        //TODO: should we count the relationship even if there is no cat attached?
        HashSet<TypeName> ct = Sets.newHashSet(TypeName.of("mans-best-friend"), TypeName.of("cat"), TypeName.of("person"));
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