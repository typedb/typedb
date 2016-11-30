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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class AnalyticsTest extends AbstractGraphTest {

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());
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
        String foofoo = dog.addEntity().getId();
        graph.commit();

        // set subgraph
        HashSet<String> ct = Sets.newHashSet("person", "animal", "mans-best-friend");
        Analytics analytics = new Analytics(graph.getKeyspace(), ct, new HashSet<>());
        analytics.degreesAndPersist();

        // check that dog has a degree to confirm sub has been inferred
        graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();
        Collection<Resource<?>> degrees = graph.getConcept(foofoo).asEntity().resources();
        assertTrue(degrees.iterator().next().getValue().equals(0L));
    }

    @Test
    public void testCount() throws Exception {
        // assert the graph is empty
        System.out.println();
        System.out.println("Counting");
        Analytics computer = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        long startTime = System.currentTimeMillis();
        Assert.assertEquals(0, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        // create 3 instances
        System.out.println();
        System.out.println("Creating 3 instances");
        graph = factory.getGraph();
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");
        thing.addEntity().getId();
        thing.addEntity().getId();
        anotherThing.addEntity().getId();
        graph.commit();
        graph.close();

        // assert computer returns the correct count of instances
        System.out.println();
        System.out.println("Counting");
        startTime = System.currentTimeMillis();
        computer = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        Assert.assertEquals(3, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        System.out.println();
        System.out.println("Counting");
        startTime = System.currentTimeMillis();
        graph = factory.getGraph();
        computer = new Analytics(graph.getKeyspace(), Collections.singleton("thing"), new HashSet<>());
        Assert.assertEquals(2, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");
    }

    @Test
    public void testDegrees() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");

        String entity1 = thing.addEntity().getId();
        String entity2 = thing.addEntity().getId();
        String entity3 = thing.addEntity().getId();
        String entity4 = anotherThing.addEntity().getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        thing.playsRole(role1).playsRole(role2);
        anotherThing.playsRole(role1).playsRole(role2);
        RelationType related = graph.putRelationType("related").hasRole(role1).hasRole(role2);

        // relate them
        String id1 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity1))
                .putRolePlayer(role2, graph.getConcept(entity2))
                .getId();

        String id2 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity2))
                .putRolePlayer(role2, graph.getConcept(entity3))
                .getId();

        String id3 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity2))
                .putRolePlayer(role2, graph.getConcept(entity4))
                .getId();

        graph.commit();
        graph.close();

        Map<String, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(id1, 2l);
        correctDegrees.put(id2, 2l);
        correctDegrees.put(id3, 2l);

        // compute degrees
        Analytics computer = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        Map<Long, Set<String>> degrees = computer.degrees();
        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id), entry.getKey());
                }
        ));

        // compute degrees again after persisting degrees
        computer.degreesAndPersist();
        Map<Long, Set<String>> degrees2 = computer.degrees();
        assertEquals(degrees.size(), degrees2.size());
        degrees2.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id), entry.getKey());
                }
        ));

        // compute degrees on subgraph
        computer = new Analytics(graph.getKeyspace(), Sets.newHashSet("thing", "related"), new HashSet<>());
        Map<Long, Set<String>> degrees3 = computer.degrees();

        correctDegrees.put(id3, 1l);

        assertTrue(!degrees3.isEmpty());
        degrees3.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id), entry.getKey());
                }
        ));
    }

    private void checkDegrees(Map<String, Long> correctDegrees) {
        correctDegrees.entrySet().forEach(entry -> {
            Collection<Resource<?>> resources =
                    graph.<Instance>getConcept(entry.getKey()).resources(graph.getResourceType(Analytics.degree));
            assertEquals(1, resources.size());
            assertEquals(entry.getValue(), resources.iterator().next().getValue());
        });
    }

    @Ignore //TODO: Stabalise this test. It fails way too often.
    @Test
    public void testDegreesAndPersist() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");

        String entity1 = thing.addEntity().getId();
        String entity2 = thing.addEntity().getId();
        String entity3 = thing.addEntity().getId();
        String entity4 = anotherThing.addEntity().getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        thing.playsRole(role1).playsRole(role2);
        anotherThing.playsRole(role1).playsRole(role2);
        RelationType related = graph.putRelationType("related").hasRole(role1).hasRole(role2);

        // relate them
        String id1 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity1))
                .putRolePlayer(role2, graph.getConcept(entity2))
                .getId();

        String id2 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity2))
                .putRolePlayer(role2, graph.getConcept(entity3))
                .getId();

        String id3 = related.addRelation()
                .putRolePlayer(role1, graph.getConcept(entity2))
                .putRolePlayer(role2, graph.getConcept(entity4))
                .getId();

        graph.commit();

        // compute degrees on subgraph
        Analytics computer = new Analytics(graph.getKeyspace(), Sets.newHashSet("thing", "related"), new HashSet<>());
        computer.degreesAndPersist();

        Map<String, Long> correctDegrees = new HashMap<>();
        correctDegrees.clear();
        correctDegrees.put(entity1, 1L);
        correctDegrees.put(entity2, 3L);
        correctDegrees.put(entity3, 1L);
        correctDegrees.put(id1, 2L);
        correctDegrees.put(id2, 2L);
        correctDegrees.put(id3, 1L);

        // assert persisted degrees are correct
        checkDegrees(correctDegrees);

        // compute again and again ...
        long numVertices = 0;
        for (int i = 0; i < 2; i++) {
            computer.degreesAndPersist();
            graph = factory.getGraph();
            checkDegrees(correctDegrees);

            // assert the number of vertices remain the same
            if (i == 0) {
                numVertices = computer.count();
            } else {
                assertEquals(numVertices, computer.count());
            }
        }

        computer = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());

        // compute degrees on all types, again and again ...
        correctDegrees.put(entity4, 1L);
        correctDegrees.put(id3, 2L);
        for (int i = 0; i < 2; i++) {

            computer.degreesAndPersist();
            graph = factory.getGraph();
            checkDegrees(correctDegrees);

            // assert the number of vertices remain the same
            if (i == 0) {
                assertEquals(1, computer.count() - numVertices);
                numVertices = computer.count();
            } else {
                assertEquals(numVertices, computer.count());
            }
        }
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
        Map<String, Long> subGraphReferenceDegrees = new HashMap<>();
        subGraphReferenceDegrees.put(coco.getId(), 1L);
        subGraphReferenceDegrees.put(dave.getId(), 1L);
        subGraphReferenceDegrees.put(daveOwnsCoco.getId(), 2L);

        // manually compute degree for almost full graph
        Map<String, Long> almostFullReferenceDegrees = new HashMap<>();
        almostFullReferenceDegrees.put(coco.getId(), 3L);
        almostFullReferenceDegrees.put(dave.getId(), 1L);
        almostFullReferenceDegrees.put(daveOwnsCoco.getId(), 2L);
        almostFullReferenceDegrees.put(cocoName.getId(), 2L);
        almostFullReferenceDegrees.put(cocoAltName.getId(), 1L);
        almostFullReferenceDegrees.put(coconut.getId(), 1L);

        // manually compute degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 3L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(coconut.getId(), 1L);
        referenceDegrees.put(stinky.getId(), 1L);
        referenceDegrees.put(daveOwnsCoco.getId(), 2L);
        referenceDegrees.put(cocoName.getId(), 2L);
        referenceDegrees.put(cocoAltName.getId(), 2L);

        graph.commit();

        // create a subgraph excluding resources and the relationship
        HashSet<String> subGraphTypes = Sets.newHashSet("animal", "person", "mans-best-friend");
        Analytics computer = new Analytics(graph.getKeyspace(), subGraphTypes, new HashSet<>());
        Map<Long, Set<String>> degrees = computer.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(subGraphReferenceDegrees.containsKey(id));
                    assertEquals(subGraphReferenceDegrees.get(id), entry.getKey());
                }
        ));

        // create a subgraph excluding resource type only
        HashSet<String> almostFullTypes = Sets.newHashSet("animal", "person", "mans-best-friend", "has-name", "name");
        computer = new Analytics(graph.getKeyspace(), almostFullTypes, new HashSet<>());
        degrees = computer.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(almostFullReferenceDegrees.containsKey(id));
                    assertEquals(almostFullReferenceDegrees.get(id), entry.getKey());
                }
        ));

        // full graph
        computer = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        degrees = computer.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees.containsKey(id));
                    assertEquals(referenceDegrees.get(id), entry.getKey());
                }
        ));
    }

    @Test
    public void testDegreeIsPersisted() throws Exception {
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
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        // validate
        graph.commit();

        // compute and persist degrees
        Analytics analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        analytics.degreesAndPersist();

        // check degrees are correct
        graph = factory.getGraph();
        GraknGraph finalGraph = graph;
        referenceDegrees.entrySet().forEach(entry -> {
            Instance instance = finalGraph.getConcept(entry.getKey());
            if (instance.isEntity()) {
                assertTrue(instance.asEntity().resources().iterator().next().getValue().equals(entry.getValue()));
            } else if (instance.isRelation()) {
                assertTrue(instance.asRelation().resources().iterator().next().getValue().equals(entry.getValue()));
            }
        });

        // check only expected resources exist
        Collection<String> allConcepts = new ArrayList<>();
        ResourceType<Long> rt = graph.getResourceType(Analytics.degree);
        Collection<Resource<Long>> degrees = rt.instances();
        Map<Instance, Long> currentDegrees = new HashMap<>();
        degrees.forEach(degree -> {
            Long degreeValue = degree.getValue();
            degree.ownerInstances().forEach(instance -> currentDegrees.put(instance, degreeValue));
        });

        // check all resources exist and no more
        assertTrue(CollectionUtils.isEqualCollection(currentDegrees.values(), referenceDegrees.values()));

        // persist again and check again
        analytics.degreesAndPersist();

        // check only expected resources exist
        graph = factory.getGraph();
        rt = graph.getResourceType(Analytics.degree);
        degrees = rt.instances();
        degrees.forEach(i -> i.ownerInstances().iterator().forEachRemaining(r ->
                allConcepts.add(r.getId())));

        // check degrees are correct
        GraknGraph finalGraph1 = graph;
        referenceDegrees.entrySet().forEach(entry -> {
            Instance instance = finalGraph1.getConcept(entry.getKey());
            if (instance.isEntity()) {
                assertTrue(instance.asEntity().resources().iterator().next().getValue().equals(entry.getValue()));
            } else if (instance.isRelation()) {
                assertTrue(instance.asRelation().resources().iterator().next().getValue().equals(entry.getValue()));
            }
        });

        degrees = rt.instances();
        currentDegrees.clear();
        degrees.forEach(degree -> {
            Long degreeValue = degree.getValue();
            degree.ownerInstances().forEach(instance -> currentDegrees.put(instance, degreeValue));
        });

        // check all resources exist and no more
        assertTrue(CollectionUtils.isEqualCollection(currentDegrees.values(), referenceDegrees.values()));
    }

    @Test
    public void testDegreeIsPersistedInPresenceOfOtherResource()
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
                .putRolePlayer(pet, coco).putRolePlayer(owner, dave);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        // create a decoy resource using same relationship
        ResourceType<Long> decoyResourceType = graph.putResourceType("decoy-resource", ResourceType.DataType.LONG);
        Resource<Long> decoyResource = decoyResourceType.putResource(100L);

        animal.hasResource(decoyResourceType);
        coco.hasResource(decoyResource);

        // validate
        graph.commit();

        HashSet<String> ct = Sets.newHashSet("person", "animal", "mans-best-friend");

        // compute and persist degrees
        Analytics analytics = new Analytics(graph.getKeyspace(), ct, new HashSet<>());
        analytics.degreesAndPersist();

        graph = factory.getGraph();
        ResourceType<Long> degreeResource = graph.getResourceType(Analytics.degree);

        // check degrees are correct
        boolean isSeen = false;
        for (Map.Entry<String, Long> entry : referenceDegrees.entrySet()) {
            Instance instance = graph.getConcept(entry.getKey());
            if (instance.isEntity()) {
                for (Resource<?> resource : instance.asEntity().resources()) {
                    if (resource.type().equals(degreeResource)) {
                        // check value is correct
                        assertTrue(resource.getValue().equals(entry.getValue()));
                        // ensure a resource of this type is seen
                        isSeen = true;
                    }
                }
            } else if (instance.isRelation()) {
                for (Resource<?> resource : instance.asRelation().resources()) {
                    if (resource.type().equals(degreeResource)) {
                        // check value
                        assertTrue(resource.getValue().equals(entry.getValue()));
                        // ensure exists
                        isSeen = true;
                    }
                }
            }
            // fails if a resource is not found for everything in the referenceDegree map
            assertTrue(isSeen);
            isSeen = false;
        }
    }

    @Test
    public void testDegreeIsCorrectAssertionAboutAssertion()
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
        Map<String, Long> referenceDegrees1 = new HashMap<>();
        referenceDegrees1.put(coco.getId(), 1L);
        referenceDegrees1.put(dave.getId(), 1L);
        referenceDegrees1.put(daveOwnsCoco.getId(), 3L);
        referenceDegrees1.put(sd.getId(), 1L);
        referenceDegrees1.put(ownsFrom.getId(), 2L);

        // manually compute degrees
        Map<String, Long> referenceDegrees2 = new HashMap<>();
        referenceDegrees2.put(coco.getId(), 1L);
        referenceDegrees2.put(dave.getId(), 1L);
        referenceDegrees2.put(daveOwnsCoco.getId(), 2L);

        graph.commit();

        // create a subgraph with assertion on assertion
        HashSet<String> ct =
                Sets.newHashSet("animal", "person", "mans-best-friend", "start-date", "has-ownership-resource");
        Analytics computer = new Analytics(graph.getKeyspace(), ct, new HashSet<>());
        Map<Long, Set<String>> degrees = computer.degrees();
        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees1.containsKey(id));
                    assertEquals(referenceDegrees1.get(id), entry.getKey());
                }
        ));

        // create subgraph without assertion on assertion
        ct.clear();
        ct.add("animal");
        ct.add("person");
        ct.add("mans-best-friend");
        computer = new Analytics(graph.getKeyspace(), ct, new HashSet<>());
        degrees = computer.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees2.containsKey(id));
                    assertEquals(referenceDegrees2.get(id), entry.getKey());
                }
        ));
    }

    @Test
    public void testDegreeIsCorrectTernaryRelationships()
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
        String marlonId = marlonBrando.getId();
        Entity donVitoCorleone = character.addEntity();

        Relation relation = hasCast.addRelation()
                .putRolePlayer(productionWithCast, godfather)
                .putRolePlayer(actor, marlonBrando)
                .putRolePlayer(characterBeingPlayed, donVitoCorleone);
        String relationId = relation.getId();

        graph.commit();

        Analytics computer = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        Map<Long, Set<String>> degrees = computer.degrees();
        graph = factory.getGraph();
        assertTrue(degrees.get(3L).contains(relationId));
        assertTrue(degrees.get(1L).contains(marlonId));
    }

    @Test
    public void testDegreeIsCorrectOneRoleplayerMultipleRoles()
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
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 2L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);

        // validate
        graph.commit();

        Analytics computer = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        Map<Long, Set<String>> degrees = computer.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees.containsKey(id));
                    assertEquals(referenceDegrees.get(id), entry.getKey());
                }
        ));
    }

    @Test
    public void testDegreeIsCorrectMissingRoleplayer()
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
                .putRolePlayer(pet, coco).putRolePlayer(owner, dave);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        // validate
        graph.commit();

        Analytics analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        Map<Long, Set<String>> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees.containsKey(id));
                    assertEquals(referenceDegrees.get(id), entry.getKey());
                }
        ));
    }

    @Test
    public void testDegreeIsCorrectRoleplayerWrongType()
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
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 4L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);
        referenceDegrees.put(daveBreedsAndOwnsBeast.getId(), 2L);

        // validate
        graph.commit();

        // check degree for dave owning cats
        //TODO: should we count the relationship even if there is no cat attached?
        HashSet<String> ct = Sets.newHashSet("mans-best-friend", "cat", "person");
        Analytics analytics = new Analytics(graph.getKeyspace(), ct, new HashSet<>());
        Map<Long, Set<String>> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(referenceDegrees.containsKey(id));
                    assertEquals(referenceDegrees.get(id), entry.getKey());
                }
        ));
    }

    @Test
    public void testMultipleExecutionOfDegreeAndPersistWhileAddingNodes()
            throws GraknValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType cat = graph.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity coco = cat.addEntity();
        Entity dave = person.addEntity();
        mansBestFriend.addRelation()
                .putRolePlayer(owner, dave).putRolePlayer(pet, coco);

        // validate
        graph.commit();

        // count and persist
        Analytics analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();

        analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();

        analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();
    }

    @Test
    public void testComputingUsingDegreeResource() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create something with degrees
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType cat = graph.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity coco = cat.addEntity();
        Entity dave = person.addEntity();
        mansBestFriend.addRelation()
                .putRolePlayer(owner, dave).putRolePlayer(pet, coco);

        // validate
        graph.commit();

        // create degrees
        new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>()).degreesAndPersist();

        // compute sum
        Analytics analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), Collections.singleton("degree"));
        assertEquals(4L, analytics.sum().get());

        // compute count
        analytics = new Analytics(graph.getKeyspace(), Collections.singleton("degree"), new HashSet<>());

        // need to reopen the graph because we have made changes on the batch graph
        graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();
        Assert.assertEquals(graph.getResourceType("degree").instances().size(), analytics.count());
    }

    @Test
    public void testNullResourceDoesntBreakAnalytics() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // make slightly odd graph
        String resourceTypeId = "degree";
        EntityType thing = graph.putEntityType("thing");

        graph.putResourceType(resourceTypeId, ResourceType.DataType.LONG);
        RoleType degreeOwner = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeId));
        RoleType degreeValue = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeId));
        RelationType relationType = graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);
        thing.playsRole(degreeOwner);

        Entity thisThing = thing.addEntity();
        relationType.addRelation().putRolePlayer(degreeOwner, thisThing);
        graph.commit();

        Analytics analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());

        // the null role-player caused analytics to fail at some stage
        try {
            analytics.degreesAndPersist();
        } catch (RuntimeException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Ignore //TODO: Fix remotely. Failing on Jenkins only
    @Test
    public void testResourcesMergedOnBulkMutate() throws GraknValidationException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());
        Cache cache = Cache.getInstance();

        //Clear Cache
        cache.getKeyspaces().forEach(keyspace -> {
            cache.getResourceJobs(keyspace).clear();
            cache.getCastingJobs(keyspace).clear();
        });

        RoleType friend1 = graph.putRoleType("friend1");
        RoleType friend2 = graph.putRoleType("friend2");
        RelationType friendship = graph.putRelationType("friendship");
        friendship.hasRole(friend1).hasRole(friend2);

        EntityType person = graph.putEntityType("person");
        person.playsRole(friend1).playsRole(friend2);

        for (int i = 0; i < 10; i++) {
            friendship.addRelation()
                    .putRolePlayer(friend1, person.addEntity())
                    .putRolePlayer(friend2, person.addEntity());
        }

        graph.commit();
        String keyspace = graph.getKeyspace();

        new Analytics(keyspace, new HashSet<>(), new HashSet<>()).degreesAndPersist();

        Collection<Resource<Object>> degrees = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph()
                .getResourceType("degree").instances();

        assertTrue(degrees.size() > 1);

        //Wait for cache to be updated
        int failCount = 0;
        while(cache.getResourceJobs(keyspace).size() < 4){
            Thread.sleep(1000);
            failCount ++;
            assertFalse("Failed to update cache with resources to merge", failCount < 10);
        }

        //Force Post Processing
        PostProcessing postProcessing = PostProcessing.getInstance();
        postProcessing.run();

        //Check all is good
        graph.close();
        degrees = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph()
                .getResourceType("degree").instances();
        assertEquals(2, degrees.size());
    }
}