/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.test.graql.analytics;

import com.google.common.collect.Sets;
import io.grakn.Mindmaps;
import io.grakn.MindmapsGraph;
import io.grakn.concept.*;
import io.grakn.exception.MindmapsValidationException;
import io.grakn.graph.internal.AbstractMindmapsGraph;
import io.grakn.graql.internal.analytics.Analytics;
import io.grakn.graql.internal.util.GraqlType;
import io.grakn.test.AbstractGraphTest;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
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
        String foofoo = graph.addEntity(dog).getId();
        graph.commit();

        // set subgraph
        HashSet<String> ct = Sets.newHashSet("person", "animal", "mans-best-friend");
        Analytics analytics = new Analytics(graph.getKeyspace(), ct, new HashSet<>());
        analytics.degreesAndPersist();

        // check that dog has a degree to confirm sub has been inferred
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, graph.getKeyspace()).getGraph();
        Collection<Resource<?>> degrees = graph.getEntity(foofoo).resources();
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
        graph.addEntity(thing).getId();
        graph.addEntity(thing).getId();
        graph.addEntity(anotherThing).getId();
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

        String entity1 = graph.addEntity(thing).getId();
        String entity2 = graph.addEntity(thing).getId();
        String entity3 = graph.addEntity(thing).getId();
        String entity4 = graph.addEntity(anotherThing).getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        thing.playsRole(role1).playsRole(role2);
        anotherThing.playsRole(role1).playsRole(role2);
        RelationType related = graph.putRelationType("related").hasRole(role1).hasRole(role2);

        // relate them
        String id1 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity1))
                .putRolePlayer(role2, graph.getInstance(entity2))
                .getId();

        String id2 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity2))
                .putRolePlayer(role2, graph.getInstance(entity3))
                .getId();

        String id3 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity2))
                .putRolePlayer(role2, graph.getInstance(entity4))
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

    private static void checkDegrees(MindmapsGraph graph, Map<String, Long> correctDegrees) {
        correctDegrees.entrySet().forEach(entry -> {
            Instance instance = graph.getInstance(entry.getKey());
            // TODO: when shortcut edges are removed properly during concurrent deletion revert code
            Collection<Resource<?>> resources = null;
            if (instance.isEntity()) {
                resources = instance.asEntity().resources();
            } else if (instance.isRelation()) {
                resources = instance.asRelation().resources();
            }
            assert resources != null;
            assertEquals(1, resources.size());
            assertEquals(entry.getValue(),resources.iterator().next().getValue());
        });
    }

    @Test
    public void testDegreesAndPersist() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");

        String entity1 = graph.addEntity(thing).getId();
        String entity2 = graph.addEntity(thing).getId();
        String entity3 = graph.addEntity(thing).getId();
        String entity4 = graph.addEntity(anotherThing).getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        thing.playsRole(role1).playsRole(role2);
        anotherThing.playsRole(role1).playsRole(role2);
        RelationType related = graph.putRelationType("related").hasRole(role1).hasRole(role2);

        // relate them
        String id1 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity1))
                .putRolePlayer(role2, graph.getInstance(entity2))
                .getId();

        String id2 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity2))
                .putRolePlayer(role2, graph.getInstance(entity3))
                .getId();

        String id3 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity2))
                .putRolePlayer(role2, graph.getInstance(entity4))
                .getId();

        graph.commit();

        // compute degrees on subgraph
        Analytics computer = new Analytics(graph.getKeyspace(), Sets.newHashSet("thing", "related"), new HashSet<>());
        computer.degreesAndPersist();

        Map<String, Long> correctDegrees = new HashMap<>();
        correctDegrees.clear();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(id1, 2l);
        correctDegrees.put(id2, 2l);
        correctDegrees.put(id3, 1l);

        // assert persisted degrees are correct
        checkDegrees(graph, correctDegrees);

        // compute again and again ...
        long numVertices = 0;
        for (int i = 0; i < 2; i++) {
            computer.degreesAndPersist();
            graph = factory.getGraph();
            checkDegrees(graph, correctDegrees);

            // assert the number of vertices remain the same
            if (i == 0) {
                numVertices = computer.count();
            } else {
                assertEquals(numVertices, computer.count());
            }
        }

        //TODO: Get rid of this close. We should be refreshing the graph in the factory when switching between normal and batch
        ((AbstractMindmapsGraph) graph).getTinkerPopGraph().close();
        computer = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());

        // compute degrees on all types, again and again ...
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(id3, 2l);
        for (int i = 0; i < 2; i++) {

            computer.degreesAndPersist();
            graph = factory.getGraph();
            checkDegrees(graph, correctDegrees);

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
    public void testDegreeIsCorrect() throws MindmapsValidationException, ExecutionException, InterruptedException {
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
        Entity coco = graph.addEntity(animal);
        Entity dave = graph.addEntity(person);
        Resource coconut = graph.putResource("coconut", name);
        Resource stinky = graph.putResource("stinky", altName);
        Relation daveOwnsCoco = graph.addRelation(mansBestFriend).putRolePlayer(owner, dave).putRolePlayer(pet, coco);
        Relation cocoName = graph.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, coconut);
        Relation cocoAltName = graph.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, stinky);

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
        Entity coco = graph.addEntity(animal);
        Entity dave = graph.addEntity(person);
        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
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
        MindmapsGraph finalGraph = graph;
        referenceDegrees.entrySet().forEach(entry -> {
            Instance instance = finalGraph.getInstance(entry.getKey());
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
        MindmapsGraph finalGraph1 = graph;
        referenceDegrees.entrySet().forEach(entry -> {
            Instance instance = finalGraph1.getInstance(entry.getKey());
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
            throws MindmapsValidationException, ExecutionException, InterruptedException {
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
        Entity coco = graph.addEntity(animal);
        Entity dave = graph.addEntity(person);
        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
                .putRolePlayer(pet, coco).putRolePlayer(owner, dave);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        // create a decoy resource using same relationship
        RoleType degreeOwner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(Analytics.degree));
        RoleType degreeValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(Analytics.degree));
        RelationType hasResource = graph.putRelationType(GraqlType.HAS_RESOURCE.getId(Analytics.degree))
                .hasRole(degreeOwner).hasRole(degreeValue);
        ResourceType<Long> decoyResourceType =
                graph.putResourceType("decoy-resource", ResourceType.DataType.LONG).playsRole(degreeValue);
        Resource<Long> decoyResource = graph.putResource(100L, decoyResourceType);
        graph.addRelation(hasResource).putRolePlayer(degreeOwner, coco).putRolePlayer(degreeValue, decoyResource);
        animal.playsRole(degreeOwner);

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
            Instance instance = graph.getInstance(entry.getKey());
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
            throws MindmapsValidationException, ExecutionException, InterruptedException {
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
        Entity coco = graph.addEntity(animal);
        Entity dave = graph.addEntity(person);
        Resource coconut = graph.putResource("coconut", name);
        Resource stinky = graph.putResource("stinky", altName);
        Relation daveOwnsCoco = graph.addRelation(mansBestFriend)
                .putRolePlayer(owner, dave).putRolePlayer(pet, coco);
        graph.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, coconut);
        graph.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, stinky);
        Resource sd = graph.putResource("01/01/01", startDate);
        Relation ownsFrom = graph.addRelation(hasOwnershipResource)
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
            throws MindmapsValidationException, ExecutionException, InterruptedException {
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

        Entity godfather = graph.addEntity(movie);
        Entity marlonBrando = graph.addEntity(person);
        String marlonId = marlonBrando.getId();
        Entity donVitoCorleone = graph.addEntity(character);

        Relation relation = graph.addRelation(hasCast)
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
            throws MindmapsValidationException, ExecutionException, InterruptedException {
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
        Entity coco = graph.addEntity(animal);
        Entity dave = graph.addEntity(person);

        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
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
            throws MindmapsValidationException, ExecutionException, InterruptedException {
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
        Entity coco = graph.addEntity(animal);
        Entity dave = graph.addEntity(person);
        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
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
            throws MindmapsValidationException, ExecutionException, InterruptedException {
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
        Entity beast = graph.addEntity(dog);
        Entity coco = graph.addEntity(cat);
        Entity dave = graph.addEntity(person);
        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
                .putRolePlayer(owner, dave).putRolePlayer(breeder, dave).putRolePlayer(pet, coco);
        Relation daveBreedsAndOwnsBeast = graph.addRelation(mansBestFriend)
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
            throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType cat = graph.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity coco = graph.addEntity(cat);
        Entity dave = graph.addEntity(person);
        graph.addRelation(mansBestFriend)
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
    public void testComputingUsingDegreeResource() throws MindmapsValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create something with degrees
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType cat = graph.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity coco = graph.addEntity(cat);
        Entity dave = graph.addEntity(person);
        graph.addRelation(mansBestFriend)
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
        assertEquals(graph.getResourceType("degree").instances().size(), analytics.count());
    }

    @Test
    public void testNullResourceDoesntBreakAnalytics() throws MindmapsValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // make slightly odd graph
        String resourceTypeId = "degree";
        EntityType thing = graph.putEntityType("thing");

        graph.putResourceType(resourceTypeId, ResourceType.DataType.LONG);
        RoleType degreeOwner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        RoleType degreeValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        RelationType relationType = graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);
        thing.playsRole(degreeOwner);

        Entity thisThing = graph.addEntity(thing);
        graph.addRelation(relationType).putRolePlayer(degreeOwner, thisThing);
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
}