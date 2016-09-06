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

package io.mindmaps.graql.internal.analytics;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.graql.internal.GraqlType;
import org.apache.commons.collections.CollectionUtils;
import org.javatuples.Pair;
import org.junit.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;
import static org.junit.Assert.*;

public class AnalyticsTest {

    String keyspace = "mindmapstest";
    MindmapsGraph graph;
    MindmapsTransaction transaction;

    // concepts
    EntityType thing;
    EntityType anotherThing;
    Entity entity1;
    Entity entity2;
    Entity entity3;
    Entity entity4;
    RoleType relation1;
    RoleType relation2;
    RelationType related;

    long startTime;

    @BeforeClass
    public static void startController() throws Exception {
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException {
        Pair<MindmapsGraph, String> result = graphWithNewKeyspace();
        graph = result.getValue0();
        keyspace = result.getValue1();
        transaction = graph.getTransaction();
    }

    @After
    public void cleanGraph() {
        graph.clear();
    }

    @Test
    public void testAkoIsAccountedForInSubgraph() throws Exception {
        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = transaction.putEntityType("person").playsRole(owner);
        EntityType animal = transaction.putEntityType("animal").playsRole(pet);
        EntityType dog = transaction.putEntityType("dog").superType(animal);
        Entity foofoo = transaction.putEntity("foofoo", dog);
        transaction.commit();

        // fetch types
        mansBestFriend = transaction.getRelationType("mans-best-friend");
        person = transaction.getEntityType("person");
        animal = transaction.getEntityType("animal");

        Set<Type> ct = new HashSet<>();
        ct.add(person);
        ct.add(animal);
        ct.add(mansBestFriend);

        Analytics analytics = new Analytics(keyspace,ct);
        analytics.degreesAndPersist();

        // check that dog has a degree to confirm ako has been inferred
        transaction.refresh();
        foofoo = transaction.getEntity("foofoo");
        Collection<Resource<?>> degrees = foofoo.resources();
        assertTrue(degrees.iterator().next().getValue().equals(0L));
    }

    @Test
    public void testCount() throws Exception {
        // assert the graph is empty
        System.out.println();
        System.out.println("Counting");
        Analytics computer = new Analytics(keyspace);
        startTime = System.currentTimeMillis();
        Assert.assertEquals(0, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        // create 3 instances
        System.out.println();
        System.out.println("Creating 3 instances");
        thing = transaction.putEntityType("thing");
        anotherThing = transaction.putEntityType("another");
        transaction.putEntity("1", thing);
        transaction.putEntity("2", thing);
        transaction.putEntity("3", anotherThing);
        transaction.commit();

        // assert computer returns the correct count of instances
        System.out.println();
        System.out.println("Counting");
        startTime = System.currentTimeMillis();
        computer = new Analytics(keyspace);
        Assert.assertEquals(3, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        System.out.println();
        System.out.println("Counting");
        startTime = System.currentTimeMillis();
        computer = new Analytics(keyspace,Collections.singleton(transaction.getType("thing")));
        Assert.assertEquals(2, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");
    }

    @Test
    public void testDegrees() throws Exception {
        instantiateSimpleConcepts();

        // relate them
        String id1 = UUID.randomUUID().toString();
        transaction.putRelation(id1, related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        transaction.putRelation(id2, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        transaction.putRelation(id3, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        transaction.commit();

        // assert degrees are correct
        instantiateSimpleConcepts();
        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(transaction.getRelation(id1), 2l);
        correctDegrees.put(transaction.getRelation(id2), 2l);
        correctDegrees.put(transaction.getRelation(id3), 2l);

        // compute degrees
        Analytics computer = new Analytics(keyspace);
        Map<Instance, Long> degrees = computer.degrees();

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertEquals(correctDegrees.get(degree.getKey()), degree.getValue());
        });

        // compute degrees again after persisting degrees
        computer.degreesAndPersist();
        Map<Instance, Long> degrees2 = computer.degrees();

        assertEquals(degrees.size(), degrees2.size());

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertTrue(correctDegrees.get(degree.getKey()).equals(degree.getValue()));
        });

        // compute degrees on subgraph
        computer = new Analytics(keyspace,Sets.newHashSet(thing, related));
        degrees = computer.degrees();

        correctDegrees.put(transaction.getRelation(id3), 1l);

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertEquals(correctDegrees.get(degree.getKey()), degree.getValue());
        });
    }

    private void instantiateSimpleConcepts() {
        // create instances
        thing = transaction.putEntityType("thing");
        anotherThing = transaction.putEntityType("another");

        entity1 = transaction.putEntity("1", thing);
        entity2 = transaction.putEntity("2", thing);
        entity3 = transaction.putEntity("3", thing);
        entity4 = transaction.putEntity("4", anotherThing);

        relation1 = transaction.putRoleType("relation1");
        relation2 = transaction.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        anotherThing.playsRole(relation1).playsRole(relation2);
        related = transaction.putRelationType("related").hasRole(relation1).hasRole(relation2);
    }

    @Test
    public void testDegreesAndPersist() throws Exception {
        instantiateSimpleConcepts();

        // relate them
        String id1 = UUID.randomUUID().toString();
        transaction.putRelation(id1, related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        transaction.putRelation(id2, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        transaction.putRelation(id3, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        transaction.commit();

        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(transaction.getRelation(id1), 2l);
        correctDegrees.put(transaction.getRelation(id2), 2l);
        correctDegrees.put(transaction.getRelation(id3), 1l);

        // compute degrees on subgraph
        Analytics computer = new Analytics(keyspace,Sets.newHashSet(thing, related));
        computer.degreesAndPersist();

        // assert persisted degrees are correct
        instantiateSimpleConcepts();
        correctDegrees.entrySet().forEach(degree -> {
            Instance instance = degree.getKey();
            Collection<Resource<?>> resources = null;
            if (instance.isEntity()) {
                resources = instance.asEntity().resources();
            } else if (instance.isRelation()) {
                resources = instance.asRelation().resources();
            }
            assert resources != null;
            assertTrue(resources.iterator().next().getValue().equals(degree.getValue()));
        });

        long numVertices = 0;

        // compute again and again ...
        for (int i = 0; i < 2; i++) {
            System.out.println();
            System.out.println("i = " + i);
            computer.degreesAndPersist();

            correctDegrees.entrySet().forEach(degree -> {
                Instance instance = degree.getKey();
                Collection<Resource<?>> resources = null;
                if (instance.isEntity()) {
                    resources = instance.asEntity().resources();
                } else if (instance.isRelation()) {
                    resources = instance.asRelation().resources();
                }
                assert resources != null;
                assertTrue(resources.iterator().next().getValue().equals(degree.getValue()));
            });

            // assert the number of vertices remain the same
            if (i == 0) {
                numVertices = computer.count();
            } else {
                assertEquals(numVertices, computer.count());
            }
        }

        correctDegrees.put(entity4, 1l);
        correctDegrees.put(transaction.getRelation(id3), 2l);

        // compute degrees on all types, again and again ...
        for (int i = 0; i < 3; i++) {
            computer = new Analytics(keyspace);
            computer.degreesAndPersist();

            Thread.sleep(5000);

            correctDegrees.entrySet().forEach(degree -> {
                Instance instance = degree.getKey();
                Collection<Resource<?>> resources = null;
                if (instance.isEntity()) {
                    resources = instance.asEntity().resources();
                } else if (instance.isRelation()) {
                    resources = instance.asRelation().resources();
                }
                assert resources != null;
                assert !resources.isEmpty();
                assertEquals(1,resources.size());
                System.out.println(instance);
                System.out.println(resources.iterator().next().getValue());
                assertEquals(resources.iterator().next().getValue(), degree.getValue());
            });

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
        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        RoleType target = transaction.putRoleType("target");
        RoleType value = transaction.putRoleType("value");
        RelationType hasName = transaction.putRelationType("has-name").hasRole(value).hasRole(target);
        EntityType person = transaction.putEntityType("person").playsRole(owner);
        EntityType animal = transaction.putEntityType("animal").playsRole(pet).playsRole(target);
        ResourceType<String> name = transaction.putResourceType("name", Data.STRING).playsRole(value);
        ResourceType<String> altName = transaction.putResourceType("alternate-name", Data.STRING).playsRole(value);

        // add data to the graph
        Entity coco = transaction.putEntity("coco", animal);
        Entity dave = transaction.putEntity("dave", person);
        Resource coconut = transaction.putResource("coconut", name);
        Resource stinky = transaction.putResource("stinky", altName);
        Relation daveOwnsCoco = transaction.addRelation(mansBestFriend).putRolePlayer(owner, dave).putRolePlayer(pet, coco);
        Relation cocoName = transaction.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, coconut);
        Relation cocoAltName = transaction.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, stinky);

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

        transaction.commit();

        mansBestFriend = transaction.getRelationType("mans-best-friend");
        hasName = transaction.getRelationType("has-name");
        person = transaction.getEntityType("person");
        animal = transaction.getEntityType("animal");
        name = transaction.getResourceType("name");

        // create a subgraph excluding resources and the relationship
        Set<Type> subGraphTypes = new HashSet<>();
        subGraphTypes.add(animal);
        subGraphTypes.add(person);
        subGraphTypes.add(mansBestFriend);

        Analytics analytics = new Analytics(keyspace,subGraphTypes);
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(subGraphReferenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });

        // create a subgraph excluding resource type only
        Set<Type> almostFullTypes = new HashSet<>();
        almostFullTypes.add(animal);
        almostFullTypes.add(person);
        almostFullTypes.add(mansBestFriend);
        almostFullTypes.add(hasName);
        almostFullTypes.add(name);

        analytics = new Analytics(keyspace,almostFullTypes);
        degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(almostFullReferenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });

        // full graph
        analytics = new Analytics(keyspace);
        degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });
    }

    @Test
    public void testDegreeIsPersisted() throws Exception {
        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RoleType breeder = transaction.putRoleType("breeder");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = transaction.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = transaction.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = transaction.putEntity("coco", animal);
        Entity dave = transaction.putEntity("dave", person);
        Relation daveBreedsAndOwnsCoco = transaction.addRelation(mansBestFriend)
                .putRolePlayer(pet,coco).putRolePlayer(owner,dave);

        // manual degrees
        Map<String,Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(),1L);
        referenceDegrees.put(dave.getId(),1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(),2L);

        // validate
        transaction.commit();

        // compute and persist degrees
        Analytics analytics = new Analytics(keyspace);
        analytics.degreesAndPersist();

        // check degrees are correct
        referenceDegrees.entrySet().forEach(entry->{
            Instance instance = transaction.getInstance(entry.getKey());
            if (instance.isEntity()) {
                assertTrue(instance.asEntity().resources().iterator().next().getValue().equals(entry.getValue()));
            } else if (instance.isRelation()) {
                assertTrue(instance.asRelation().resources().iterator().next().getValue().equals(entry.getValue()));
            }
        });

        // check only expected resources exist
        Collection<String> allConcepts = new ArrayList<>();
        ResourceType<Long> rt = transaction.getResourceType(Analytics.degree);
        Collection<Resource<Long>> degrees = rt.instances();
        Map<Instance,Long> currentDegrees = new HashMap<>();
        degrees.forEach(degree -> {
            Long degreeValue = degree.getValue();
            degree.ownerInstances().forEach(instance -> {
                currentDegrees.put(instance,degreeValue);
            });
        });

        // check all resources exist and no more
        assertTrue(CollectionUtils.isEqualCollection(currentDegrees.values(), referenceDegrees.values()));

        // persist again and check again
        analytics.degreesAndPersist();

        // check only expected resources exist
        transaction.refresh();
        rt = transaction.getResourceType(Analytics.degree);
        degrees = rt.instances();
        degrees.forEach(i->i.ownerInstances().iterator().forEachRemaining(r ->
                allConcepts.add(r.getId())));

        // check degrees are correct
        referenceDegrees.entrySet().forEach(entry->{
            Instance instance = transaction.getInstance(entry.getKey());
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
            degree.ownerInstances().forEach(instance -> {
                currentDegrees.put(instance,degreeValue);
            });
        });

        // check all resources exist and no more
        assertTrue(CollectionUtils.isEqualCollection(currentDegrees.values(),referenceDegrees.values()));
    }

    @Test
    public void testDegreeIsPersistedInPresenceOfOtherResource() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RoleType breeder = transaction.putRoleType("breeder");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = transaction.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = transaction.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = transaction.putEntity("coco", animal);
        Entity dave = transaction.putEntity("dave", person);
        Relation daveBreedsAndOwnsCoco = transaction.addRelation(mansBestFriend)
                .putRolePlayer(pet,coco).putRolePlayer(owner,dave);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        // create a decoy resource using same relationship
        RoleType degreeOwner = transaction.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(Analytics.degree));
        RoleType degreeValue = transaction.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(Analytics.degree));
        RelationType hasResource = transaction.putRelationType(GraqlType.HAS_RESOURCE.getId(Analytics.degree))
                .hasRole(degreeOwner).hasRole(degreeValue);
        ResourceType<Long> decoyResourceType = transaction.putResourceType("decoy-resource", Data.LONG).playsRole(degreeValue);
        Resource<Long> decoyResource = transaction.putResource(100L, decoyResourceType);
        transaction.addRelation(hasResource).putRolePlayer(degreeOwner,coco).putRolePlayer(degreeValue,decoyResource);
        animal.playsRole(degreeOwner);

        // validate
        transaction.commit();

        mansBestFriend = transaction.getRelationType("mans-best-friend");
        person = transaction.getEntityType("person");
        animal = transaction.getEntityType("animal");
        Set<Type> ct = new HashSet<>();
        ct.add(mansBestFriend);
        ct.add(person);
        ct.add(animal);

        // compute and persist degrees
        Analytics analytics = new Analytics(keyspace,ct);
        analytics.degreesAndPersist();
        ResourceType<Long> degreeResource = transaction.getResourceType(Analytics.degree);

        // check degrees are correct
        boolean isSeen = false;
        for (Map.Entry<String, Long> entry : referenceDegrees.entrySet()) {
            Instance instance = transaction.getInstance(entry.getKey());
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
            isSeen=false;
        }
    }

    @Test
    public void testDegreeIsCorrectAssertionAboutAssertion() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        RoleType target = transaction.putRoleType("target");
        RoleType value = transaction.putRoleType("value");
        RelationType hasName = transaction.putRelationType("has-name").hasRole(value).hasRole(target);
        EntityType person = transaction.putEntityType("person").playsRole(owner);
        EntityType animal = transaction.putEntityType("animal").playsRole(pet).playsRole(target);
        ResourceType<String> name = transaction.putResourceType("name", Data.STRING).playsRole(value);
        ResourceType<String> altName = transaction.putResourceType("alternate-name", Data.STRING).playsRole(value);
        RoleType ownership = transaction.putRoleType("ownership");
        RoleType ownershipResource = transaction.putRoleType("ownership-resource");
        RelationType hasOwnershipResource = transaction.putRelationType("has-ownership-resource").hasRole(ownership).hasRole(ownershipResource);
        ResourceType<String> startDate = transaction.putResourceType("start-date",Data.STRING).playsRole(ownershipResource);
        mansBestFriend.playsRole(ownership);

        // add data to the graph
        Entity coco = transaction.putEntity("coco", animal);
        Entity dave = transaction.putEntity("dave", person);
        Resource coconut = transaction.putResource("coconut",name);
        Resource stinky = transaction.putResource("stinky",altName);
        Relation daveOwnsCoco = transaction.addRelation(mansBestFriend).putRolePlayer(owner,dave).putRolePlayer(pet,coco);
        transaction.addRelation(hasName).putRolePlayer(target,coco).putRolePlayer(value,coconut);
        transaction.addRelation(hasName).putRolePlayer(target,coco).putRolePlayer(value,stinky);
        Resource sd = transaction.putResource("01/01/01",startDate);
        Relation ownsFrom = transaction.addRelation(hasOwnershipResource).putRolePlayer(ownershipResource,sd).putRolePlayer(ownership,daveOwnsCoco);

        // manually compute the degree
        Map<String,Long> referenceDegrees1 = new HashMap<>();
        referenceDegrees1.put(coco.getId(),1L);
        referenceDegrees1.put(dave.getId(),1L);
        referenceDegrees1.put(daveOwnsCoco.getId(),3L);
        referenceDegrees1.put(sd.getId(),1L);
        referenceDegrees1.put(ownsFrom.getId(),2L);

        // manually compute degrees
        Map<String,Long> referenceDegrees2 = new HashMap<>();
        referenceDegrees2.put(coco.getId(),1L);
        referenceDegrees2.put(dave.getId(),1L);
        referenceDegrees2.put(daveOwnsCoco.getId(),2L);

        transaction.commit();

        mansBestFriend = transaction.getRelationType("mans-best-friend");
        person = transaction.getEntityType("person");
        animal = transaction.getEntityType("animal");
        startDate = transaction.getResourceType("start-date");
        hasOwnershipResource = transaction.getRelationType("has-ownership-resource");

        // create a subgraph with assertion on assertion
        Set<Type> ct = new HashSet<>();
        ct.add(animal);
        ct.add(person);
        ct.add(mansBestFriend);
        ct.add(startDate);
        ct.add(hasOwnershipResource);
        Analytics analytics = new Analytics(keyspace,ct);
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees1.get(entry.getKey().getId()),entry.getValue());
        });

        // create subgraph without assertion on assertion
        ct.clear();
        ct.add(animal);
        ct.add(person);
        ct.add(mansBestFriend);
        analytics = new Analytics(keyspace,ct);
        degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees2.get(entry.getKey().getId()),entry.getValue());
        });
    }

    @Test
    public void testDegreeIsCorrectTernaryRelationships() throws MindmapsValidationException, ExecutionException, InterruptedException {

        // make relation
        RoleType productionWithCast = transaction.putRoleType("production-with-cast");
        RoleType actor = transaction.putRoleType("actor");
        RoleType characterBeingPlayed = transaction.putRoleType("character-being-played");
        RelationType hasCast = transaction.putRelationType("has-cast")
                .hasRole(productionWithCast)
                .hasRole(actor)
                .hasRole(characterBeingPlayed);

        EntityType movie = transaction.putEntityType("movie").playsRole(productionWithCast);
        EntityType person = transaction.putEntityType("person").playsRole(actor);
        EntityType character = transaction.putEntityType("character").playsRole(characterBeingPlayed);

        Entity godfather = transaction.putEntity("Godfather", movie);
        Entity marlonBrando = transaction.putEntity("Marlon-Brando", person);
        String marlonId = marlonBrando.getId();
        Entity donVitoCorleone = transaction.putEntity("Don-Vito-Corleone", character);

        Relation relation = transaction.addRelation(hasCast)
                .putRolePlayer(productionWithCast,godfather)
                .putRolePlayer(actor,marlonBrando)
                .putRolePlayer(characterBeingPlayed,donVitoCorleone);
        String relationId = relation.getId();

        transaction.commit();

        Analytics analytics = new Analytics(keyspace);
        Map<Instance, Long> degrees = analytics.degrees();
        assertTrue(degrees.get(transaction.getRelation(relationId)).equals(3L));
        assertTrue(degrees.get(transaction.getEntity(marlonId)).equals(1L));
    }

    @Test
    public void testDegreeIsCorrectOneRoleplayerMultipleRoles() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RoleType breeder = transaction.putRoleType("breeder");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = transaction.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = transaction.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = transaction.putEntity("coco", animal);
        Entity dave = transaction.putEntity("dave", person);

        Relation daveBreedsAndOwnsCoco = transaction.addRelation(mansBestFriend)
                .putRolePlayer(pet, coco)
                .putRolePlayer(owner,dave)
                .putRolePlayer(breeder,dave);

        // manual degrees
        Map<String,Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(),1L);
        referenceDegrees.put(dave.getId(),2L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(),3L);

        // validate
        transaction.commit();

        Analytics analytics = new Analytics(keyspace);
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees.get(entry.getKey().getId()),entry.getValue());
        });
    }

    @Test
    public void testDegreeIsCorrectMissingRoleplayer() throws MindmapsValidationException, ExecutionException, InterruptedException {

        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RoleType breeder = transaction.putRoleType("breeder");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = transaction.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = transaction.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = transaction.putEntity("coco", animal);
        Entity dave = transaction.putEntity("dave", person);
        Relation daveBreedsAndOwnsCoco = transaction.addRelation(mansBestFriend)
                .putRolePlayer(pet,coco).putRolePlayer(owner,dave);

        // manual degrees
        Map<String,Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(),1L);
        referenceDegrees.put(dave.getId(),1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(),2L);

        // validate
        transaction.commit();

        Analytics analytics = new Analytics(keyspace);
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees.get(entry.getKey().getId()),entry.getValue());
        });
    }

    @Test
    public void testDegreeIsCorrectRoleplayerWrongType() throws MindmapsValidationException, ExecutionException, InterruptedException {

        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RoleType breeder = transaction.putRoleType("breeder");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = transaction.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType dog = transaction.putEntityType("dog").playsRole(pet);
        EntityType cat = transaction.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity beast = transaction.putEntity("beast", dog);
        Entity coco = transaction.putEntity("coco", cat);
        Entity dave = transaction.putEntity("dave", person);
        Relation daveBreedsAndOwnsCoco = transaction.addRelation(mansBestFriend)
                .putRolePlayer(owner,dave).putRolePlayer(breeder,dave).putRolePlayer(pet,coco);
        Relation daveBreedsAndOwnsBeast = transaction.addRelation(mansBestFriend)
                .putRolePlayer(owner,dave).putRolePlayer(breeder,dave).putRolePlayer(pet,beast);

        // manual degrees
        Map<String,Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(),1L);
        referenceDegrees.put(dave.getId(),4L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(),3L);
        referenceDegrees.put(daveBreedsAndOwnsBeast.getId(),2L);

        // validate
        transaction.commit();

        // check degree for dave owning cats
        //TODO: should we count the relationship even if there is no cat attached?
        mansBestFriend = transaction.getRelationType("mans-best-friend");
        person = transaction.getEntityType("person");
        cat = transaction.getEntityType("cat");

        Set<Type> ct = new HashSet<>();
        ct.add(mansBestFriend);
        ct.add(person);
        ct.add(cat);
        Analytics analytics = new Analytics(keyspace,ct);
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees.get(entry.getKey().getId()),entry.getValue());
        });
    }

    @Test
    public void testMultipleExecutionOfDegreeAndPersistWhileAddingNodes() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // create a simple graph
        RoleType pet = transaction.putRoleType("pet");
        RoleType owner = transaction.putRoleType("owner");
        RelationType mansBestFriend = transaction.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = transaction.putEntityType("person").playsRole(owner);
        EntityType dog = transaction.putEntityType("dog").playsRole(pet);
        EntityType cat = transaction.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity coco = transaction.putEntity("coco", cat);
        Entity dave = transaction.putEntity("dave", person);
        transaction.addRelation(mansBestFriend)
                .putRolePlayer(owner, dave).putRolePlayer(pet, coco);

        // validate
        transaction.commit();

        // count and persist
        Analytics analytics = new Analytics(keyspace);
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();

        analytics = new Analytics(keyspace);
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();

        analytics = new Analytics(keyspace);
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();
    }
}