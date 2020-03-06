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

package grakn.core.graql.analytics;

import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import org.junit.After;
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

import static graql.lang.Graql.Token.Compute.Algorithm.DEGREE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class DegreeIT {

    public Session session;
    private Transaction tx;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.writeTransaction();
    }

    @After
    public void closeSession() { session.close(); }

    @Test
    public void testDegreesSimple() {
        // create instances
        EntityType thingy = tx.putEntityType("thingy");
        EntityType anotherThing = tx.putEntityType("another");

        ConceptId entity1 = thingy.create().id();
        ConceptId entity2 = thingy.create().id();
        ConceptId entity3 = thingy.create().id();
        ConceptId entity4 = anotherThing.create().id();

        Role role1 = tx.putRole("role1");
        Role role2 = tx.putRole("role2");
        thingy.plays(role1).plays(role2);
        anotherThing.plays(role1).plays(role2);
        RelationType related = tx.putRelationType("related").relates(role1).relates(role2);

        // relate them
        related.create()
                .assign(role1, tx.getConcept(entity1))
                .assign(role2, tx.getConcept(entity2));
        related.create()
                .assign(role1, tx.getConcept(entity2))
                .assign(role2, tx.getConcept(entity3));
        related.create()
                .assign(role1, tx.getConcept(entity2))
                .assign(role2, tx.getConcept(entity4));
        tx.commit();

        tx = session.readTransaction();

        Map<ConceptId, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1L);
        correctDegrees.put(entity2, 3L);
        correctDegrees.put(entity3, 1L);
        correctDegrees.put(entity4, 1L);

        // compute degrees
        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }
        tx.close();

        Set<List<ConceptSetMeasure>> result = list.parallelStream().map(i -> {
            try (Transaction tx = session.readTransaction()) {
                return tx.execute(Graql.compute().centrality().using(DEGREE));
            }
        }).collect(Collectors.toSet());
        assertEquals(1, result.size());
        List<ConceptSetMeasure> degrees0 = result.iterator().next();
        assertEquals(2, degrees0.size());
        degrees0.forEach(conceptSetMeasure -> conceptSetMeasure.set().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id).longValue(), conceptSetMeasure.measurement().longValue());
                }
        ));

        try (Transaction tx = session.readTransaction()) {
            List<ConceptSetMeasure> degrees1 = tx.execute(Graql.compute().centrality().using(DEGREE).of("thingy"));

            assertEquals(2, degrees1.size());

            assertEquals(2, degrees1.get(0).set().size());
            assertEquals(1, degrees1.get(0).measurement().intValue());

            assertEquals(1, degrees1.get(1).set().size());
            assertEquals(3, degrees1.get(1).measurement().intValue());

            degrees1.forEach(conceptSetMeasure -> conceptSetMeasure.set().forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(id));
                        assertEquals(correctDegrees.get(id).longValue(), conceptSetMeasure.measurement().longValue());
                    }
            ));

            List<ConceptSetMeasure> degrees2 = tx.execute(Graql.compute().centrality().using(DEGREE).of("thingy", "related"));
            assertTrue(degrees1.containsAll(degrees2));

            degrees2 = tx.execute(Graql.compute().centrality().using(DEGREE));
            assertTrue(degrees0.containsAll(degrees2));

            // compute degrees on subgraph
            List<ConceptSetMeasure> degrees3 = tx.execute(Graql.compute().centrality().using(DEGREE).in("thingy", "related"));
            assertTrue(degrees1.containsAll(degrees3));

            degrees3 = tx.execute(Graql.compute().centrality().using(DEGREE).of("thingy").in("related"));
            assertTrue(degrees1.containsAll(degrees3));
        }
    }

    @Test
    public void testSubIsAccountedForInSubgraph() {
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");

        Entity person = tx.putEntityType("person").plays(owner).create();

        EntityType animal = tx.putEntityType("animal").plays(pet);
        Entity dog = tx.putEntityType("dog").sup(animal).create();

        tx.putRelationType("mans-best-friend").relates(pet).relates(owner)
                .create().assign(pet, dog).assign(owner, person);

        List<ConceptSetMeasure> correctDegrees = new ArrayList<>();
        correctDegrees.add(new ConceptSetMeasure(Sets.newHashSet(person.id(), dog.id()), 1));

        tx.commit();

        try (Transaction tx = session.readTransaction()) {
            // set subgraph, use animal instead of dog
            Set<String> ct = Sets.newHashSet("person", "animal", "mans-best-friend");
            List<ConceptSetMeasure> degrees = tx.execute(Graql.compute().centrality().using(DEGREE).in(ct));
            // check that dog has a degree to confirm sub has been inferred
            assertTrue(correctDegrees.containsAll(degrees));
        }
    }

    @Test
    public void testDegreeTwoAttributes() throws InvalidKBException {
        // create a simple tx
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        RelationType mansBestFriend = tx.putRelationType("mans-best-friend").relates(pet).relates(owner);

        EntityType person = tx.putEntityType("person").plays(owner);
        EntityType animal = tx.putEntityType("animal").plays(pet);
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        AttributeType<String> altName =
                tx.putAttributeType("alternate-name", AttributeType.DataType.STRING);

        animal.has(name).has(altName);

        // add data to the graph
        Entity coco = animal.create();
        Entity dave = person.create();
        Attribute coconut = name.create("coconut");
        Attribute stinky = altName.create("stinky");
        mansBestFriend.create().assign(owner, dave).assign(pet, coco);
        coco.has(coconut).has(stinky);

        // manually compute the degree for small graph
        List<ConceptSetMeasure> subgraphReferenceDegrees = new ArrayList<>();
        subgraphReferenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(coco.id(), dave.id()), 1));

        // manually compute degree for almost full graph
        List<ConceptSetMeasure> almostFullReferenceDegrees = new ArrayList<>();
        almostFullReferenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(coco.id()), 2));
        almostFullReferenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(dave.id(), coconut.id()), 1));

        // manually compute degrees
        List<ConceptSetMeasure> fullReferenceDegrees = new ArrayList<>();
        fullReferenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(coco.id()), 3));
        fullReferenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(dave.id(), coconut.id(), stinky.id()), 1));

        tx.commit();

        try (Transaction tx = session.readTransaction()) {

            // create a subgraph excluding attributes and their relation
            HashSet<String> subGraphTypes = Sets.newHashSet("animal", "person", "mans-best-friend");
            List<ConceptSetMeasure> degrees = tx.execute(Graql.compute().centrality().using(DEGREE)
                    .in(subGraphTypes));
            assertTrue(subgraphReferenceDegrees.containsAll(degrees));

            // create a subgraph excluding one attribute type only
            HashSet<String> almostFullTypes = Sets.newHashSet("animal", "person", "mans-best-friend", "@has-name", "name");
            degrees = tx.execute(Graql.compute().centrality().using(DEGREE).in(almostFullTypes));
            assertTrue(almostFullReferenceDegrees.containsAll(degrees));

            // full graph
            degrees = tx.execute(Graql.compute().centrality().using(DEGREE));
            assertTrue(fullReferenceDegrees.containsAll(degrees));
        }
    }

    @Test
    public void testDegreeMissingRolePlayer() {
        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        Role breeder = tx.putRole("breeder");
        RelationType mansBestFriend = tx.putRelationType("mans-best-friend")
                .relates(pet).relates(owner).relates(breeder);
        EntityType person = tx.putEntityType("person").plays(owner).plays(breeder);
        EntityType animal = tx.putEntityType("animal").plays(pet);

        // make one person breeder and owner
        Entity coco = animal.create();
        Entity dave = person.create();
        mansBestFriend.create().assign(pet, coco).assign(owner, dave);

        // manual degrees
        List<ConceptSetMeasure> referenceDegrees = new ArrayList<>();
        referenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(coco.id(), dave.id()), 1));

        tx.commit();

        try (Transaction tx = session.readTransaction()) {
            List<ConceptSetMeasure> degrees = tx.execute(Graql.compute().centrality().using(DEGREE));
            assertTrue(referenceDegrees.containsAll(degrees));
        }
    }

    @Test
    public void testRelationPlaysARole() throws InvalidKBException {

        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        RelationType mansBestFriend = tx.putRelationType("mans-best-friend").relates(pet).relates(owner);

        EntityType person = tx.putEntityType("person").plays(owner);
        EntityType animal = tx.putEntityType("animal").plays(pet);

        Role ownership = tx.putRole("ownership");
        Role ownershipResource = tx.putRole("ownership-resource");
        RelationType hasOwnershipResource = tx.putRelationType("has-ownership-resource")
                .relates(ownership).relates(ownershipResource);

        AttributeType<String> startDate = tx.putAttributeType("start-date", AttributeType.DataType.STRING);
        startDate.plays(ownershipResource);
        mansBestFriend.plays(ownership);

        // add instances
        Entity coco = animal.create();
        Entity dave = person.create();
        Relation daveOwnsCoco = mansBestFriend.create()
                .assign(owner, dave).assign(pet, coco);
        Attribute aStartDate = startDate.create("01/01/01");
        hasOwnershipResource.create()
                .assign(ownershipResource, aStartDate).assign(ownership, daveOwnsCoco);

        List<ConceptSetMeasure> referenceDegrees = new ArrayList<>();
        referenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(coco.id(), dave.id(), aStartDate.id(), daveOwnsCoco.id()), 1));

        tx.commit();

        try (Transaction tx = session.readTransaction()) {
            List<ConceptSetMeasure> degrees = tx.execute(Graql.compute().centrality().using(DEGREE));
            assertTrue(referenceDegrees.containsAll(degrees));
        }
    }

    @Test
    public void testDegreeTernaryRelations() throws InvalidKBException {
        // make relation
        Role productionWithCast = tx.putRole("production-with-cast");
        Role actor = tx.putRole("actor");
        Role characterBeingPlayed = tx.putRole("character-being-played");
        RelationType hasCast = tx.putRelationType("has-cast")
                .relates(productionWithCast)
                .relates(actor)
                .relates(characterBeingPlayed);

        EntityType movie = tx.putEntityType("movie").plays(productionWithCast);
        EntityType person = tx.putEntityType("person").plays(actor);
        EntityType character = tx.putEntityType("character").plays(characterBeingPlayed);

        Entity godfather = movie.create();
        Entity marlonBrando = person.create();
        Entity donVitoCorleone = character.create();

        hasCast.create()
                .assign(productionWithCast, godfather)
                .assign(actor, marlonBrando)
                .assign(characterBeingPlayed, donVitoCorleone);

        List<ConceptSetMeasure> referenceDegrees = new ArrayList<>();
        referenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(godfather.id(), marlonBrando.id(), donVitoCorleone.id()), 1));

        tx.commit();

        try (Transaction tx = session.readTransaction()) {
            List<ConceptSetMeasure> degrees = tx.execute(Graql.compute().centrality().using(DEGREE));
            assertTrue(referenceDegrees.containsAll(degrees));
        }
    }

    @Test
    public void testOneRolePlayerMultipleRoles() throws InvalidKBException {

        Role pet = tx.putRole("pet");
        Role owner = tx.putRole("owner");
        Role breeder = tx.putRole("breeder");
        RelationType mansBestFriend = tx.putRelationType("mans-best-friend")
                .relates(pet).relates(owner).relates(breeder);
        EntityType person = tx.putEntityType("person").plays(owner).plays(breeder);
        EntityType animal = tx.putEntityType("animal").plays(pet);

        // make one person breeder and owner
        Entity coco = animal.create();
        Entity dave = person.create();

        mansBestFriend.create()
                .assign(pet, coco)
                .assign(owner, dave)
                .assign(breeder, dave);

        List<ConceptSetMeasure> referenceDegrees = new ArrayList<>();
        referenceDegrees.add(new ConceptSetMeasure(Sets.newHashSet(coco.id()), 1));
        referenceDegrees.add(new ConceptSetMeasure(Collections.singleton(dave.id()), 2));

        tx.commit();

        try (Transaction tx = session.readTransaction()) {
            List<ConceptSetMeasure> degrees = tx.execute(Graql.compute().centrality().using(DEGREE));
            assertTrue(referenceDegrees.containsAll(degrees));
        }
    }
}