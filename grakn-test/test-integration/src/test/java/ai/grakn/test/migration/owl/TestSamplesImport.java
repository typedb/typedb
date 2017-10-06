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
package ai.grakn.test.migration.owl;

import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Role;
import ai.grakn.graql.internal.reasoner.rule.RuleUtil;
import ai.grakn.migration.owl.OwlModel;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.semanticweb.owlapi.model.OWLOntology;

import java.util.Optional;
import java.util.stream.Collectors;

import static ai.grakn.test.migration.MigratorTestUtils.assertRelationBetweenInstancesExists;
import static ai.grakn.test.migration.MigratorTestUtils.assertResourceEntityRelationExists;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Load and verify the schema from the test sample resources.
 *
 * @author borislav
 *
 */
public class TestSamplesImport extends TestOwlGraknBase {

    @Ignore //TODO: Failing due to tighter temporary restrictions
    @Test
    public void testShoppingSchema()  {
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "Shopping.owl");
            migrator.ontology(O).tx(tx).migrate();
            migrator.tx().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            tx = factory.open(GraknTxType.WRITE);
            EntityType type = tx.getEntityType("tMensWear");
            EntityType sub = tx.getEntityType("tTshirts");
            Assert.assertNotNull(type);
            Assert.assertNotNull(sub);
            assertThat(type.subs().collect(Collectors.toSet()), hasItem(sub));
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }       
    }

    @Ignore //TODO: Fix this test. Not sure why it is not working remotely
    @Test
    public void testShakespeareSchema()   {
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "shakespeare.owl");
            migrator.ontology(O).tx(tx).migrate();
            migrator.tx().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            tx = factory.open(GraknTxType.WRITE);
            EntityType top = tx.getEntityType("tThing");
            EntityType type = tx.getEntityType("tAuthor");
            Assert.assertNotNull(type);
            Assert.assertNull(tx.getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
            Assert.assertNotNull(type.sup());
            Assert.assertEquals("tPerson", type.sup().getLabel());
            Assert.assertEquals(top, type.sup().sup());
            assertTrue(top.subs().anyMatch(sub -> sub.equals(tx.getEntityType("tPlace"))));
            Assert.assertNotEquals(0, type.instances().count());

            assertTrue(
                type.instances()
                        .flatMap(inst -> inst
                                .attributes(tx.getAttributeType(OwlModel.IRI.owlname())))
                        .anyMatch(s -> s.getValue().equals("eShakespeare"))
            );
            final Entity author = getEntity("eShakespeare");
            Assert.assertNotNull(author);
            final Entity work = getEntity("eHamlet");
            Assert.assertNotNull(work);
            assertRelationBetweenInstancesExists(tx, work, author, Label.of("op-wrote"));
            Assert.assertTrue(RuleUtil.getRules(tx).findFirst().isPresent());
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }       
    }

    @Ignore("I suspect we going to kill OWL migrator so I am not fussed to fix this test")
    @Test
    public void testProductSchema()   {
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "Product.owl");
            migrator.ontology(O).tx(tx).migrate();
            migrator.tx().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            tx = factory.open(GraknTxType.WRITE);
            EntityType type = tx.getEntityType("tProduct");
            Assert.assertNotNull(type);
            Optional<Entity> e = findById(type.instances().collect(toSet()), "eProduct5");
            assertTrue(e.isPresent());
            e.get().attributes().map(Attribute::type).forEach(System.out::println);
            assertResourceEntityRelationExists(tx, "Product_Available", "14", e.get());
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
    }

    @Ignore("I suspect we going to kill OWL migrator so I am not fussed to fix this test")
    @Test
    public void test1Ontology() {       
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "test1.owl");
            O.axioms().forEach(System.out::println);            
            migrator.ontology(O).tx(tx).migrate();
            migrator.tx().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            tx = factory.open(GraknTxType.WRITE);
            migrator.tx(tx);
            EntityType type = migrator.entityType(owlManager().getOWLDataFactory().getOWLClass(OwlModel.THING.owlname()));          
            Assert.assertNotNull(type);         
            assertTrue(type.instances().flatMap(inst -> inst
                    .attributes(tx.getAttributeType(OwlModel.IRI.owlname())))
                    .anyMatch(s -> s.getValue().equals("eItem1")));

            Entity item1 = getEntity("eItem1");
            // Item1 name data property is "First Name"
            assertTrue(item1.attributes().anyMatch(r -> r.getValue().equals("First Item")));
            item1.attributes().forEach(System.out::println);
            Entity item2 = getEntity("eItem2");
            Role subjectRole = tx.getSchemaConcept(migrator.namer().subjectRole(Label.of("op-related")));
            Role objectRole = tx.getSchemaConcept(migrator.namer().objectRole(Label.of("op-related")));
            assertTrue(item2.relationships(subjectRole).anyMatch(
                    relation -> item1.equals(relation.rolePlayers(objectRole).iterator().next())));
            Role catsubjectRole = tx.getSchemaConcept(migrator.namer().subjectRole(Label.of("op-hasCategory")));
            Role catobjectRole = tx.getSchemaConcept(migrator.namer().objectRole(Label.of("op-hasCategory")));
            assertTrue(catobjectRole.playedByTypes().collect(toSet()).contains(migrator.tx().getEntityType("tCategory")));
            assertTrue(catsubjectRole.playedByTypes().collect(toSet()).contains(migrator.tx().getEntityType("tThing")));
            //Assert.assertFalse(catobjectRole.playedByTypes().contains(migrator.graph().getEntityType("Thing")));

            Entity category2 = getEntity("eCategory2");
            assertTrue(category2.relationships(catobjectRole).anyMatch(
                    relation -> item1.equals(relation.rolePlayers(catsubjectRole).iterator().next())));
            Entity category1 = getEntity("eCategory1");
            category1.attributes().forEach(System.out::println);
            // annotation assertion axioms don't seem to be visited for some reason...need to troubleshoot seems like 
            // OWLAPI issue
            //this.checkResource(category1, "comment", "category 1 comment");
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
    }

    @Ignore //TODO: Fix this test. Not sure why it is not working remotely
    @Test
    public void testFamilySchema()   {
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "family.owl");
            migrator.ontology(O).tx(tx).migrate();
            migrator.tx().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            EntityType type = migrator.tx().getEntityType("tPerson");
            Assert.assertNotNull(type);

            RelationshipType ancestor = migrator.tx().getRelationshipType("op-hasAncestor");
            RelationshipType isSiblingOf = migrator.tx().getRelationshipType("op-isSiblingOf");
            RelationshipType isAuntOf = migrator.tx().getRelationshipType("op-isAuntOf");
            RelationshipType isUncleOf = migrator.tx().getRelationshipType("op-isUncleOf");
            RelationshipType bloodRelation = migrator.tx().getRelationshipType("op-isBloodRelationOf");

            assertTrue(bloodRelation.subs().anyMatch(sub -> sub.equals(ancestor)));
            assertTrue(bloodRelation.subs().anyMatch(sub -> sub.equals(isSiblingOf)));
            assertTrue(bloodRelation.subs().anyMatch(sub -> sub.equals(isAuntOf)));
            assertTrue(bloodRelation.subs().anyMatch(sub -> sub.equals(isUncleOf)));

            assertTrue(RuleUtil.getRules(tx).findFirst().isPresent());
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
    }
}