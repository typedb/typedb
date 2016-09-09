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
package test.io.mindmaps.migration.owl;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.semanticweb.owlapi.model.OWLOntology;

import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.RoleType;
import io.mindmaps.migration.owl.OwlModel;

/**
 * Load and verify the ontology from the test sample resources. 
 * 
 * @author borislav
 *
 */
public class TestSamplesImport extends TestOwlMindMapsBase {
    
    @Test
    public void testShoppingOntology()  {       
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("/io/mindmaps/migration/owl/samples/Shopping.owl");
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }
        // Verify
        try {
            EntityType type = migrator.graph().getEntityType("tMensWear");
            EntityType sub = migrator.graph().getEntityType("tTshirts");
            Assert.assertNotNull(type);
            Assert.assertNotNull(sub);
            Assert.assertTrue(type.subTypes().contains(sub));
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }       
    }
    
    @Test
    public void testShakespeareOntology()   {       
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("/io/mindmaps/migration/owl/samples/shakespeare.owl");         
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }
        // Verify
        try {
            EntityType top = migrator.graph().getEntityType("tThing");
            EntityType type = migrator.graph().getEntityType("tAuthor");
            Assert.assertNotNull(type);
            Assert.assertNull(migrator.graph().getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
            Assert.assertNotNull(type.superType());
            Assert.assertEquals("tPerson", type.superType().getId());
            Assert.assertEquals(top, type.superType().superType());
            Assert.assertTrue(top.subTypes().contains(migrator.graph().getEntityType("tPlace")));
            Assert.assertNotEquals(0, type.instances().size());
            Assert.assertTrue(
                type.instances().stream().map(Entity::getId).anyMatch(s -> s.equals("eShakespeare"))
            );
            final Entity author = migrator.graph().getEntity("eShakespeare");
            Assert.assertNotNull(author);
            final Entity work = migrator.graph().getEntity("eHamlet");
            Assert.assertNotNull(work);
            checkRelation(author, "op-wrote", work);
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }       
    }
    
    @Test
    public void testProductOntology()   {
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("/io/mindmaps/migration/owl/samples/Product.owl");
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }
        // Verify
        try {
            EntityType type = migrator.graph().getEntityType("tProduct");
            Assert.assertNotNull(type);
            Optional<Entity> e = findById(type.instances(), "eProduct5");
            Assert.assertTrue(e.isPresent());
            e.get().resources().stream().map(Resource::type).forEach(System.out::println);
            checkResource(e.get(),  "Product_Available", "14"); 
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }   
    
    @Test
    public void test1Ontology() {       
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("/io/mindmaps/migration/owl/samples/test1.owl");
            O.axioms().forEach(System.out::println);            
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }
        // Verify
        try {
            EntityType type = migrator.entityType(owlManager().getOWLDataFactory().getOWLClass(OwlModel.THING.owlname()));          
            Assert.assertNotNull(type);         
            Assert.assertTrue(type.instances().stream().map(Entity::getId).anyMatch(id -> id.equals("eItem1")));
            Entity item1 = migrator.graph().getEntity("eItem1");
            // Item1 name data property is "First Name"
            item1.resources().stream().anyMatch(r -> r.getValue().equals("First Item"));
            item1.resources().stream().forEach(System.out::println);
            Entity item2 = migrator.graph().getEntity("eItem2");
            RoleType subjectRole = migrator.graph().getRoleType(migrator.namer().subjectRole("op-related"));
            RoleType objectRole = migrator.graph().getRoleType(migrator.namer().objectRole("op-related"));
            Assert.assertTrue(item2.relations(subjectRole).stream().anyMatch(
                    relation -> item1.equals(relation.rolePlayers().get(objectRole))));
            RoleType catsubjectRole = migrator.graph().getRoleType(migrator.namer().subjectRole("op-hasCategory"));
            RoleType catobjectRole = migrator.graph().getRoleType(migrator.namer().objectRole("op-hasCategory"));
            Assert.assertTrue(catobjectRole.playedByTypes().contains(migrator.graph().getEntityType("tCategory")));
            Assert.assertTrue(catsubjectRole.playedByTypes().contains(migrator.graph().getEntityType("tThing")));
            //Assert.assertFalse(catobjectRole.playedByTypes().contains(migrator.graph().getEntityType("Thing")));
            Entity category2 = migrator.graph().getEntity("eCategory2");
            Assert.assertTrue(category2.relations(catobjectRole).stream().anyMatch(
                    relation -> item1.equals(relation.rolePlayers().get(catsubjectRole))));
            Entity category1 = migrator.graph().getEntity("eCategory1");
            category1.resources().forEach(System.out::println);
            // annotation assertion axioms don't seem to be visited for some reason...need to troubleshoot seems like 
            // OWLAPI issue
            //this.checkResource(category1, "comment", "category 1 comment");
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }   
    
    public static void main(String []argv) {
        JUnitCore junit = new JUnitCore();
        Result result = null;
        do      {
            result = junit.run(Request.method(TestSamplesImport.class, "test1Ontology"));
        } while (result.getFailureCount() == 0 && false);
        System.out.println("Failures " + result.getFailureCount());
        if (result.getFailureCount() > 0) {
            for (Failure failure : result.getFailures()) {
                failure.getException().printStackTrace();
            }
        }
        System.exit(0);     
    }
}