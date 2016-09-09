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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.RoleType;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.migration.owl.OWLMigrator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for OWL migrator unit tests: create and holds OWL manager and
 * MM graph, statically because they don't need to be re-initialized on a per
 * test basis.
 * 
 * @author borislav
 *
 */
public class TestOwlMindMapsBase {
    public static final String OWL_TEST_GRAPH = "owltestgraph";
 
    static MindmapsGraph graph =
             MindmapsTestGraphFactory.newEmptyGraph();  
             // MindmapsClient.getGraph(OWL_TEST_GRAPH);
    
    static OWLOntologyManager manager;
    
    @BeforeClass
    public static void initStatic() {
        manager = OWLManager.createOWLOntologyManager();
    }
    
    @AfterClass
    public static void closeGraph() {   
        graph.close();
    }
    
    OWLMigrator migrator;

    @Before
    public void initMigrator() {
         migrator = new OWLMigrator();
    }

    OWLOntologyManager owlManager() {
        return manager;
    }
    
    OWLOntology loadOntologyFromResource(String resource) {
        try (InputStream in = this.getClass().getResourceAsStream(resource)) {
            if (in == null)
                throw new NullPointerException("Resource : " + resource + " not found.");
            return owlManager().loadOntologyFromOntologyDocument(in);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }   

    <T extends Concept> Optional<T> findById(Collection<T> C, String id) {
        return C.stream().filter(x -> x.getId().equals(id)).findFirst();
    }
    
    void checkResource(final Entity e, final String resourceTypeId, final Object value) {
        Optional<Resource<?>> r = e.resources().stream().filter(x -> x.type().getId().equals(resourceTypeId)).findFirst();
        Assert.assertTrue(r.isPresent());
        Assert.assertEquals(value, r.get().getValue());
    }
    
    void checkRelation(Entity subject, String relationTypeId, Entity object) {
        RelationType relationType = graph.getRelationType(relationTypeId);
        final RoleType subjectRole = graph.getRoleType(migrator.namer().subjectRole(relationType.getId()));
        final RoleType objectRole = graph.getRoleType(migrator.namer().objectRole(relationType.getId()));
        Assert.assertNotNull(subjectRole);
        Assert.assertNotNull(objectRole);
        Optional<Relation> relation = relationType.instances().stream().filter(rel -> {
            Map<RoleType, Instance> players = rel.rolePlayers();
            return subject.equals(players.get(subjectRole)) && object.equals(players.get(objectRole)); 
        }).findFirst();
        Assert.assertTrue(relation.isPresent());
    }
}