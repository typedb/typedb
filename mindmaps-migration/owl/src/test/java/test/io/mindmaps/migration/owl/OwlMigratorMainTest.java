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
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.engine.MindmapsEngineServer;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.migration.owl.Main;
import io.mindmaps.migration.owl.OwlModel;
import java.util.Collection;
import java.util.Optional;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.ExpectedException;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class OwlMigratorMainTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final String GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
    private MindmapsGraph graph;

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        MindmapsEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        MindmapsEngineServer.stop();
    }

    @Before
    public void setup(){
        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);

        exit.expectSystemExitWithStatus(0);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void owlMainFileTest(){
        String owlFile = getFile("shakespeare.owl").getAbsolutePath();
        runAndAssertDataCorrect(new String[]{"owl", "-input", owlFile, "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void owlMainNoFileSpecifiedTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Please specify owl file with the -i option.");
        runAndAssertDataCorrect(new String[]{"owl", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void owlMainCannotOpenFileTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Cannot find file: grah/?*");
        runAndAssertDataCorrect(new String[]{"owl", "-input", "grah/?*", "-keyspace", graph.getKeyspace()});
    }

    public ResourceType<String> owlIriResource(){ return graph.getResourceType(OwlModel.IRI.owlname());}

    public <T> Entity getEntity(T id, ResourceType<T> rtype){
        Resource<T> iri = graph.getResource(id, rtype);
        Instance inst = iri != null? iri.ownerInstances().stream().findFirst().orElse(null) : null;
        return inst != null? inst.asEntity() : null;
    }

    public Entity getEntity(String id){ return getEntity(id, owlIriResource());}

    public void runAndAssertDataCorrect(String[] args){
        Main.main(args);

        EntityType top = graph.getEntityType("tThing");
        EntityType type = graph.getEntityType("tAuthor");
        Assert.assertNotNull(type);
        Assert.assertNull(graph.getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
        Assert.assertNotNull(type.superType());
        Assert.assertEquals("tPerson", type.superType().getId());
        Assert.assertEquals(top, type.superType().superType());
        assertTrue(top.subTypes().contains(graph.getEntityType("tPlace")));
        Assert.assertNotEquals(0, type.instances().size());
        assertTrue(type.instances().stream()
                .flatMap(inst -> inst.asEntity()
                .resources(graph.getResourceType(OwlModel.IRI.owlname())).stream())
                .anyMatch(s -> s.getValue().equals("eShakespeare"))
        );
        final Entity author = getEntity("eShakespeare");
        Assert.assertNotNull(author);
        final Entity work = getEntity("eHamlet");
        Assert.assertNotNull(work);
        assertRelationBetweenInstancesExists(work, author, "op-wrote");
        Reasoner reasoner = new Reasoner(graph);
        assertTrue(!reasoner.getRules(graph).isEmpty());
    }

    public static File getFile(String fileName){
        return new File(OwlMigratorMainTest.class.getClassLoader().getResource(fileName).getPath());
    }

    public void assertRelationBetweenInstancesExists(Instance instance1, Instance instance2, String relation){
        RelationType relationType = graph.getRelationType(relation);

        RoleType role1 = instance1.playsRoles().stream().filter(r -> r.relationType().equals(relationType)).findFirst().get();
        assertTrue(instance1.relations(role1).stream().anyMatch(rel -> rel.rolePlayers().values().contains(instance2)));
    }
}
