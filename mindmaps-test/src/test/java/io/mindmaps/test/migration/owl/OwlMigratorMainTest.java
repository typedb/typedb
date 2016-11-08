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

package io.mindmaps.test.migration.owl;

import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.migration.owl.Main;
import io.mindmaps.migration.owl.OwlModel;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OwlMigratorMainTest extends TestOwlMindMapsBase{

    @Test
    public void owlMainFileTest(){
        String owlFile = getFile("owl", "shakespeare.owl").getAbsolutePath();
        exit.expectSystemExitWithStatus(0);
        runAndataCorrect(new String[]{"owl", "-input", owlFile, "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void owlMainNoFileSpecifiedTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Please specify owl file with the -i option.");
        run(new String[]{"owl", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void owlMainCannotOpenFileTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Cannot find file: grah/?*");
        run(new String[]{"owl", "-input", "grah/?*", "-keyspace", graph.getKeyspace()});
    }

    public void run(String[] args){
        Main.main(args);
    }

    public void runAndataCorrect(String[] args){
        exit.checkAssertionAfterwards(() -> {
            graph = factory.getGraph();

            EntityType top = graph.getEntityType("tThing");
            EntityType type = graph.getEntityType("tAuthor");
            assertNotNull(type);
            assertNull(graph.getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
            assertNotNull(type.superType());
            assertEquals("tPerson", type.superType().getId());
            assertEquals(top, type.superType().superType());
            assertTrue(top.subTypes().contains(graph.getEntityType("tPlace")));
            assertNotEquals(0, type.instances().size());

            assertTrue(
                    type.instances().stream()
                            .flatMap(inst -> inst.asEntity()
                                    .resources(graph.getResourceType(OwlModel.IRI.owlname())).stream())
                            .anyMatch(s -> s.getValue().equals("eShakespeare"))
            );
            final Entity author = getEntity("eShakespeare");
            assertNotNull(author);
            final Entity work = getEntity("eHamlet");
            assertNotNull(work);
            assertRelationBetweenInstancesExists(work, author, "op-wrote");
            assertTrue(!Reasoner.getRules(graph).isEmpty());
        });

        run(args);
    }
}
