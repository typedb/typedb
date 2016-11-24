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

import ai.grakn.concept.Entity;
import ai.grakn.graql.Reasoner;
import ai.grakn.migration.owl.Main;
import ai.grakn.migration.owl.OwlModel;
import ai.grakn.concept.EntityType;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OwlMigratorMainTest extends TestOwlGraknBase {

    @Test
    public void owlMainFileTest(){
        String owlFile = getFile("owl", "shakespeare.owl").getAbsolutePath();
        runAndataCorrect("owl", "-input", owlFile, "-keyspace", graph.getKeyspace());
    }

    @Test
    public void owlMainNoFileSpecifiedTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Please specify owl file with the -i option.");
        run("owl", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void owlMainCannotOpenFileTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Cannot find file: grah/?*");
        run("owl", "-input", "grah/?*", "-keyspace", graph.getKeyspace());
    }

    public void run(String... args){
        Main.main(args);
    }

    public void runAndataCorrect(String... args){
        run(args);

        graph = factory.getGraph();

        EntityType top = graph.getEntityType("tThing");
        EntityType type = graph.getEntityType("tAuthor");
        assertNotNull(type);
        assertNull(graph.getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
        assertNotNull(type.superType());
        assertEquals("tPerson", type.superType().getName());
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
    }
}
