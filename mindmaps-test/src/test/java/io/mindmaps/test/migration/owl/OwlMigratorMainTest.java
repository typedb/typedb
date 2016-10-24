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
import org.junit.Assert;
import org.junit.Test;

public class OwlMigratorMainTest extends TestOwlMindMapsBase{

    @Test
    public void owlMainFileTest(){
        String owlFile = getFile("owl", "shakespeare.owl").getAbsolutePath();
        runAndAssertDataCorrect(new String[]{"owl", "-file", owlFile, "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void owlMainNoFileSpecifiedTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Please specify owl file with the -owl option.");
        runAndAssertDataCorrect(new String[]{"owl", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void owlMainCannotOpenFileTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Cannot find file: grah/?*");
        runAndAssertDataCorrect(new String[]{"owl", "-file", "grah/?*", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void owlMainInvalidFileTest(){
        exception.expect(RuntimeException.class);
        String tsvFile = getFile("csv", "single-file/data/cars.tsv").getAbsolutePath();
        runAndAssertDataCorrect(new String[]{"owl", "-file", tsvFile, "-keyspace", graph.getKeyspace()});
    }

    public void runAndAssertDataCorrect(String[] args){
        Main.main(args);

        EntityType top = graph.getEntityType("tThing");
        EntityType type = graph.getEntityType("tAuthor");
        Assert.assertNotNull(type);
        Assert.assertNull(graph.getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
        Assert.assertNotNull(type.superType());
        Assert.assertEquals("tPerson", type.superType().getId());
        Assert.assertEquals(top, type.superType().superType());
        Assert.assertTrue(top.subTypes().contains(graph.getEntityType("tPlace")));
        Assert.assertNotEquals(0, type.instances().size());
        Assert.assertTrue(
                type.instances().stream().map(Entity::getId).anyMatch(s -> s.equals("eShakespeare"))
        );
        final Entity author = graph.getEntity("eShakespeare");
        Assert.assertNotNull(author);
        final Entity work = graph.getEntity("eHamlet");
        Assert.assertNotNull(work);
        assertRelationBetweenInstancesExists(work, author, "op-wrote");
        Reasoner reasoner = new Reasoner(graph);
        Assert.assertTrue(!reasoner.getRules().isEmpty());
    }
}
