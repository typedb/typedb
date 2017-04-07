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

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.migration.owl.Main;
import ai.grakn.migration.owl.OwlModel;
import org.junit.Before;
import org.junit.Test;

import static ai.grakn.test.migration.MigratorTestUtils.assertRelationBetweenInstancesExists;
import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OwlMigratorMainTest extends TestOwlGraknBase {

    private String keyspace;

    @Before
    public void setup(){
        keyspace = graph.getKeyspace();
        graph.close();
    }

    @Test
    public void owlMainFileTest(){
        String owlFile = getFile("owl", "shakespeare.owl").getAbsolutePath();
        runAndAssertDataCorrect("owl", "-input", owlFile, "-keyspace", keyspace);
    }

    @Test
    public void owlMainNoFileSpecifiedTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Data file missing (-i)");
        run("owl", "-keyspace", keyspace);
    }

    @Test
    public void owlMainCannotOpenFileTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Cannot find file:");
        run("owl", "-input", "grah/?*", "-keyspace", keyspace);
    }

    public void run(String... args){
        Main.main(args);
    }

    public void runAndAssertDataCorrect(String... args){
        run(args);

        graph = Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE);
        EntityType top = graph.getEntityType("tThing");
        EntityType type = graph.getEntityType("tAuthor");
        assertNotNull(type);
        assertNull(graph.getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
        assertNotNull(type.superType());
        assertEquals("tPerson", type.superType().getLabel().getValue());
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
        assertRelationBetweenInstancesExists(graph, work, author, TypeLabel.of("op-wrote"));
        assertTrue(!Reasoner.getRules(graph).isEmpty());
    }
}
