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
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.migration.owl.Main;
import ai.grakn.migration.owl.OwlModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;

import static ai.grakn.test.migration.MigratorTestUtils.assertRelationBetweenInstancesExists;
import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OwlMigratorMainTest extends TestOwlGraknBase {

    private String keyspace;

    @Rule
    public final SystemErrRule sysErr = new SystemErrRule().enableLog();

    @Before
    public void setup(){
        keyspace = graph.getKeyspace();
        graph.close();
    }

    @After
    public void delete(){
        graph.admin().delete();
    }

    @Ignore //TODO: Failing due to tighter temporary restrictions
    @Test
    public void owlMigratorCalledWithCorrectArgs_DataMigratedCorrectly(){
        String owlFile = getFile("owl", "shakespeare.owl").getAbsolutePath();
        runAndAssertDataCorrect("owl", "-u", engine.uri(), "-input", owlFile, "-keyspace", keyspace);
    }

    @Test
    public void owlMigratorCalledWithNoData_ErrorIsPrintedToSystemErr(){
        run("owl", "-keyspace", keyspace, "-u", engine.uri());
        assertThat(sysErr.getLog(), containsString("Data file missing (-i)"));
    }

    @Test
    public void owlMigratorCalledInvalidInputFile_ErrorIsPrintedToSystemErr(){
        run("owl", "-input", "grah/?*", "-keyspace", keyspace, "-u", engine.uri());
        assertThat(sysErr.getLog(), containsString("Cannot find file:"));
    }

    public void run(String... args){
        Main.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);

        try(GraknGraph graph = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)) {
            EntityType top = graph.getEntityType("tThing");
            EntityType type = graph.getEntityType("tAuthor");
            assertNotNull(type);
            assertNull(graph.getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
            assertNotNull(type.sup());
            assertEquals("tPerson", type.sup().getLabel().getValue());
            assertEquals(top, type.sup().sup());
            assertTrue(top.subs().contains(graph.getEntityType("tPlace")));
            assertNotEquals(0, type.instances().size());

            assertTrue(
                    type.instances().stream()
                            .flatMap(inst -> inst.resources(graph.getResourceType(OwlModel.IRI.owlname())).stream())
                            .anyMatch(s -> s.getValue().equals("eShakespeare"))
            );
            final Entity author = getEntity("eShakespeare");
            assertNotNull(author);
            final Entity work = getEntity("eHamlet");
            assertNotNull(work);
            assertRelationBetweenInstancesExists(graph, work, author, Label.of("op-wrote"));
            assertTrue(!ReasonerUtils.getRules(graph).isEmpty());
        }
    }
}
