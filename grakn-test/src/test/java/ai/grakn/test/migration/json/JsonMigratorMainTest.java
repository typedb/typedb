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

package ai.grakn.test.migration.json;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.TypeLabel;
import ai.grakn.migration.json.JsonMigrator;
import ai.grakn.test.EngineContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.rules.ExpectedException;

import java.util.Collection;

import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.MigratorTestUtils.getProperties;
import static ai.grakn.test.migration.MigratorTestUtils.getProperty;
import static ai.grakn.test.migration.MigratorTestUtils.getResource;
import static ai.grakn.test.migration.MigratorTestUtils.load;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class JsonMigratorMainTest {

    private final String dataFile = getFile("json", "simple-schema/data.json").getAbsolutePath();
    private final String templateFile = getFile("json", "simple-schema/template.gql").getAbsolutePath();

    private GraknGraph graph;

    @Rule
    public final SystemErrRule sysErr = new SystemErrRule().enableLog();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void setup() {
        GraknSession factory = engine.factoryWithNewKeyspace();
        load(factory, getFile("json", "simple-schema/schema.gql"));
        graph = factory.open(GraknTxType.WRITE);
    }

    @Test
    public void jsonMigratorMainTest(){
        runAndAssertDataCorrect("-u", engine.uri(), "-input", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace());
    }

    @Test
    public void jsonMainNoArgsTest() {
        run("json");
    }

    @Test
    public void jsonMainNoTemplateFileNameTest(){
        run("-input", "");
        assertThat(sysErr.getLog(), containsString("Template file missing (-t)"));
    }

    @Test
    public void jsonMainUnknownArgumentTest(){
        run("-whale", "");
        assertThat(sysErr.getLog(), containsString("Unrecognized option: -whale"));
    }

    @Test
    public void jsonMainNoDataFileExistsTest(){
        run("-input", dataFile + "wrong", "-template", templateFile + "wrong");
        assertThat(sysErr.getLog(), containsString("Cannot find file:"));
    }

    @Test
    public void jsonMainNoTemplateFileExistsTest(){
        run("-input", dataFile, "-template", templateFile + "wrong");
        assertThat(sysErr.getLog(), containsString("Cannot find file:"));
    }

    @Test
    public void jsonMainBatchSizeArgumentTest(){
        runAndAssertDataCorrect("-u", engine.uri(), "-input", dataFile, "-template", templateFile, "-batch", "100", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void jsonMainActiveTasksArgumentTest(){
        runAndAssertDataCorrect("-u", engine.uri(), "-input", dataFile, "-template", templateFile, "-a", "2", "-keyspace", graph.getKeyspace());
    }

    private void run(String... args){
        JsonMigrator.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);

        EntityType personType = graph.getEntityType("person");
        assertEquals(1, personType.instances().size());

        Entity person = personType.instances().iterator().next();
        Entity address = getProperty(graph, person, "has-address").asEntity();
        Entity streetAddress = getProperty(graph, address, "address-has-street").asEntity();

        Resource number = getResource(graph, streetAddress, TypeLabel.of("number")).asResource();
        assertEquals(21L, number.getValue());

        Collection<Instance> phoneNumbers = getProperties(graph, person, "has-phone");
        assertEquals(2, phoneNumbers.size());
    }
}
