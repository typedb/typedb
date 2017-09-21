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

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.migration.json.JsonMigrator;
import ai.grakn.test.EngineContext;
import ai.grakn.util.SampleKBLoader;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;

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

    private Keyspace keyspace;
    private GraknSession session;

    @Rule
    public final SystemOutRule sysOut = new SystemOutRule().enableLog();

    @Rule
    public final SystemErrRule sysErr = new SystemErrRule().enableLog();

    @ClassRule
    public static final EngineContext engine = EngineContext.inMemoryServer();

    @Before
    public void setup() {
        keyspace = SampleKBLoader.randomKeyspace();
        session = Grakn.session(engine.uri(), keyspace);

        load(session, getFile("json", "simple-schema/schema.gql"));
    }

    @Test
    public void jsonMigratorCalledWithCorrectArgs_DataMigratedCorrectly(){
        runAndAssertDataCorrect("-u", engine.uri(), "-input", dataFile, "-template", templateFile, "-keyspace", keyspace.getValue());
    }

    @Test
    public void jsonMigratorCalledWithNoArgs_HelpMessagePrintedToSystemOut() {
        run("json");
        assertThat(sysOut.getLog(), containsString("usage: graql migrate"));
    }

    @Test
    public void jsonMigratorCalledWithNoTemplate_ErrorIsPrintedToSystemErr(){
        run("-input", "", "-u", engine.uri());
        assertThat(sysErr.getLog(), containsString("Template file missing (-t)"));
    }

    @Test
    public void jsonMigratorCalledWithUnknownArgument_ErrorIsPrintedToSystemErr(){
        run("-whale", "");
        assertThat(sysErr.getLog(), containsString("Unrecognized option: -whale"));
    }

    @Test
    public void jsonMigratorCalledInvalidInputFile_ErrorIsPrintedToSystemErr(){
        run("-input", dataFile + "wrong", "-template", templateFile + "wrong", "-u", engine.uri());
        assertThat(sysErr.getLog(), containsString("Cannot find file:"));
    }

    @Test
    public void jsonMigratorCalledInvalidTemplateFile_ErrorIsPrintedToSystemErr(){
        run("-input", dataFile, "-template", templateFile + "wrong", "-u", engine.uri());
        assertThat(sysErr.getLog(), containsString("Cannot find file:"));
    }

    @Test
    public void whenMigrationFailsOnTheServer_ErrorIsPrintedToSystemErr(){
        run("-u", engine.uri(), "-input", dataFile, "-template", templateFile, "-keyspace", "wrongkeyspace");
        String expectedMessage = GraknBackendException.noSuchKeyspace(Keyspace.of("wrongkeyspace")).getMessage();
        // TODO Temporarily checking sysOut. Change it so it goes to sysErr
        assertThat(sysOut.getLog(), containsString(expectedMessage));
    }

    private void run(String... args){
        JsonMigrator.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);

        try(GraknTx graph = session.open(GraknTxType.READ)) {
            EntityType personType = graph.getEntityType("person");
            assertEquals(1, personType.instances().count());

            Entity person = personType.instances().iterator().next();
            Entity address = getProperty(graph, person, "has-address").asEntity();
            Entity streetAddress = getProperty(graph, address, "address-has-street").asEntity();

            Attribute number = getResource(graph, streetAddress, Label.of("number"));
            assertEquals(21L, number.getValue());

            Collection<Thing> phoneNumbers = getProperties(graph, person, "has-phone");
            assertEquals(2, phoneNumbers.size());
        }
    }
}
