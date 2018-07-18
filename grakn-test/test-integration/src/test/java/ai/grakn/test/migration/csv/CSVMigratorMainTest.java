/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.migration.csv;

import ai.grakn.GraknSession;
import ai.grakn.Keyspace;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.migration.csv.CSVMigrator;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.SampleKBLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;

import static ai.grakn.test.migration.MigratorTestUtils.assertPetGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.assertPokemonGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.MigratorTestUtils.load;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

public class CSVMigratorMainTest {
    private GraknSession factory;
    private Keyspace keyspace;

    private final String dataFile = getFile("csv", "pets/data/pets.csv").getAbsolutePath();
    private final String dataFileBroken = getFile("csv", "pets/data/petsbroken.csv").getAbsolutePath();
    private final String templateFile = getFile("csv", "pets/template.gql").getAbsolutePath();
    private final String templateCorruptFile = getFile("csv", "pets/template-corrupt.gql").getAbsolutePath();

    @ClassRule
    public static final EngineContext engine = EngineContext.create();

    @Rule
    public final SystemOutRule sysOut = new SystemOutRule().enableLog();

    @Rule
    public final SystemErrRule sysErr = new SystemErrRule().enableLog();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup(){
        keyspace = SampleKBLoader.randomKeyspace();
        factory = EmbeddedGraknSession.createEngineSession(keyspace);
        load(factory, getFile("csv", "pets/schema.gql"));
    }
    @After
    public void closeSession(){
        factory.close();
    }

    @Test
    public void whenAFailureOccursDuringLoadingAndTheDebugFlagIsNotSet_DontThrow(){
        run("-u", engine.uri().toString(), "-input", dataFile, "-template", templateCorruptFile, "-keyspace", keyspace.getValue());
    }

    @Test
    public void whenAFailureOccursDuringLoading_PrintError(){
        run("-u", engine.uri().toString(), "-input", dataFile, "-template", templateCorruptFile, "-keyspace", keyspace.getValue());
        assertThat(sysErr.getLog(), anyOf(containsString("bob"), containsString("fridge"), containsString("potato")));
    }

    @Test
    public void whenAFailureOccursDuringLoadingAndTheDebugFlagIsNotSet_DontThrowAndContinue(){
        runAndAssertDataCorrect("-u", engine.uri().toString(), "-input", dataFileBroken, "-template", templateFile, "-keyspace", keyspace.getValue());
    }

    @Test
    public void runningCSVMigrationFromScript_PetDataMigratedCorrectly(){
        runAndAssertDataCorrect("-u", engine.uri().toString(), "-input", dataFile, "-template", templateFile, "-keyspace", keyspace.getValue());
    }

    @Test
    public void usingTabsAsSeparatorInCSVMigratorScript_PetDataMigratedCorrectly(){
        String tsvFile = getFile("csv", "pets/data/pets.tsv").getAbsolutePath();
        runAndAssertDataCorrect("-u", engine.uri().toString(), "-input", tsvFile, "-template", templateFile, "-separator", "\t", "-keyspace", keyspace.getValue());
    }

    @Test
    public void usingSpacesAsSeparatorInCSVMigratorScript_PetDataMigratedCorrectly(){
        String tsvFile = getFile("csv", "pets/data/pets.spaces").getAbsolutePath();
        runAndAssertDataCorrect("-u", engine.uri().toString(), "-input", tsvFile, "-template", templateFile, "-separator", " ", "-keyspace", keyspace.getValue());
    }

    @Test
    public void usingSingleQuotesForStringInCSVMigratorScript_PetDataMigratedCorrectly(){
        String quoteFile = getFile("csv", "pets/data/pets.singlequotes").getAbsolutePath();
        runAndAssertDataCorrect("-u", engine.uri().toString(), "-input", quoteFile, "-template", templateFile, "-quote", "\'", "-keyspace", keyspace.getValue());
    }

    @Test
    public void usingNullsInTemplateInCSVMigratorScript_PetDataMigratedCorrectly(){
        String nullTemplate = getFile("csv", "pets/template-null.gql").getAbsolutePath();
        runAndAssertDataCorrect("-u", engine.uri().toString(), "-input", dataFile, "-template", nullTemplate, "-keyspace", keyspace.getValue(), "-null", "");
    }

    @Test
    public void specifyingIncorrectURIInCSVMigratorScript_ErrorIsPrintedToSystemErr(){
        run("csv", "-input", dataFile, "-template", templateFile, "-uri", "192.0.2.4:4567", "-keyspace", keyspace.getValue());

        assertThat(sysErr.getLog(), containsString("Could not connect to Grakn Engine. Have you run 'grakn server startBackground'?"));
    }

    @Test
    public void usingPropertiesFileInCSVMigratorScript_PetDataMigratedCorrectly(){
        load(factory, getFile("csv", "multi-file/schema.gql"));
        String configurationFile = getFile("csv", "multi-file/migration.yaml").getAbsolutePath();
        run("csv", "-u", engine.uri().toString(), "-config", configurationFile, "-keyspace", keyspace.getValue());

        assertPokemonGraphCorrect(factory);
    }

    @Test
    public void csvMigratorCalledWithNoArgs_HelpMessagePrintedToSystemOut(){
        run();

        assertThat(sysOut.getLog(), containsString("usage: graql migrate"));
    }

    @Test
    public void csvMigratorCalled_PrintsNumberOfQueriesExecuted(){
        run("-u", engine.uri().toString(), "-input", dataFile, "-template", templateFile, "-keyspace", keyspace.getValue());

        assertThat(sysOut.getLog(), containsString("Loaded 9 statements"));
    }

    @Test
    public void csvMigratorCalledWithNoTemplate_ErrorIsPrintedToSystemErr(){
        run("-input", dataFile, "-u", engine.uri().toString());
        assertThat(sysErr.getLog(), containsString("Template file missing (-t)"));
    }

    @Test
    public void csvMigratorCalledWithInvalidTemplateFile_ErrorIsPrintedToSystemErr(){
        run("-input", dataFile + "wrong", "-template", templateFile + "wrong", "-u", engine.uri().toString());
        assertThat(sysErr.getLog(), containsString("Cannot find file"));
    }

    @Test
    public void csvMigratorCalledWithUnknownArgument_ErrorIsPrintedToSystemErr(){
        run("-whale", "");
        assertThat(sysErr.getLog(), containsString("Unrecognized option: -whale"));
    }

    private void run(String... args){
        CSVMigrator.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);

        assertPetGraphCorrect(factory);
    }
}
