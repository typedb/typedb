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

package ai.grakn.test.migration.sql;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.Keyspace;
import ai.grakn.migration.sql.SQLMigrator;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.SampleKBLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;

import java.sql.Connection;
import java.sql.SQLException;

import static ai.grakn.test.migration.MigratorTestUtils.assertPetGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.assertPokemonGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.DRIVER;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.PASS;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.URL;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.USER;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.setupExample;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class SQLMigratorMainTest {

    private final String templateFile = getFile("sql", "pets/template.gql").getAbsolutePath();
    private final String query = "SELECT * FROM pet";
    private Connection connection;
    private GraknSession factory;
    private Keyspace keyspace;

    @Rule
    public final SystemErrRule sysErr = new SystemErrRule().enableLog();

    @ClassRule
    public static final EngineContext engine = EngineContext.create();

    @Before
    public void setup() throws SQLException {
        keyspace = SampleKBLoader.randomKeyspace();
        factory = Grakn.session(engine.uri(), keyspace);
        connection = setupExample(factory, "pets");
    }

    @After
    public void stop() throws SQLException {
        connection.close();
    }

    @Test
    public void runningSQLMigrationFromScript_PetDataMigratedCorrectly(){
        runAndAssertDataCorrect("sql", "-u", engine.uri().toString(), "-t", templateFile,
                "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query, "-k", keyspace.getValue());
    }

    @Test
    public void sqlMigratorCalledWithoutKeyspace_ErrorIsPrintedToSystemErr(){
        run("sql", "-u", engine.uri().toString(), "-pass", PASS, "-location", URL, "-q", query, "-t", templateFile, "-user", USER);
        assertThat(sysErr.getLog(), containsString("Keyspace missing (-k)"));
    }

    @Test
    public void sqlMigratorCalledWithoutSQLConnectionUser_ErrorIsPrintedToSystemErr(){
        run("sql", "-u", engine.uri().toString(), "-pass", PASS, "-location", URL, "-q", query, "-t", templateFile, "-k", keyspace.getValue());
        assertThat(sysErr.getLog(), containsString("No username specified (-user)"));
    }

    @Test
    public void sqlMigratorCalledWithoutSQLConnectionPassword_ErrorIsPrintedToSystemErr(){
        run("sql", "-u", engine.uri().toString(), "-t", templateFile, "-driver", DRIVER, "-location", URL, "-user", USER, "-q", query, "-k", keyspace.getValue());
        assertThat(sysErr.getLog(), containsString("No password specified (-pass)"));
    }

    @Test
    public void sqlMigratorCalledWithoutSQLConnectionURL_ErrorIsPrintedToSystemErr(){
        run("sql", "-u", engine.uri().toString(), "-driver", DRIVER, "-q", query, "-t", templateFile);
        assertThat(sysErr.getLog(), containsString("No db specified (-location)"));
    }

    @Test
    public void sqlMigratorCalledWithoutSQLQuery_ErrorIsPrintedToSystemErr(){
        run("sql", "-u", engine.uri().toString(), "-t", templateFile, "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", keyspace.getValue());
        assertThat(sysErr.getLog(), containsString("No SQL query specified (-query)"));

    }

    @Test
    public void sqlMigratorCalledWithoutTemplate_ErrorIsPrintedToSystemErr(){
        run("sql", "-u", engine.uri().toString(), "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query);
        assertThat(sysErr.getLog(), containsString("Template file missing (-t)"));
    }

    @Test
    public void sqlMigratorCalledWithTemplateThatDoesntExist_ErrorIsPrintedToSystemErr(){
        run("sql", "-u", engine.uri().toString(), "-t", templateFile + "wrong", "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query);
        assertThat(sysErr.getLog(), containsString("Cannot find file"));
    }

    @Test
    public void sqlMigratorCalledWithPropertiesThatDoesntExist_ErrorIsPrintedToSystemErr(){
        run("sql", "-u", engine.uri().toString(), "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", keyspace.getValue(),
                "-c", "randomDoesNotExistFile");
        assertThat(sysErr.getLog(), containsString("Could not find configuration file randomDoesNotExistFile"));
    }

    @Test
    public void sqlMigratorCalledWithPropertiesFile_PetDataMigratedCorrectly() throws SQLException {
        connection.close();
        connection = setupExample(factory, "pokemon");

        String configurationFile = getFile("sql", "pokemon/migration.yaml").getAbsolutePath();

        run("sql", "-u", engine.uri().toString(), "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", keyspace.getValue(),
                "-c", configurationFile);

        assertPokemonGraphCorrect(factory);
    }

    private void run(String... args){
        SQLMigrator.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);
        assertPetGraphCorrect(factory);
    }
}
