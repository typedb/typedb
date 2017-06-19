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

package ai.grakn.test.migration.sql;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.migration.sql.SQLMigrator;
import ai.grakn.test.EngineContext;
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
    private String keyspace;
    private GraknGraph graph;

    @Rule
    public final SystemErrRule sysOut = new SystemErrRule().enableLog();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void setup() throws SQLException {
        factory = engine.factoryWithNewKeyspace();
        connection = setupExample(factory, "pets");
        graph = factory.open(GraknTxType.WRITE);
        keyspace = graph.getKeyspace();
    }

    @After
    public void stop() throws SQLException {
        connection.close();
    }

    @Test
    public void sqlMainTest(){
        graph.close();
        runAndAssertDataCorrect("sql", "-u", engine.uri(), "-t", templateFile,
                "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query, "-k", keyspace);
    }

    @Test
    public void sqlMainNoKeyspace(){
        run("sql", "-u", engine.uri(), "-pass", PASS, "-location", URL, "-q", query, "-t", templateFile, "-user", USER);
        assertThat(sysOut.getLog(), containsString("Keyspace missing (-k)"));
    }

    @Test
    public void sqlMainNoUserTest(){
        run("sql", "-u", engine.uri(), "-pass", PASS, "-location", URL, "-q", query, "-t", templateFile, "-k", keyspace);
        assertThat(sysOut.getLog(), containsString("No username specified (-user)"));
    }

    @Test
    public void sqlMainNoPassTest(){
        run("sql", "-u", engine.uri(), "-t", templateFile, "-driver", DRIVER, "-location", URL, "-user", USER, "-q", query, "-k", keyspace);
        assertThat(sysOut.getLog(), containsString("No password specified (-pass)"));
    }

    @Test
    public void sqlMainNoURLTest(){
        run("sql", "-u", engine.uri(), "-driver", DRIVER, "-q", query, "-t", templateFile);
        assertThat(sysOut.getLog(), containsString("No db specified (-location)"));
    }

    @Test
    public void sqlMainNoQueryTest(){
        run("sql", "-u", engine.uri(), "-t", templateFile, "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", keyspace);
        assertThat(sysOut.getLog(), containsString("No SQL query specified (-query)"));

    }

    @Test
    public void sqlMainNoTemplateTest(){
        run("sql", "-u", engine.uri(), "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query);
        assertThat(sysOut.getLog(), containsString("Template file missing (-t)"));
    }

    @Test
    public void sqlMainTemplateNoExistTest(){
        run("sql", "-u", engine.uri(), "-t", templateFile + "wrong", "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query);
        assertThat(sysOut.getLog(), containsString("Cannot find file"));
    }

    @Test
    public void sqlMainPropertiesTest() throws SQLException {
        connection.close();
        graph.close();
        connection = setupExample(factory, "pokemon");

        String configurationFile = getFile("sql", "pokemon/migration.yaml").getAbsolutePath();

        run("sql", "-u", engine.uri(), "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", keyspace,
                "-c", configurationFile);

        graph = factory.open(GraknTxType.WRITE); //Reopen transaction
        assertPokemonGraphCorrect(graph);
    }

    private void run(String... args){
        SQLMigrator.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);
        graph = factory.open(GraknTxType.WRITE); //Reopen transaction
        assertPetGraphCorrect(graph);
    }

}
