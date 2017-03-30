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
import org.junit.rules.ExpectedException;

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

public class SQLMigratorMainTest {

    private final String templateFile = getFile("sql", "pets/template.gql").getAbsolutePath();
    private final String query = "SELECT * FROM pet";
    private Connection connection;
    private GraknSession factory;
    private String keyspace;
    private GraknGraph graph;

    @Rule
    public final ExpectedException exception = ExpectedException.none();
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
        runAndAssertDataCorrect("sql", "-t", templateFile,
                "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query, "-k", keyspace);
    }

    @Test
    public void sqlMainNoKeyspace(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Keyspace missing (-k)");
        run("sql", "-pass", PASS, "-location", URL, "-q", query, "-t", templateFile);
    }

    @Test
    public void sqlMainNoUserTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No username specified (-user)");
        run("sql", "-pass", PASS, "-location", URL, "-q", query, "-t", templateFile, "-k", keyspace);
    }

    @Test
    public void sqlMainNoPassTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No password specified (-pass)");
        run("sql", "-t", templateFile, "-driver", DRIVER, "-location", URL, "-user", USER, "-q", query, "-k", keyspace);
    }

    @Test
    public void sqlMainNoURLTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No db specified (-location)");
        run("sql", "-driver", DRIVER, "-q", query, "-t", templateFile);
    }

    @Test
    public void sqlMainNoQueryTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No SQL query specified (-query)");
        run("sql", "-t", templateFile, "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", keyspace);
    }

    @Test
    public void sqlMainNoTemplateTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Template file missing (-t)");
        run("sql", "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query);
    }

    @Test
    public void sqlMainTemplateNoExistTest(){
        exception.expect(RuntimeException.class);
        run("sql", "-t", templateFile + "wrong", "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query);
    }

    @Test
    public void sqlMainPropertiesTest() throws SQLException {
        connection.close();
        graph.close();
        connection = setupExample(factory, "pokemon");

        String configurationFile = getFile("sql", "pokemon/migration.yaml").getAbsolutePath();

        run("sql", "-driver", DRIVER, "-location", URL,
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
