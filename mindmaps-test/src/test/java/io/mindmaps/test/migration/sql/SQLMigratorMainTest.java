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
 * along with MindmapsDB. If sqlMainNot, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.migration.sql;

import io.mindmaps.migration.sql.Main;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class SQLMigratorMainTest extends SQLMigratorTestBase {

    private final String templateFile = getFile("sql", "pets/template.gql").getAbsolutePath();
    private final String query = "SELECT * FROM pet";
    private Connection connection;

    @Before
    public void setup() throws SQLException {
        connection = setupExample("pets");
    }

    @After
    public void shutdown() throws SQLException {
        connection.close();
    }

    @Test
    public void sqlMainTest(){
        exit.expectSystemExitWithStatus(0);
        runAndAssertDataCorrect("sql", "-t", templateFile,
                "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query, "-k", graph.getKeyspace());
    }

    @Test
    public void sqlMainNoDriverTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No driver specified (-driver)");
        run("sql", "-pass", PASS);
    }

    @Test
    public void sqlMainNoUserTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No username specified (-user)");
        run("sql", "-driver", DRIVER, "-location", URL);
    }

    @Test
    public void sqlMainNoPassTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No password specified (-pass)");
        run("sql", "-driver", DRIVER, "-location", URL, "-user", USER);
    }

    @Test
    public void sqlMainNoURLTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No db specified (-location)");
        run("sql", "-driver", DRIVER);
    }

    @Test
    public void sqlMainNoQueryTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No SQL query specified (-query)");
        run("sql", "-t", templateFile, "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", graph.getKeyspace());
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

    private void run(String... args){
        Main.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        exit.checkAssertionAfterwards(this::assertPetGraphCorrect);
        run(args);
    }

}
