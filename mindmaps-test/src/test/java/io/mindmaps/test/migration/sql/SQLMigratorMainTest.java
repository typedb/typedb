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

package io.mindmaps.test.migration.sql;

import io.mindmaps.concept.Entity;
import io.mindmaps.migration.sql.Main;
import io.mindmaps.test.migration.AbstractMindmapsMigratorTest;
import org.junit.*;

import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;

public class SQLMigratorMainTest extends AbstractMindmapsMigratorTest {

    @BeforeClass
    public static void setup() throws SQLException {
        SQLMigratorUtil.setupExample("simple");
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void sqlMainMissingDriverTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No driver specified (-driver)");
        runAndAssertDataCorrect(new String[]{"sql"});
    }

    @Test
    public void sqlMainMissingURLTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No db specified (-database)");
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER});
    }

    @Test
    public void sqlMainMissingUserTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No username specified (-user)");
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL});
    }

    @Test
    public void sqlMainMissingPassTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No password specified (-pass)");
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL, "-user", SQLMigratorUtil.USER});
    }

    @Test
    public void unknownArgumentTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Unrecognized option: -whale");
        runAndAssertDataCorrect(new String[]{ "-whale", ""});
    }

    @Test
    public void sqlMainDifferentGraphNameTest(){
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL, "-user", SQLMigratorUtil.USER, "-pass", SQLMigratorUtil.PASS,
                                             "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void sqlMainDistributedLoaderTest(){
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL, "-user", SQLMigratorUtil.USER, "-pass", SQLMigratorUtil.PASS,
                                             "-uri", "localhost:4567", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void sqlMainThrowableTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Wrong user name or password [28000-192]");
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL, "-user", "none", "-pass", SQLMigratorUtil.PASS,});
    }

    private void runAndAssertDataCorrect(String[] args){
        Main.main(args);

        Entity alex = graph.getEntity("USERS-2");
        assertNotNull(alex);

        assertResourceEntityRelationExists("NAME", "alex", alex);
        assertResourceEntityRelationExists("EMAIL", "alex@yahoo.com", alex);
        assertResourceEntityRelationExists("ID", 2L, alex);

        Entity alexandra = graph.getEntity("USERS-4");
        assertNotNull(alexandra);

        assertResourceEntityRelationExists("NAME", "alexandra", alexandra);
        assertResourceEntityRelationExists("ID", 4L, alexandra);
    }
}
