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

package io.grakn.migration.sql;

import io.grakn.GraknGraph;
import io.grakn.concept.Entity;
import io.grakn.engine.MindmapsEngineServer;
import io.grakn.engine.util.ConfigProperties;
import io.grakn.factory.GraphFactory;
import org.junit.*;

import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SQLMigratorMainTest {

    private Namer namer = new Namer() {};
    private GraknGraph graph;

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        MindmapsEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        MindmapsEngineServer.stop();
    }

    @Before
    public void setup() throws SQLException {
        String graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        graph = GraphFactory.getInstance().getGraphBatchLoading(graphName);
        Util.setupExample("simple");
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test(expected = RuntimeException.class)
    public void sqlMainUnknownOptionTest(){
        runAndAssertDataCorrect(new String[]{"sql", "whale"});
    }

    @Test(expected = RuntimeException.class)
    public void sqlMainMissingDriverTest(){
        runAndAssertDataCorrect(new String[]{"sql"});
    }

    @Test(expected = RuntimeException.class)
    public void sqlMainMissingURLTest(){
        runAndAssertDataCorrect(new String[]{"-driver", Util.DRIVER});
    }

    @Test(expected = RuntimeException.class)
    public void sqlMainMissingUserTest(){
        runAndAssertDataCorrect(new String[]{"-driver", Util.DRIVER, "-database", Util.URL});
    }

    @Test(expected = RuntimeException.class)
    public void sqlMainMissingPassTest(){
        runAndAssertDataCorrect(new String[]{"-driver", Util.DRIVER, "-database", Util.URL, "-user", Util.USER});
    }

    @Test
    public void sqlMainDifferentGraphNameTest(){
        String name = "different";
        graph = GraphFactory.getInstance().getGraphBatchLoading(name);
        runAndAssertDataCorrect(new String[]{"-driver", Util.DRIVER, "-database", Util.URL, "-user", Util.USER, "-pass", Util.PASS,
                                             "-graph", name});
    }

    @Test
    public void sqlMainDistributedLoaderTest(){
        runAndAssertDataCorrect(new String[]{"-driver", Util.DRIVER, "-database", Util.URL, "-user", Util.USER, "-pass", Util.PASS,
                                             "-engine", "0.0.0.0"});
    }

    @Test(expected = RuntimeException.class)
    public void sqlMainThrowableTest(){
        runAndAssertDataCorrect(new String[]{"-driver", Util.DRIVER, "-database", Util.URL, "-user", "none", "-pass", Util.PASS,});
    }

    private void runAndAssertDataCorrect(String[] args){
        Main.main(args);

        Entity alex = graph.getEntity("USERS-2");
        assertNotNull(alex);

        assertResourceRelationExists("NAME", "alex", alex, "USERS");
        assertResourceRelationExists("EMAIL", "alex@yahoo.com", alex, "USERS");
        assertResourceRelationExists("ID", 2L, alex, "USERS");

        Entity alexandra = graph.getEntity("USERS-4");
        assertNotNull(alexandra);

        assertResourceRelationExists("NAME", "alexandra", alexandra, "USERS");
        assertResourceRelationExists("ID", 4L, alexandra, "USERS");
    }

    private void assertResourceRelationExists(String type, Object value, Entity owner, String tableName){
        assertTrue(owner.resources().stream().anyMatch(resource ->
                resource.type().getId().equals(namer.resourceName(tableName, type)) &&
                        resource.getValue().equals(value)));
    }
}
