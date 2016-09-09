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

package io.mindmaps.migration.sql;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.engine.controller.CommitLogController;
import io.mindmaps.engine.controller.GraphFactoryController;
import io.mindmaps.engine.controller.TransactionController;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SQLDataMigratorTest {

    private String GRAPH_NAME = "test";

    private MindmapsGraph graph;
    private BlockingLoader loader;
    private Namer namer = new Namer() {};

    private static SQLSchemaMigrator schemaMigrator;
    private static SQLDataMigrator dataMigrator;

    @BeforeClass
    public static void start(){
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);

        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        new TransactionController();
        new CommitLogController();
        new GraphFactoryController();

        schemaMigrator = new SQLSchemaMigrator();
        dataMigrator = new SQLDataMigrator();
    }

    @Before
    public void setup(){
        loader = new BlockingLoader(GRAPH_NAME);
        loader.setThreadsNumber(1);
        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
    }

    @After
    public void shutdown() throws SQLException {
        graph.clear();
        dataMigrator.close();
        schemaMigrator.close();
    }

    @Test
    public void usersDataTest() throws SQLException {
        Connection connection = Util.setupExample("simple");
        schemaMigrator.configure(connection).migrate(loader);
        dataMigrator.configure(connection).migrate(loader);

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

    @Test(expected = AssertionError.class)
    public void usersDataDoesNotExist() throws SQLException {
        Connection connection = Util.setupExample("simple");
        schemaMigrator.configure(connection).migrate(loader);
        dataMigrator.configure(connection).migrate(loader);

        Entity alexandra = graph.getEntity("USERS-4");
        assertResourceRelationExists("email", "alexandra@yahoo.com", alexandra, "USERS");
    }

    @Test
    public void postgresDataTest() throws SQLException, MindmapsValidationException {
        Connection connection = Util.setupExample("postgresql-example");
        schemaMigrator.configure(connection).migrate(loader);
        dataMigrator.configure(connection).migrate(loader);

        Type country = graph.getEntityType("COUNTRY");
        RoleType countryCodeChild = graph.getRoleType("COUNTRYCODE-child");
        assertNotNull(country);
        assertNotNull(countryCodeChild);

        assertTrue(country.playsRoles().contains(countryCodeChild));

        Type city = graph.getEntityType("CITY");
        assertNotNull(country);
        assertNotNull(city);

        Entity japan = graph.getEntity("COUNTRY-JPN");
        Entity japanese = graph.getEntity("COUNTRYLANGUAGE-JPNJapanese");
        Entity tokyo = graph.getEntity("CITY-1532");

        assertNotNull(japan);
        assertNotNull(japanese);
        assertNotNull(tokyo);

        assertRelationExists(japan, tokyo, "CAPITAL");
        assertRelationExists(japanese, japan, "COUNTRYCODE");
    }

    @Test
    public void combinedKeyDataTest() throws SQLException {
        Connection connection = Util.setupExample("combined-key");
        schemaMigrator.configure(connection).migrate(loader);
        dataMigrator.configure(connection).migrate(loader);

        System.out.println(graph.getEntityType("USERS").instances());
        assertEquals(graph.getEntityType("USERS").instances().size(), 5);

        Instance orth = graph.getInstance("USERS-alexandraorth");
        Instance louise = graph.getInstance("USERS-alexandralouise");

        assertNotNull(orth);
        assertNotNull(louise);
    }

    private void assertResourceRelationExists(String type, Object value, Entity owner, String tableName){
        assertTrue(owner.resources().stream().anyMatch(resource ->
                resource.type().getId().equals(namer.resourceName(tableName, type)) &&
                        resource.getValue().equals(value)));
    }

    private void assertRelationExists(Entity parent, Entity child, String relName) {
        RoleType parentRole = graph.getRoleType(relName + "-parent");

        assertTrue(parent.relations(parentRole).stream().anyMatch(relation ->
                relation.rolePlayers().values().contains(child)));
    }
}
