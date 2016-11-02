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

import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.migration.sql.Namer;
import io.mindmaps.migration.sql.SQLDataMigrator;
import io.mindmaps.migration.sql.SQLSchemaMigrator;
import io.mindmaps.test.migration.AbstractMindmapsMigratorTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SQLDataMigratorTest extends AbstractMindmapsMigratorTest {

    private static Namer namer = new Namer(){};

    private static BlockingLoader loader;
    private static SQLSchemaMigrator schemaMigrator;
    private static SQLDataMigrator dataMigrator;

    @BeforeClass
    public static void start(){
        schemaMigrator = new SQLSchemaMigrator();
        dataMigrator = new SQLDataMigrator();
    }

    @AfterClass
    public static void stop(){
        dataMigrator.close();
        schemaMigrator.close();
    }

    @Before
    public void setup(){
        loader = new BlockingLoader(graph.getKeyspace());
        loader.setExecutorSize(1);
    }

    @Test
    public void usersDataTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("simple");
        schemaMigrator.configure(connection).migrate(loader);
        dataMigrator.configure(connection).migrate(loader);

        Entity alex = graph.getEntity("USERS-2");
        assertNotNull(alex);

        assertResourceEntityRelation("NAME", "alex", alex);
        assertResourceEntityRelation("EMAIL", "alex@yahoo.com", alex);
        assertResourceEntityRelation("ID", 2L, alex);

        Entity alexandra = graph.getEntity("USERS-4");
        assertNotNull(alexandra);

        assertResourceEntityRelation("NAME", "alexandra", alexandra);
        assertResourceEntityRelation("ID", 4L, alexandra);
    }

    @Test(expected = AssertionError.class)
    public void usersDataDoesNotExist() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("simple");
        schemaMigrator.configure(connection).migrate(loader);
        dataMigrator.configure(connection).migrate(loader);

        Entity alexandra = graph.getEntity("USERS-4");
        assertResourceEntityRelation("email", "alexandra@yahoo.com", alexandra);
    }

    @Test
    public void postgresDataTest() throws SQLException, MindmapsValidationException {
        Connection connection = SQLMigratorUtil.setupExample("postgresql-example");
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

        assertRelationBetweenInstances(japan, tokyo, "CAPITAL");
        assertRelationBetweenInstances(japanese, japan, "COUNTRYCODE");
    }

    @Test
    public void combinedKeyDataTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("combined-key");
        schemaMigrator.configure(connection).migrate(loader);
        dataMigrator.configure(connection).migrate(loader);

        assertEquals(graph.getEntityType("USERS").instances().size(), 5);

        Instance orth = graph.getInstance("USERS-alexandraorth");
        Instance louise = graph.getInstance("USERS-alexandralouise");

        assertNotNull(orth);
        assertNotNull(louise);
    }

    public void assertRelationBetweenInstances(Instance instance1, Instance instance2, String relation){
        String relName = namer.relationName(relation);
        assertRelationBetweenInstancesExists(instance1, instance2, relName);
    }

    public void assertResourceEntityRelation(String resourceName, Object resourceValue, Entity owner){
        String resource = namer.resourceName(owner.type().getId(), resourceName);
        assertResourceEntityRelationExists(resource, resourceValue, owner);
    }
}
