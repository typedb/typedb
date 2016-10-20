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

import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.migration.sql.Namer;
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

public class SQLSchemaMigratorTest extends AbstractMindmapsMigratorTest {

    private static Namer namer = new Namer(){};

    private static BlockingLoader loader;
    private static SQLSchemaMigrator migrator;

    @BeforeClass
    public static void start(){
        migrator = new SQLSchemaMigrator();
    }

    @AfterClass
    public static void stop() {
        migrator.close();
    }

    @Before
    public void setup(){
        loader = new BlockingLoader(graph.getKeyspace());
    }

    @Test
    public void usersTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("simple");
        migrator.configure(connection).migrate(loader);

        Type type = graph.getEntityType("USERS");
        assertNotNull(type);

        assertResourceTypeRelationExists("ID", ResourceType.DataType.LONG, type);
        assertResourceTypeRelationExists("NAME", ResourceType.DataType.STRING, type);
        assertResourceTypeRelationExists("EMAIL", ResourceType.DataType.STRING, type);
    }

    @Test
    public void alterTableTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("alter-table");
        migrator.configure(connection).migrate(loader);

        Type cart = graph.getEntityType("CART");
        assertNotNull(cart);

        assertResourceTypeRelationExists("ID", ResourceType.DataType.LONG, cart);
        assertResourceTypeRelationExists("NAME", ResourceType.DataType.STRING, cart);

        Type cartItem = graph.getEntityType("CART_ITEM");

        assertResourceTypeRelationExists("ITEM_QTY", ResourceType.DataType.LONG, cartItem);
        assertResourceTypeRelationExists("LAST_UPDATED", ResourceType.DataType.STRING, cartItem);

        Type category = graph.getEntityType("CATEGORY");
        assertResourceTypeRelationExists("ID", ResourceType.DataType.LONG, category);
        assertResourceTypeRelationExists("DESCRIPTION", ResourceType.DataType.STRING, category);
        assertResourceTypeRelationExists("NAME", ResourceType.DataType.STRING, category);

        Type customer = graph.getEntityType("CUSTOMER");
        assertResourceTypeRelationExists("ID", ResourceType.DataType.LONG, customer);
        assertResourceTypeRelationExists("NAME", ResourceType.DataType.STRING, customer);
        assertResourceTypeRelationExists("PASSWORD", ResourceType.DataType.STRING, customer);
        assertResourceTypeRelationExists("LAST_UPDATED", ResourceType.DataType.STRING, customer);
        assertResourceTypeRelationExists("REGISTRATION_DATE", ResourceType.DataType.STRING, customer);

        Type product = graph.getEntityType("PRODUCT");

        assertRelationBetweenTypesExists(cart, customer, "CUSTOMER_ID");
        assertRelationBetweenTypesExists(cartItem, cart, "CART_ID");
        assertRelationBetweenTypesExists(cartItem, product, "PRODUCT_ID");
        assertRelationBetweenTypesExists(product, category, "CATEGORY_ID");
    }

    @Test
    public void emptyTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("empty");
        migrator.configure(connection).migrate(loader);

        System.out.println(graph.getMetaType().instances());
        assertEquals(2, graph.getMetaType().instances().size());
        assertEquals(0, graph.getMetaEntityType().instances().size());
        assertEquals(0, graph.getMetaRelationType().instances().size());
        assertEquals(0, graph.getMetaResourceType().instances().size());
        assertEquals(0, graph.getMetaRoleType().instances().size());
        assertEquals(2, graph.getMetaRuleType().instances().size());
    }

    @Test
    public void datavaultSchemaTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("datavault");
        migrator.configure(connection).migrate(loader);

        Type entity = graph.getEntityType("AZ_BAKUAPPEALCOURT_CASES");
        assertNotNull(entity);
        assertResourceTypeRelationExists("ID", ResourceType.DataType.LONG, entity);
        assertResourceTypeRelationExists("DATE", ResourceType.DataType.STRING, entity);
        assertResourceTypeRelationExists("CASE_ID", ResourceType.DataType.STRING, entity);
        assertResourceTypeRelationExists("DETAILS", ResourceType.DataType.STRING, entity);
        assertResourceTypeRelationExists("SOURCE_URL", ResourceType.DataType.STRING, entity);
    }

    @Test
    public void postgresSchemaTest() throws SQLException, ClassNotFoundException {
        Connection connection = SQLMigratorUtil.setupExample("postgresql-example");
        migrator.configure(connection).migrate(loader);

        Type city = graph.getEntityType("CITY");
        assertNotNull(city);

        assertResourceTypeRelationExists("ID", ResourceType.DataType.LONG, city);
        assertResourceTypeRelationExists("NAME", ResourceType.DataType.STRING, city);
        assertResourceTypeRelationExists("COUNTRYCODE", ResourceType.DataType.STRING, city);
        assertResourceTypeRelationExists("DISTRICT", ResourceType.DataType.STRING, city);
        assertResourceTypeRelationExists("POPULATION", ResourceType.DataType.LONG, city);

        Type country = graph.getEntityType("COUNTRY");
        assertNotNull(country);

        Type language = graph.getEntityType("COUNTRYLANGUAGE");
        assertNotNull(language);

        assertRelationBetweenTypesExists(country, city, "CAPITAL");
        assertRelationBetweenTypesExists(language, country, "COUNTRYCODE");

        RoleType countryCodeChild = graph.getRoleType("COUNTRYCODE-child");
        assertTrue(country.playsRoles().contains(countryCodeChild));
    }

    @Test
    public void mysqlSchemaTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("mysql-example");
        migrator.configure(connection).migrate(loader);

        System.out.println(graph.getMetaEntityType().instances());
        Type pet = graph.getEntityType("PET");
        Type event = graph.getEntityType("EVENT");
        assertNotNull(pet);
        assertNotNull(event);

        assertResourceTypeRelationExists("NAME", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelationExists("OWNER", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelationExists("SPECIES", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelationExists("SEX", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelationExists("BIRTH", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelationExists("DEATH", ResourceType.DataType.STRING, pet);

        assertResourceTypeRelationExists("NAME", ResourceType.DataType.STRING, event);
        assertResourceTypeRelationExists("DATE", ResourceType.DataType.STRING, event);
        assertResourceTypeRelationExists("EVENTTYPE", ResourceType.DataType.STRING, event);
        assertResourceTypeRelationExists("REMARK", ResourceType.DataType.STRING, event);
    }

    @Test
    public void combinedKeyTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("combined-key");
        migrator.configure(connection).migrate(loader);

        Type type = graph.getEntityType("USERS");
        assertNotNull(type);

        assertResourceTypeRelationExists("FIRSTNAME", ResourceType.DataType.STRING, type);
        assertResourceTypeRelationExists("LASTNAME", ResourceType.DataType.STRING, type);
        assertResourceTypeRelationExists("EMAIL", ResourceType.DataType.STRING, type);
    }

    public static void assertResourceTypeRelationExists(String name, ResourceType.DataType datatype, Type owner) {
        String resourceName = namer.resourceName(owner.getId(), name);
        AbstractMindmapsMigratorTest.assertResourceTypeRelationExists(resourceName, datatype, owner);
    }

    public static void assertRelationBetweenTypesExists(Type type1, Type type2, String relation) {
        String relName = namer.relationName(relation);
        AbstractMindmapsMigratorTest.assertRelationBetweenTypesExists(type1, type2, relName);
    }
}
