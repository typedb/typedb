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

        assertResourceTypeRelation("ID", ResourceType.DataType.LONG, type);
        assertResourceTypeRelation("NAME", ResourceType.DataType.STRING, type);
        assertResourceTypeRelation("EMAIL", ResourceType.DataType.STRING, type);
    }

    @Test
    public void alterTableTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("alter-table");
        migrator.configure(connection).migrate(loader);

        Type cart = graph.getEntityType("CART");
        assertNotNull(cart);

        assertResourceTypeRelation("ID", ResourceType.DataType.LONG, cart);
        assertResourceTypeRelation("NAME", ResourceType.DataType.STRING, cart);

        Type cartItem = graph.getEntityType("CART_ITEM");

        assertResourceTypeRelation("ITEM_QTY", ResourceType.DataType.LONG, cartItem);
        assertResourceTypeRelation("LAST_UPDATED", ResourceType.DataType.STRING, cartItem);

        Type category = graph.getEntityType("CATEGORY");
        assertResourceTypeRelation("ID", ResourceType.DataType.LONG, category);
        assertResourceTypeRelation("DESCRIPTION", ResourceType.DataType.STRING, category);
        assertResourceTypeRelation("NAME", ResourceType.DataType.STRING, category);

        Type customer = graph.getEntityType("CUSTOMER");
        assertResourceTypeRelation("ID", ResourceType.DataType.LONG, customer);
        assertResourceTypeRelation("NAME", ResourceType.DataType.STRING, customer);
        assertResourceTypeRelation("PASSWORD", ResourceType.DataType.STRING, customer);
        assertResourceTypeRelation("LAST_UPDATED", ResourceType.DataType.STRING, customer);
        assertResourceTypeRelation("REGISTRATION_DATE", ResourceType.DataType.STRING, customer);

        Type product = graph.getEntityType("PRODUCT");

        assertRelationBetweenTypes(cart, customer, "CUSTOMER_ID");
        assertRelationBetweenTypes(cartItem, cart, "CART_ID");
        assertRelationBetweenTypes(cartItem, product, "PRODUCT_ID");
        assertRelationBetweenTypes(product, category, "CATEGORY_ID");
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
        assertResourceTypeRelation("ID", ResourceType.DataType.LONG, entity);
        assertResourceTypeRelation("DATE", ResourceType.DataType.STRING, entity);
        assertResourceTypeRelation("CASE_ID", ResourceType.DataType.STRING, entity);
        assertResourceTypeRelation("DETAILS", ResourceType.DataType.STRING, entity);
        assertResourceTypeRelation("SOURCE_URL", ResourceType.DataType.STRING, entity);
    }

    @Test
    public void postgresSchemaTest() throws SQLException, ClassNotFoundException {
        Connection connection = SQLMigratorUtil.setupExample("postgresql-example");
        migrator.configure(connection).migrate(loader);

        Type city = graph.getEntityType("CITY");
        assertNotNull(city);

        assertResourceTypeRelation("ID", ResourceType.DataType.LONG, city);
        assertResourceTypeRelation("NAME", ResourceType.DataType.STRING, city);
        assertResourceTypeRelation("COUNTRYCODE", ResourceType.DataType.STRING, city);
        assertResourceTypeRelation("DISTRICT", ResourceType.DataType.STRING, city);
        assertResourceTypeRelation("POPULATION", ResourceType.DataType.LONG, city);

        Type country = graph.getEntityType("COUNTRY");
        assertNotNull(country);

        Type language = graph.getEntityType("COUNTRYLANGUAGE");
        assertNotNull(language);

        assertRelationBetweenTypes(country, city, "CAPITAL");
        assertRelationBetweenTypes(language, country, "COUNTRYCODE");

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

        assertResourceTypeRelation("NAME", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelation("OWNER", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelation("SPECIES", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelation("SEX", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelation("BIRTH", ResourceType.DataType.STRING, pet);
        assertResourceTypeRelation("DEATH", ResourceType.DataType.STRING, pet);

        assertResourceTypeRelation("NAME", ResourceType.DataType.STRING, event);
        assertResourceTypeRelation("DATE", ResourceType.DataType.STRING, event);
        assertResourceTypeRelation("EVENTTYPE", ResourceType.DataType.STRING, event);
        assertResourceTypeRelation("REMARK", ResourceType.DataType.STRING, event);
    }

    @Test
    public void combinedKeyTest() throws SQLException {
        Connection connection = SQLMigratorUtil.setupExample("combined-key");
        migrator.configure(connection).migrate(loader);

        Type type = graph.getEntityType("USERS");
        assertNotNull(type);

        assertResourceTypeRelation("FIRSTNAME", ResourceType.DataType.STRING, type);
        assertResourceTypeRelation("LASTNAME", ResourceType.DataType.STRING, type);
        assertResourceTypeRelation("EMAIL", ResourceType.DataType.STRING, type);
    }

    public void assertResourceTypeRelation(String name, ResourceType.DataType datatype, Type owner) {
        String resourceName = namer.resourceName(owner.getId(), name);
        assertResourceTypeRelationExists(resourceName, datatype, owner);
    }

    public void assertRelationBetweenTypes(Type type1, Type type2, String relation) {
        String relName = namer.relationName(relation);
        assertRelationBetweenTypesExists(type1, type2, relName);
    }
}
