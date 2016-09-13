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
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
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

public class SQLSchemaMigratorTest {

    private static final String GRAPH_NAME = "test";

    private MindmapsGraph graph;
    private BlockingLoader loader;

    private Namer namer = new Namer() {};
    private static SQLSchemaMigrator migrator;

    @BeforeClass
    public static void start(){
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);

        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        new TransactionController();
        new CommitLogController();
        new GraphFactoryController();

        migrator = new SQLSchemaMigrator();
    }

    @Before
    public void setup() throws MindmapsValidationException {
        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
        loader = new BlockingLoader(GRAPH_NAME);
    }

    @After
    public void shutdown() throws SQLException {
        graph.clear();
        migrator.close();
    }

    @Test
    public void usersTest() throws SQLException {
        Connection connection = Util.setupExample("simple");
        migrator.configure(connection).migrate(loader);

        Type type = graph.getEntityType("USERS");
        assertNotNull(type);

        assertResourceRelationExists("ID", ResourceType.DataType.LONG, type);
        assertResourceRelationExists("NAME", ResourceType.DataType.STRING, type);
        assertResourceRelationExists("EMAIL", ResourceType.DataType.STRING, type);
    }

    @Test
    public void alterTableTest() throws SQLException {
        Connection connection = Util.setupExample("alter-table");
        migrator.configure(connection).migrate(loader);
        graph.rollback();

        Type cart = graph.getEntityType("CART");
        assertNotNull(cart);

        assertResourceRelationExists("ID", ResourceType.DataType.LONG, cart);
        assertResourceRelationExists("NAME", ResourceType.DataType.STRING, cart);

        Type cartItem = graph.getEntityType("CART_ITEM");

        assertResourceRelationExists("ITEM_QTY", ResourceType.DataType.LONG, cartItem);
        assertResourceRelationExists("LAST_UPDATED", ResourceType.DataType.STRING, cartItem);

        Type category = graph.getEntityType("CATEGORY");
        assertResourceRelationExists("ID", ResourceType.DataType.LONG, category);
        assertResourceRelationExists("DESCRIPTION", ResourceType.DataType.STRING, category);
        assertResourceRelationExists("NAME", ResourceType.DataType.STRING, category);

        Type customer = graph.getEntityType("CUSTOMER");
        assertResourceRelationExists("ID", ResourceType.DataType.LONG, customer);
        assertResourceRelationExists("NAME", ResourceType.DataType.STRING, customer);
        assertResourceRelationExists("PASSWORD", ResourceType.DataType.STRING, customer);
        assertResourceRelationExists("LAST_UPDATED", ResourceType.DataType.STRING, customer);
        assertResourceRelationExists("REGISTRATION_DATE", ResourceType.DataType.STRING, customer);

        Type product = graph.getEntityType("PRODUCT");

        assertRelationExists(cart, customer, "CUSTOMER_ID");
        assertRelationExists(cartItem, cart, "CART_ID");
        assertRelationExists(cartItem, product, "PRODUCT_ID");
        assertRelationExists(product, category, "CATEGORY_ID");
    }

    @Test
    public void emptyTest() throws SQLException {
        Connection connection = Util.setupExample("empty");
        migrator.configure(connection).migrate(loader);
        graph.rollback();

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
        Connection connection = Util.setupExample("datavault");
        migrator.configure(connection).migrate(loader);
        graph.rollback();

        Type entity = graph.getEntityType("AZ_BAKUAPPEALCOURT_CASES");
        assertNotNull(entity);
        assertResourceRelationExists("ID", ResourceType.DataType.LONG, entity);
        assertResourceRelationExists("DATE", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("CASE_ID", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("DETAILS", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("SOURCE_URL", ResourceType.DataType.STRING, entity);
    }

    @Test
    public void postgresSchemaTest() throws SQLException, ClassNotFoundException {
        Connection connection = Util.setupExample("postgresql-example");
        migrator.configure(connection).migrate(loader);
        graph.rollback();

        Type city = graph.getEntityType("CITY");
        assertNotNull(city);

        assertResourceRelationExists("ID", ResourceType.DataType.LONG, city);
        assertResourceRelationExists("NAME", ResourceType.DataType.STRING, city);
        assertResourceRelationExists("COUNTRYCODE", ResourceType.DataType.STRING, city);
        assertResourceRelationExists("DISTRICT", ResourceType.DataType.STRING, city);
        assertResourceRelationExists("POPULATION", ResourceType.DataType.LONG, city);

        Type country = graph.getEntityType("COUNTRY");
        assertNotNull(country);

        Type language = graph.getEntityType("COUNTRYLANGUAGE");
        assertNotNull(language);

        assertRelationExists(country, city, "CAPITAL");
        assertRelationExists(language, country, "COUNTRYCODE");

        RoleType countryCodeChild = graph.getRoleType("COUNTRYCODE-child");
        assertTrue(country.playsRoles().contains(countryCodeChild));
    }

    @Test
    public void mysqlSchemaTest() throws SQLException {
        Connection connection = Util.setupExample("mysql-example");
        migrator.configure(connection).migrate(loader);
        graph.rollback();

        System.out.println(graph.getMetaEntityType().instances());
        Type pet = graph.getEntityType("PET");
        Type event = graph.getEntityType("EVENT");
        assertNotNull(pet);
        assertNotNull(event);

        assertResourceRelationExists("NAME", ResourceType.DataType.STRING, pet);
        assertResourceRelationExists("OWNER", ResourceType.DataType.STRING, pet);
        assertResourceRelationExists("SPECIES", ResourceType.DataType.STRING, pet);
        assertResourceRelationExists("SEX", ResourceType.DataType.STRING, pet);
        assertResourceRelationExists("BIRTH", ResourceType.DataType.STRING, pet);
        assertResourceRelationExists("DEATH", ResourceType.DataType.STRING, pet);

        assertResourceRelationExists("NAME", ResourceType.DataType.STRING, event);
        assertResourceRelationExists("DATE", ResourceType.DataType.STRING, event);
        assertResourceRelationExists("EVENTTYPE", ResourceType.DataType.STRING, event);
        assertResourceRelationExists("REMARK", ResourceType.DataType.STRING, event);
    }

    @Test
    public void combinedKeyTest() throws SQLException {
        Connection connection = Util.setupExample("combined-key");
        migrator.configure(connection).migrate(loader);
        graph.rollback();

        Type type = graph.getEntityType("USERS");
        assertNotNull(type);

        assertResourceRelationExists("FIRSTNAME", ResourceType.DataType.STRING, type);
        assertResourceRelationExists("LASTNAME", ResourceType.DataType.STRING, type);
        assertResourceRelationExists("EMAIL", ResourceType.DataType.STRING, type);
    }

    private ResourceType assertResourceExists(String name, ResourceType.DataType datatype) {
        ResourceType resourceType = graph.getResourceType(name);
        assertNotNull(resourceType);
        assertEquals(datatype.getName(), resourceType.getDataType().getName());
        return resourceType;
    }

    private void assertResourceRelationExists(String name, ResourceType.DataType datatype, Type owner){
        String resourceName = namer.resourceName(owner.getId(), name);
        ResourceType resource = assertResourceExists(resourceName, datatype);

        RelationType relationType = graph.getRelationType("has-" + resourceName);
        RoleType roleOwner = graph.getRoleType("has-" + resourceName + "-owner");
        RoleType roleOther = graph.getRoleType("has-" + resourceName + "-value");

        assertNotNull(relationType);
        assertNotNull(roleOwner);
        assertNotNull(roleOther);

        assertEquals(relationType, roleOwner.relationType());
        assertEquals(relationType, roleOther.relationType());

        assertTrue(owner.playsRoles().contains(roleOwner));
        assertTrue(resource.playsRoles().contains(roleOther));
    }

    private void assertRelationExists(Type owner, Type other, String relation) {

        RelationType relationType = graph.getRelationType(relation + "-relation");
        RoleType roleOwner = graph.getRoleType(relation + "-parent");
        RoleType roleOther = graph.getRoleType(relation + "-child");

        assertNotNull(relationType);
        assertNotNull(roleOwner);
        assertNotNull(roleOther);

        assertEquals(relationType, roleOwner.relationType());
        assertEquals(relationType, roleOther.relationType());

        assertTrue(owner.playsRoles().contains(roleOwner));
        assertTrue(other.playsRoles().contains(roleOther));
    }
}
