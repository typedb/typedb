package io.mindmaps.migration.sql;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.Data;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.ResourceType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.engine.controller.CommitLogController;
import io.mindmaps.engine.controller.GraphFactoryController;
import io.mindmaps.engine.controller.TransactionController;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

        assertResourceRelationExists("ID", Data.LONG, type);
        assertResourceRelationExists("NAME", Data.STRING, type);
        assertResourceRelationExists("EMAIL", Data.STRING, type);
    }

    @Test
    public void alterTableTest() throws SQLException {
        Connection connection = Util.setupExample("alter-table");
        migrator.configure(connection).migrate(loader);
        graph.refresh();

        Type cart = graph.getEntityType("CART");
        assertNotNull(cart);

        assertResourceRelationExists("ID", Data.LONG, cart);
        assertResourceRelationExists("NAME", Data.STRING, cart);

        Type cartItem = graph.getEntityType("CART_ITEM");

        assertResourceRelationExists("ITEM_QTY", Data.LONG, cartItem);
        assertResourceRelationExists("LAST_UPDATED", Data.STRING, cartItem);

        Type category = graph.getEntityType("CATEGORY");
        assertResourceRelationExists("ID", Data.LONG, category);
        assertResourceRelationExists("DESCRIPTION", Data.STRING, category);
        assertResourceRelationExists("NAME", Data.STRING, category);

        Type customer = graph.getEntityType("CUSTOMER");
        assertResourceRelationExists("ID", Data.LONG, customer);
        assertResourceRelationExists("NAME", Data.STRING, customer);
        assertResourceRelationExists("PASSWORD", Data.STRING, customer);
        assertResourceRelationExists("LAST_UPDATED", Data.STRING, customer);
        assertResourceRelationExists("REGISTRATION_DATE", Data.STRING, customer);

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
        graph.refresh();

        System.out.println(graph.getMetaType().instances());
        assertTrue(graph.getMetaType().instances().size() == 8);
        assertTrue(graph.getMetaEntityType().instances().size() == 0);
        assertTrue(graph.getMetaRelationType().instances().size() == 0);
        assertTrue(graph.getMetaResourceType().instances().size() == 0);
        assertTrue(graph.getMetaRoleType().instances().size() == 0);
        assertTrue(graph.getMetaRuleType().instances().size() == 2);
    }

    @Test
    public void datavaultSchemaTest() throws SQLException {
        Connection connection = Util.setupExample("datavault");
        migrator.configure(connection).migrate(loader);
        graph.refresh();

        Type entity = graph.getEntityType("AZ_BAKUAPPEALCOURT_CASES");
        assertNotNull(entity);
        assertResourceRelationExists("ID", Data.LONG, entity);
        assertResourceRelationExists("DATE", Data.STRING, entity);
        assertResourceRelationExists("CASE_ID", Data.STRING, entity);
        assertResourceRelationExists("DETAILS", Data.STRING, entity);
        assertResourceRelationExists("SOURCE_URL", Data.STRING, entity);
    }

    @Test
    public void postgresSchemaTest() throws SQLException, ClassNotFoundException {
        Connection connection = Util.setupExample("postgresql-example");
        migrator.configure(connection).migrate(loader);
        graph.refresh();

        Type city = graph.getEntityType("CITY");
        assertNotNull(city);

        assertResourceRelationExists("ID", Data.LONG, city);
        assertResourceRelationExists("NAME", Data.STRING, city);
        assertResourceRelationExists("COUNTRYCODE", Data.STRING, city);
        assertResourceRelationExists("DISTRICT", Data.STRING, city);
        assertResourceRelationExists("POPULATION", Data.LONG, city);

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
        graph.refresh();

        System.out.println(graph.getMetaEntityType().instances());
        Type pet = graph.getEntityType("PET");
        Type event = graph.getEntityType("EVENT");
        assertNotNull(pet);
        assertNotNull(event);

        assertResourceRelationExists("NAME", Data.STRING, pet);
        assertResourceRelationExists("OWNER", Data.STRING, pet);
        assertResourceRelationExists("SPECIES", Data.STRING, pet);
        assertResourceRelationExists("SEX", Data.STRING, pet);
        assertResourceRelationExists("BIRTH", Data.STRING, pet);
        assertResourceRelationExists("DEATH", Data.STRING, pet);

        assertResourceRelationExists("NAME", Data.STRING, event);
        assertResourceRelationExists("DATE", Data.STRING, event);
        assertResourceRelationExists("EVENTTYPE", Data.STRING, event);
        assertResourceRelationExists("REMARK", Data.STRING, event);
    }

    @Test
    public void combinedKeyTest() throws SQLException {
        Connection connection = Util.setupExample("combined-key");
        migrator.configure(connection).migrate(loader);
        graph.refresh();

        Type type = graph.getEntityType("USERS");
        assertNotNull(type);

        assertResourceRelationExists("FIRSTNAME", Data.STRING, type);
        assertResourceRelationExists("LASTNAME", Data.STRING, type);
        assertResourceRelationExists("EMAIL", Data.STRING, type);
    }

    private ResourceType assertResourceExists(String name, Data datatype) {
        ResourceType resourceType = graph.getResourceType(name);
        assertNotNull(resourceType);
        assertEquals(datatype.getName(), resourceType.getDataType().getName());
        return resourceType;
    }

    private void assertResourceRelationExists(String name, Data datatype, Type owner){
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
