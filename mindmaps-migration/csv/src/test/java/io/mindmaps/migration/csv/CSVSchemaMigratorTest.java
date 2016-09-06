package io.mindmaps.migration.csv;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.Data;
import io.mindmaps.core.model.EntityType;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CSVSchemaMigratorTest {

    private String GRAPH_NAME = "test";

    private MindmapsGraph graph;
    private static CSVSchemaMigrator migrator;
    private BlockingLoader loader;
    private Namer namer = new Namer() {};

    @BeforeClass
    public static void start(){
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);

        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        new TransactionController();
        new CommitLogController();
        new GraphFactoryController();

        migrator = new CSVSchemaMigrator();
    }

    @Before
    public void setup(){
        loader = new BlockingLoader(GRAPH_NAME);
        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void icijEntityTest() throws IOException {
        migrator.configure("entity", parser("icij/Entities.csv"))
                .migrate(loader);

        // Check entity type and resources
        EntityType entity = graph.getEntityType("entity");
        assertNotNull(entity);

        assertResourceRelationExists("name", Data.STRING, entity);
        assertResourceRelationExists("original_name", Data.STRING, entity);
        assertResourceRelationExists("former_name", Data.STRING, entity);
        assertResourceRelationExists("jurisdiction", Data.STRING, entity);
        assertResourceRelationExists("jurisdiction_description", Data.STRING, entity);
        assertResourceRelationExists("company_type", Data.STRING, entity);
        assertResourceRelationExists("address", Data.STRING, entity);
        assertResourceRelationExists("internal_id", Data.STRING, entity);
        assertResourceRelationExists("incorporation_date", Data.STRING, entity);
        assertResourceRelationExists("inactivation_date", Data.STRING, entity);
        assertResourceRelationExists("struck_off_date", Data.STRING, entity);
        assertResourceRelationExists("dorm_date", Data.STRING, entity);
        assertResourceRelationExists("status", Data.STRING, entity);
        assertResourceRelationExists("service_provider", Data.STRING, entity);
        assertResourceRelationExists("ibcRUC", Data.STRING, entity);
        assertResourceRelationExists("country_codes", Data.STRING, entity);
        assertResourceRelationExists("countries", Data.STRING, entity);
        assertResourceRelationExists("note", Data.STRING, entity);
        assertResourceRelationExists("valid_until", Data.STRING, entity);
        assertResourceRelationExists("node_id", Data.STRING, entity);
        assertResourceRelationExists("sourceID", Data.STRING, entity);
    }

    @Test
    public void icijAddressTest() throws IOException {
        migrator.configure("address", parser("icij/Addresses.csv"))
                .migrate(loader);

        // Check address type and resources
        EntityType address = graph.getEntityType("address");
        assertNotNull(address);

        assertResourceRelationExists("icij_id", Data.STRING, address);
        assertResourceRelationExists("valid_until", Data.STRING, address);
        assertResourceRelationExists("country_codes", Data.STRING, address);
        assertResourceRelationExists("countries", Data.STRING, address);
        assertResourceRelationExists("node_id", Data.STRING, address);
        assertResourceRelationExists("sourceID", Data.STRING, address);
    }

    @Test
    public void icijOfficerTest() throws IOException {
        migrator.configure("officer", parser("icij/Officers.csv"))
                .migrate(loader);

        // check officers type and resources
        EntityType officer = graph.getEntityType("officer");
        assertNotNull(officer);

        assertResourceRelationExists("name", Data.STRING, officer);
        assertResourceRelationExists("icij_id", Data.STRING, officer);
        assertResourceRelationExists("valid_until", Data.STRING, officer);
        assertResourceRelationExists("country_codes", Data.STRING, officer);
        assertResourceRelationExists("countries", Data.STRING, officer);
        assertResourceRelationExists("node_id", Data.STRING, officer);
        assertResourceRelationExists("sourceID", Data.STRING, officer);
    }

    @Test
    public void icijIntermediaryTest() throws IOException {
        migrator.configure("intermediary", parser("icij/Intermediaries.csv"))
                .migrate(loader);

        // check intermediaries type and resources
        EntityType intermediary = graph.getEntityType("intermediary");
        assertNotNull(intermediary);

        assertResourceRelationExists("name", Data.STRING, intermediary);
        assertResourceRelationExists("internal_id", Data.STRING, intermediary);
        assertResourceRelationExists("address", Data.STRING, intermediary);
        assertResourceRelationExists("valid_until", Data.STRING, intermediary);
        assertResourceRelationExists("country_codes", Data.STRING, intermediary);
        assertResourceRelationExists("countries", Data.STRING, intermediary);
        assertResourceRelationExists("status", Data.STRING, intermediary);
        assertResourceRelationExists("node_id", Data.STRING, intermediary);
        assertResourceRelationExists("sourceID", Data.STRING, intermediary);
    }

    @Test
    public void icijRelationshipTest() throws IOException {
        migrator.configure("relationship", parser("icij/all_edges.csv"))
                .migrate(loader);

        // check relations were created
        EntityType relationship = graph.getEntityType("relationship");
        assertNotNull(relationship);

        assertResourceRelationExists("node_1", Data.STRING, relationship);
        assertResourceRelationExists("rel_type", Data.STRING, relationship);
        assertResourceRelationExists("node_2", Data.STRING, relationship);
    }

    private ResourceType assertResourceExists(String name, Data datatype) {
        ResourceType resourceType = graph.getResourceType(name);
        assertNotNull(resourceType);
        assertEquals(datatype.getName(), resourceType.getDataType().getName());
        return resourceType;
    }

    private void assertResourceRelationExists(String name, Data datatype, Type owner){
        String resourceName = namer.resourceName(name);
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

    private CSVParser parser(String fileName){
        File file = new File(CSVSchemaMigratorTest.class.getClassLoader().getResource(fileName).getPath());

        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(file.toURI().toURL(),
                    StandardCharsets.UTF_8, CSVFormat.DEFAULT.withHeader());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return csvParser;
    }
}
