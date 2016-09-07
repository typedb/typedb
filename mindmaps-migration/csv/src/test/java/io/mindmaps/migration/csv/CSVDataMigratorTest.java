package io.mindmaps.migration.csv;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.model.Entity;
import io.mindmaps.engine.controller.CommitLogController;
import io.mindmaps.engine.controller.GraphFactoryController;
import io.mindmaps.engine.controller.TransactionController;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Graql;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.mindmaps.graql.Graql.var;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CSVDataMigratorTest {

    private String GRAPH_NAME;

    private MindmapsGraph graph;
    private BlockingLoader loader;

    private static CSVSchemaMigrator schemaMigrator;
    private static CSVDataMigrator dataMigrator;

    @BeforeClass
    public static void start(){
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);

        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        new TransactionController();
        new CommitLogController();
        new GraphFactoryController();

        schemaMigrator = new CSVSchemaMigrator();
        dataMigrator = new CSVDataMigrator();
    }

    @Before
    public void setup(){
        GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

        loader = new BlockingLoader(GRAPH_NAME);
        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void icijEntityDataTest() throws IOException {

        schemaMigrator.configure("entity", parser("icij/Entities.csv")).migrate(loader);
        dataMigrator.configure("entity", parser("icij/Entities.csv")).migrate(loader);

        // test a entity
        Entity thing = Graql.withGraph(graph).match(var("x").isa("entity")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("name-resource", thing);
        assertResourceRelationExists("country_codes-resource", thing);
    }

    @Test
    public void icijAddressDataTest() throws IOException {
        schemaMigrator.configure("address", parser("icij/Addresses.csv")).migrate(loader);
        dataMigrator.configure("address", parser("icij/Addresses.csv")).migrate(loader);

        // test an address
        Entity thing = Graql.withGraph(graph).match(var("x").isa("address")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("valid_until-resource", thing);
        assertResourceRelationExists("countries-resource", thing);
    }

    @Test
    public void icijOfficerDataTest() throws IOException {
        schemaMigrator.configure("officer", parser("icij/Officers.csv")).migrate(loader);
        dataMigrator.configure("officer", parser("icij/Officers.csv")).migrate(loader);

//        // test an officer
        Entity thing = Graql.withGraph(graph).match(var("x").isa("officer")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("valid_until-resource", thing);
        assertResourceRelationExists("country_codes-resource", thing);
    }

    @Test
    public void icijIntermediaryDataTest() throws IOException {
        schemaMigrator.configure("intermediary", parser("icij/Intermediaries.csv")).migrate(loader);
        dataMigrator.configure("intermediary", parser("icij/Intermediaries.csv")).migrate(loader);

        // test an intermediary
        Entity thing = Graql.withGraph(graph).match(var("x").isa("intermediary")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("countries-resource", thing);
        assertResourceRelationExists("status-resource", thing);
    }

    private void assertResourceRelationExists(String type, Entity owner){
        System.out.println(owner);
        System.out.println(owner.resources());
        assertTrue(owner.resources().stream().anyMatch(resource ->
                resource.type().getId().equals(type)));
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
