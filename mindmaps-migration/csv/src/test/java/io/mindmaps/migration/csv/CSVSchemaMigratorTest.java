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

package io.mindmaps.migration.csv;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
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

        assertResourceRelationExists("name", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("original_name", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("former_name", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("jurisdiction", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("jurisdiction_description", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("company_type", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("address", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("internal_id", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("incorporation_date", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("inactivation_date", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("struck_off_date", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("dorm_date", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("status", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("service_provider", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("ibcRUC", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("country_codes", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("countries", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("note", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("valid_until", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("node_id", ResourceType.DataType.STRING, entity);
        assertResourceRelationExists("sourceID", ResourceType.DataType.STRING, entity);
    }

    @Test
    public void icijAddressTest() throws IOException {
        migrator.configure("address", parser("icij/Addresses.csv"))
                .migrate(loader);

        // Check address type and resources
        EntityType address = graph.getEntityType("address");
        assertNotNull(address);

        assertResourceRelationExists("icij_id", ResourceType.DataType.STRING, address);
        assertResourceRelationExists("valid_until", ResourceType.DataType.STRING, address);
        assertResourceRelationExists("country_codes", ResourceType.DataType.STRING, address);
        assertResourceRelationExists("countries", ResourceType.DataType.STRING, address);
        assertResourceRelationExists("node_id", ResourceType.DataType.STRING, address);
        assertResourceRelationExists("sourceID", ResourceType.DataType.STRING, address);
    }

    @Test
    public void icijOfficerTest() throws IOException {
        migrator.configure("officer", parser("icij/Officers.csv"))
                .migrate(loader);

        // check officers type and resources
        EntityType officer = graph.getEntityType("officer");
        assertNotNull(officer);

        assertResourceRelationExists("name", ResourceType.DataType.STRING, officer);
        assertResourceRelationExists("name", ResourceType.DataType.STRING, officer);
        assertResourceRelationExists("icij_id", ResourceType.DataType.STRING, officer);
        assertResourceRelationExists("valid_until", ResourceType.DataType.STRING, officer);
        assertResourceRelationExists("country_codes", ResourceType.DataType.STRING, officer);
        assertResourceRelationExists("countries", ResourceType.DataType.STRING, officer);
        assertResourceRelationExists("node_id", ResourceType.DataType.STRING, officer);
        assertResourceRelationExists("sourceID", ResourceType.DataType.STRING, officer);
    }

    @Test
    public void icijIntermediaryTest() throws IOException {
        migrator.configure("intermediary", parser("icij/Intermediaries.csv"))
                .migrate(loader);

        // check intermediaries type and resources
        EntityType intermediary = graph.getEntityType("intermediary");
        assertNotNull(intermediary);

        assertResourceRelationExists("name", ResourceType.DataType.STRING, intermediary);
        assertResourceRelationExists("internal_id", ResourceType.DataType.STRING, intermediary);
        assertResourceRelationExists("address", ResourceType.DataType.STRING, intermediary);
        assertResourceRelationExists("valid_until", ResourceType.DataType.STRING, intermediary);
        assertResourceRelationExists("country_codes", ResourceType.DataType.STRING, intermediary);
        assertResourceRelationExists("countries", ResourceType.DataType.STRING, intermediary);
        assertResourceRelationExists("status", ResourceType.DataType.STRING, intermediary);
        assertResourceRelationExists("node_id", ResourceType.DataType.STRING, intermediary);
        assertResourceRelationExists("sourceID", ResourceType.DataType.STRING, intermediary);
    }

    @Test
    public void icijRelationshipTest() throws IOException {
        migrator.configure("relationship", parser("icij/all_edges.csv"))
                .migrate(loader);

        // check relations were created
        EntityType relationship = graph.getEntityType("relationship");
        assertNotNull(relationship);

        assertResourceRelationExists("node_1", ResourceType.DataType.STRING, relationship);
        assertResourceRelationExists("rel_type", ResourceType.DataType.STRING, relationship);
        assertResourceRelationExists("node_2", ResourceType.DataType.STRING, relationship);
    }

    private ResourceType assertResourceExists(String name, ResourceType.DataType datatype) {
        ResourceType resourceType = graph.getResourceType(name);
        assertNotNull(resourceType);
        assertEquals(datatype.getName(), resourceType.getDataType().getName());
        return resourceType;
    }

    private void assertResourceRelationExists(String name, ResourceType.DataType datatype, Type owner){
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
