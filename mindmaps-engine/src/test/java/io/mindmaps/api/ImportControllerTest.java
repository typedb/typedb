package io.mindmaps.api;

import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class ImportControllerTest {

    ImportController importer;
    Properties prop = new Properties();
    String graphName;

    @Before
    public void setUp() throws Exception {
        try {
            prop.load(ImportControllerTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        graphName = prop.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        importer = new ImportController(graphName);
        new CommitLogController();
    }

    @Test
    public void testLoadOntologyAndData() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("dblp-ontology.gql").getFile());
        try {
            importer.importOntologyFromFile(file.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }
        File fileData= new File(classLoader.getResource("small_nametags.gql").getFile());
        try {
            importer.importDataFromFile(fileData.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assert.assertNotNull(GraphFactory.getInstance().getGraph(graphName).newTransaction().getConcept("X546f736869616b69204b61776173616b69").getId());
    }

    @After
    public void cleanGraph(){
        GraphFactory.getInstance().getGraph(graphName).clear();
    }

}