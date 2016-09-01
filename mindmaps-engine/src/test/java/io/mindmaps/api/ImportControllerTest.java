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

package io.mindmaps.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.postprocessing.BackgroundTasks;
import io.mindmaps.util.ConfigProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ImportControllerTest {

    ImportController importer;
    String graphName;

    @BeforeClass
    public static void startController() {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
    }

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        importer = new ImportController(graphName);
        new CommitLogController();
        new GraphFactoryController();

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
        Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getTransaction().getConcept("X506965727265204162656c").getId());
    }

    @After
    public void cleanGraph(){
        GraphFactory.getInstance().getGraphBatchLoading(graphName).clear();
    }

}