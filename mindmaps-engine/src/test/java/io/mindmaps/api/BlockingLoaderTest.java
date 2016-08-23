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
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.Var;
import io.mindmaps.loader.BlockingLoader;
import io.mindmaps.util.ConfigProperties;
import org.junit.*;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.mindmaps.graql.Graql.insert;

public class BlockingLoaderTest {

    Properties prop = new Properties();
    String graphName;
    BlockingLoader loader;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(BlockingLoaderTest.class);


    @BeforeClass
    public static void startController() {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
    }

    @Before
    public void setUp() throws Exception {
        try {
            prop.load(BlockingLoaderTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        graphName = prop.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        loader = new BlockingLoader(graphName);
        new CommitLogController();
    }


    @Test
    public void testLoadOntologyAndData() {
        loadOntology();

        ClassLoader classLoader = getClass().getClassLoader();
        File fileData = new File(classLoader.getResource("small_nametags.gql").getFile());
        long startTime = System.currentTimeMillis();
        loader.setExecutorSize(2);
        loader.setBatchSize(10);
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(fileData)).forEach(pattern -> loader.addToQueue(pattern.admin().asVar()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        loader.waitToFinish();
        long endTime = System.currentTimeMillis();
        long firstLoadingTime = endTime - startTime;

        cleanGraph();
        loadOntology();

        loader.setExecutorSize(16);
        loader.setBatchSize(60);
        startTime = System.currentTimeMillis();
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(fileData)).forEach(pattern -> loader.addToQueue(pattern.admin().asVar()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        loader.waitToFinish();
        endTime = System.currentTimeMillis();
        long secondLoadingTime = endTime - startTime;
        LOG.info("First load time " + firstLoadingTime + ". Second load time " + secondLoadingTime);

        Assert.assertTrue(secondLoadingTime < firstLoadingTime);
        Assert.assertNotNull(GraphFactory.getInstance().getGraph(graphName).getTransaction().getConcept("X546f736869616b69204b61776173616b69").getId());
    }

    private void loadOntology() {
        MindmapsTransaction transaction = GraphFactory.getInstance().getGraph(graphName).getTransaction();
        List<Var> ontologyBatch = new ArrayList<>();
        ClassLoader classLoader = getClass().getClassLoader();

        LOG.info("Loading new ontology .. ");
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(classLoader.getResource("dblp-ontology.gql").getFile())).map(x -> x.admin().asVar()).forEach(ontologyBatch::add);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        insert(ontologyBatch).withTransaction(transaction).execute();
        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }

        LOG.info("Ontology loaded. ");
    }

    @After
    public void cleanGraph() {
            GraphFactory.getInstance().getGraph(graphName).clear();
    }

}