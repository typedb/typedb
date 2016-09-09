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

package io.mindmaps.engine.loader;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.controller.CommitLogController;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryParser;
import org.junit.*;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class BlockingLoaderTest {

    String graphName;
    BlockingLoader loader;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(BlockingLoaderTest.class);


    @BeforeClass
    public static void startController() {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
    }

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
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
        loader.setBatchSize(10);
        startTime = System.currentTimeMillis();
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(fileData)).forEach(pattern -> loader.addToQueue(pattern.admin().asVar()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        loader.waitToFinish();
        endTime = System.currentTimeMillis();
        long secondLoadingTime = endTime - startTime;
        LOG.debug("First load time " + firstLoadingTime + ". Second load time " + secondLoadingTime);

        // TODO: Make this assertion consistently pass
        // Assert.assertTrue(secondLoadingTime < firstLoadingTime);

        Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getConcept("X506965727265204162656c").getId());
    }

    private void loadOntology() {
        MindmapsGraph graph = GraphFactory.getInstance().getGraphBatchLoading(graphName);
        ClassLoader classLoader = getClass().getClassLoader();

        LOG.debug("Loading new ontology .. ");

            List<String> lines = null;
            try {
                lines = Files.readAllLines(Paths.get(classLoader.getResource("dblp-ontology.gql").getFile()), StandardCharsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            QueryParser.create().parseInsertQuery(query).withGraph(graph).execute();
        try {
            graph.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }

        LOG.debug("Ontology loaded. ");
    }

    @After
    public void cleanGraph() {
            GraphFactory.getInstance().getGraphBatchLoading(graphName).clear();
    }

}