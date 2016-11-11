/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.loader;

import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.Pattern;
import ai.grakn.MindmapsGraph;
import ai.grakn.engine.MindmapsEngineTestBase;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.MindmapsValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.parsePatterns;

public class BlockingLoaderTest extends MindmapsEngineTestBase {

    private String graphName;
    private BlockingLoader loader;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(BlockingLoaderTest.class);

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        loader = new BlockingLoader(graphName);
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

            Stream<Pattern> patterns = parsePatterns(new FileInputStream(fileData));
            patterns.map(p -> Graql.insert(p.admin().asVar())).forEach(loader::add);
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
            Stream<Pattern> patterns = parsePatterns(new FileInputStream(fileData));
            patterns.map(p -> Graql.insert(p.admin().asVar())).forEach(loader::add);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        loader.waitToFinish();
        endTime = System.currentTimeMillis();
        long secondLoadingTime = endTime - startTime;
        LOG.debug("First load time " + firstLoadingTime + ". Second load time " + secondLoadingTime);

        // TODO: Make this assertion consistently pass
        // Assert.assertTrue(secondLoadingTime < firstLoadingTime);

        Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getResourcesByValue("X506965727265204162656c").iterator().next().getId());
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
        Graql.parse(query).withGraph(graph).execute();
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