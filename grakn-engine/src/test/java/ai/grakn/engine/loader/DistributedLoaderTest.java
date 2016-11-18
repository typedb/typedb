/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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

import ai.grakn.GraknGraph;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.parsePatterns;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DistributedLoaderTest extends GraknEngineTestBase {

    private final Logger LOG = LoggerFactory.getLogger(DistributedLoaderTest.class);

    private GraknGraph graph;

    private DistributedLoader loader;

    private String graphName;

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        loader = new DistributedLoader(graphName, Collections.singletonList("localhost"));
        graph = GraphFactory.getInstance().getGraphBatchLoading(graphName);
    }

    @Test
    public void testLoad1000() {
        ClassLoader classLoader = getClass().getClassLoader();
        File data = new File(classLoader.getResource("small_nametags.gql").getFile());

        loadOntologyFromFile();
        loadDataFromFile(data);

        assertNotNull(graph.getResourcesByValue("X4d616e75656c20417a656e6861").iterator().next().getId());
        assertNotNull(graph.getResourcesByValue("X44616e69656c61204675696f726561").iterator().next().getId());
        assertNotNull(graph.getResourcesByValue("X422e20476174686d616e6e").iterator().next().getId());
        assertNotNull(graph.getResourcesByValue("X416e6472657720522e2057656262").iterator().next().getId());
        assertNotNull(graph.getResourcesByValue("X4a752d4d696e205a68616f").iterator().next().getId());

        int size = graph.getEntityType("name_tag").instances().size();
        assertEquals(size, 100);
    }


    public void loadDataFromFile(File file) {
        loader.setBatchSize(50);
        loader.setPollingFrequency(1000);
        try {
            Stream<Pattern> patterns = parsePatterns(new FileInputStream(file));
            patterns.map(p -> Graql.insert(p.admin().asVar())).forEach(loader::add);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        loader.waitToFinish();
    }

    private void loadOntologyFromFile() {
        GraknGraph graph = GraphFactory.getInstance().getGraph(graphName);
        ClassLoader classLoader = getClass().getClassLoader();

        LOG.info("Loading new ontology .. ");

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
        } catch (GraknValidationException e) {
            e.printStackTrace();
        }

        LOG.info("Ontology loaded. ");
    }

    @After
    public void cleanGraph() {
        graph.clear();
    }
}