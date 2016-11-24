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

package ai.grakn.engine.loader;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Entity;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.admin.PatternAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static ai.grakn.graql.Graql.parsePatterns;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LoaderTest extends GraknEngineTestBase {

    private static final String keyspace = "KEYSPACE";
    private Loader loader;
    private GraknGraph graph;

    @Before
    public void setup() {
        graph = GraphFactory.getInstance().getGraph(keyspace);
        loader = new LoaderImpl(keyspace);
    }

    @After
    public void clean() throws InterruptedException {
        graph.clear();
        graph.close();
    }

    @Test
    public void loaderDefaultBatchSizeTest() {
        loadOntology("dblp-ontology.gql", keyspace);

        String nametags = readFileAsString("small_nametags.gql");
        loadAndTime(nametags);

        Collection<Entity> nameTags = graph.getEntityType("name_tag").instances();

        assertEquals(100, nameTags.size());
        assertNotNull(graph.getResourcesByValue("X506965727265204162656c").iterator().next().getId());
    }

    @Test
    public void loaderNewBatchSizeTest() {
        loader.setBatchSize(50);
        loadOntology("dblp-ontology.gql", keyspace);

        String nametags = readFileAsString("small_nametags.gql");
        loadAndTime(nametags);

        Collection<Entity> nameTags = graph.getEntityType("name_tag").instances();

        assertEquals(nameTags.size(), 100);
        assertNotNull(graph.getResourcesByValue("X506965727265204162656c").iterator().next().getId());
    }

    @Test
    public void loadWithSmallQueueSizeToBlockTest(){
        loader.setQueueSize(1);
        loadOntology("dblp-ontology.gql", keyspace);

        String nametags = readFileAsString("small_nametags.gql");
        loadAndTime(nametags);

        Collection<Entity> nameTags = graph.getEntityType("name_tag").instances();

        assertEquals(nameTags.size(), 100);
        assertNotNull(graph.getResourcesByValue("X506965727265204162656c").iterator().next().getId());
    }

    private void loadAndTime(String toLoad){
        long startTime = System.currentTimeMillis();

        parsePatterns(toLoad).stream()
                .map(Pattern::admin)
                .map(PatternAdmin::getVars)
                .map(Graql::insert)
                .forEach(loader::add);

        loader.waitToFinish();

        System.out.println("Time to load:");
        System.out.println(System.currentTimeMillis() - startTime);
    }
}
