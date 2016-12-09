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

package ai.grakn.test.engine.loader;

import ai.grakn.concept.Entity;
import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.engine.backgroundtasks.distributed.Scheduler;
import ai.grakn.engine.backgroundtasks.distributed.TaskRunner;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.loader.Loader;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.test.AbstractGraphTest;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

import static ai.grakn.graql.Graql.parse;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LoaderTest extends AbstractGraphTest {
    private Loader loader;

    @BeforeClass
    public static void startup() throws Exception {
        ((Logger) org.slf4j.LoggerFactory.getLogger(Loader.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(GraknStateStorage.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(Loader.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(Scheduler.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(TaskRunner.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(ClusterManager.class)).setLevel(Level.DEBUG);
    }

    @Before
    public void setup() {
        loader = new Loader(graph.getKeyspace());
    }

    @Test
    public void loaderDefaultBatchSizeTest() {
        loadOntology("dblp-ontology.gql", graph.getKeyspace());
        loadAndTime();
    }

    @Test
    public void loaderNewBatchSizeTest() {
        loader.setBatchSize(20);
        loadOntology("dblp-ontology.gql", graph.getKeyspace());
        loadAndTime();
    }

    @Test
    public void loadWithSmallQueueSizeToBlockTest(){
        loader.setQueueSize(1);
        loadOntology("dblp-ontology.gql", graph.getKeyspace());
        loadAndTime();
    }

    private void loadAndTime(){
        String toLoad = readFileAsString("small_nametags.gql");
        long startTime = System.currentTimeMillis();

        ((InsertQuery) parse(toLoad)).admin().getVars().stream()
                .map(Pattern::admin)
                .map(PatternAdmin::getVars)
                .map(Graql::insert)
                .forEach(loader::add);

        loader.waitToFinish(300000);

        System.out.println("Time to load:");
        System.out.println(System.currentTimeMillis() - startTime);

        graph = GraphFactory.getInstance().getGraph(graph.getKeyspace());
        Collection<Entity> nameTags = graph.getEntityType("name_tag").instances();

        assertEquals(100, nameTags.size());
        assertNotNull(graph.getResourcesByValue("X506965727265204162656c").iterator().next().getId());
    }
}
