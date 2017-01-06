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

package ai.grakn.test.graql.analytics;

import ai.grakn.concept.EntityType;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.AbstractGraphTest;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assume.assumeFalse;
import static ai.grakn.test.GraknTestEnv.*;

public class CountTest extends AbstractGraphTest {

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(ComputeQuery.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testCount() throws Exception {
        // assert the graph is empty
        long startTime = System.currentTimeMillis();
        Assert.assertEquals(0L, Graql.compute().count().withGraph(graph).execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");
        Assert.assertEquals(0L, graph.graql().compute().count().execute().longValue());

        // create 3 instances
        System.out.println("Creating 3 instances");
        graph = factory.getGraph();
        String nameThing = "thing";
        String nameAnotherThing = "another";
        EntityType thing = graph.putEntityType(nameThing);
        EntityType anotherThing = graph.putEntityType(nameAnotherThing);
        thing.addEntity().getId();
        thing.addEntity().getId();
        anotherThing.addEntity().getId();
        graph.commit();

        // assert computer returns the correct count of instances
        startTime = System.currentTimeMillis();
        Assert.assertEquals(2L,
                graph.graql().compute().count().in(Collections.singleton(nameThing)).execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");
        startTime = System.currentTimeMillis();
        Assert.assertEquals(2L,
                Graql.compute().withGraph(graph).count().in(nameThing).execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        startTime = System.currentTimeMillis();
        Assert.assertEquals(3L, graph.graql().compute().count().execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");
        startTime = System.currentTimeMillis();
        Assert.assertEquals(3L, Graql.compute().count().withGraph(graph).execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");
    }
}