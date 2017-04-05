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

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graph.internal.computer.GraknSparkComputer;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static ai.grakn.test.GraknTestEnv.usingOrientDB;
import static org.junit.Assume.assumeFalse;

public class CountTest {

    @ClassRule
    public static final EngineContext rule = EngineContext.startInMemoryServer();

    private GraknSession factory;
    private GraknGraph graph;

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());

        factory = rule.factoryWithNewKeyspace();
        graph = factory.open(GraknTxType.WRITE);
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
//        graph = factory.getGraph();
        String nameThing = "thing";
        String nameAnotherThing = "another";
        EntityType thing = graph.putEntityType(nameThing);
        EntityType anotherThing = graph.putEntityType(nameAnotherThing);
        thing.addEntity().getId();
        thing.addEntity().getId();
        anotherThing.addEntity().getId();
        graph.commit();
        graph = factory.open(GraknTxType.WRITE);

        // assert computer returns the correct count of instances
        startTime = System.currentTimeMillis();
        Assert.assertEquals(2L,
                graph.graql().compute().count().in(Collections.singleton(TypeLabel.of(nameThing))).execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");
        startTime = System.currentTimeMillis();
        Assert.assertEquals(2L,
                Graql.compute().withGraph(graph).count().in(nameThing).execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        startTime = System.currentTimeMillis();
        Assert.assertEquals(3L, graph.graql().compute().count().execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");
        GraknSparkComputer.clear();
        startTime = System.currentTimeMillis();
        Assert.assertEquals(3L, Graql.compute().count().withGraph(graph).execute().longValue());
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        List<Long> list = new ArrayList<>(4);
        for (long i = 0L; i < 4L; i++) {
            list.add(i);
        }
        GraknSparkComputer.clear();

        graph.close();
        // running 4 jobs at the same time
        list.parallelStream()
                .map(i -> executeCount(factory))
                .forEach(i -> Assert.assertEquals(3L, i.longValue()));
        list.parallelStream()
                .map(i -> executeCount(factory))
                .forEach(i -> Assert.assertEquals(3L, i.longValue()));
    }
    private Long executeCount(GraknSession factory){
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        Long result = graph.graql().compute().count().execute();
        graph.close();
        return result;
    }
}