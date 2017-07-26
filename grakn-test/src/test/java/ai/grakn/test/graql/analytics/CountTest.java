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
import ai.grakn.graph.internal.computer.GraknSparkComputer;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assume.assumeFalse;

public class CountTest {

    @ClassRule
    public static final EngineContext rule = EngineContext.startInMemoryServer();

    private GraknSession factory;

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(GraknTestSetup.usingOrientDB());

        factory = rule.factoryWithNewKeyspace();
    }

    @Test
    public void testCountAfterCommit() throws Exception {
        String nameThing = "thingy";
        String nameAnotherThing = "another";

        // assert the graph is empty
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            Assert.assertEquals(0L, Graql.compute().count().withGraph(graph).execute().longValue());
            Assert.assertEquals(0L, graph.graql().compute().count().execute().longValue());
        }

        // add 2 instances
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            EntityType thingy = graph.putEntityType(nameThing);
            thingy.addEntity().getId();
            thingy.addEntity().getId();
            graph.commit();
        }

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            Assert.assertEquals(2L,
                    Graql.compute().withGraph(graph).count().in(nameThing).execute().longValue());
        }

        // create 1 more, rdd is refreshed
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            EntityType anotherThing = graph.putEntityType(nameAnotherThing);
            anotherThing.addEntity().getId();
            graph.commit();
        }

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            // assert computer returns the correct count of instances
            Assert.assertEquals(2L,
                    Graql.compute().withGraph(graph).count().in(nameThing).execute().longValue());
            Assert.assertEquals(3L, graph.graql().compute().count().execute().longValue());
            GraknSparkComputer.clear();
            Assert.assertEquals(3L, Graql.compute().count().withGraph(graph).execute().longValue());
        }

        GraknSparkComputer.clear();
        List<Long> list = new ArrayList<>(4);
        for (long i = 0L; i < 4L; i++) {
            list.add(i);
        }

        // running 4 jobs at the same time
        // collecting the result in the end so engine won't stop before the test finishes
        Set<Long> result;
        result = list.parallelStream()
                .map(i -> executeCount(factory))
                .collect(Collectors.toSet());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3L, result.iterator().next().longValue());

        result = list.parallelStream()
                .map(i -> executeCount(factory))
                .collect(Collectors.toSet());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3L, result.iterator().next().longValue());

    }

    private Long executeCount(GraknSession factory) {
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            return graph.graql().compute().count().execute();
        }
    }
}