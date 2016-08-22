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
 *
 */

package io.mindmaps.graql.parser;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.QueryParser;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mindmaps.graql.Graql.*;
import static org.junit.Assert.assertEquals;

public class ComputeQueryTest {

    private static MindmapsTransaction transaction;
    private QueryParser qp;
    private QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.getTransaction();
    }

    @Before
    public void setUp() {
        qp = QueryParser.create(transaction);
        qb = withTransaction(transaction);
    }

    @Test
    public void testCount() {
        // TODO: Confirm this count from graql is what is expected in analytics
        long graqlCount = qb.match(
                var("x").isa(var("y")),
                or(var("y").isa("entity-type"), var("y").isa("resource-type"), var("y").isa("relation-type"))
        ).stream().count();

        long computeCount = (long) qp.parseQuery("compute count()");

        assertEquals(graqlCount, computeCount);
    }
}
