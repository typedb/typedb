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

package ai.grakn.test.graql.reasoner.inference;

import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graphs.AbstractGraph;
import ai.grakn.test.GraphContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Ignore
public class AbstractInferenceTest {
    private static QueryBuilder qb;
    private static QueryBuilder iqb;

    @ClassRule
    public static GraphContext abstractGraph = GraphContext.preLoad(AbstractGraph.get());

    @BeforeClass
    public static void setUpClass() throws Exception {
        qb = abstractGraph.graph().graql().infer(false);
        iqb = abstractGraph.graph().graql().infer(true);
    }

    /**silently allows multiple isas*/
    @Test
    public void testQuery() {
        String queryString = "match $x isa Q;";
        String explicitQuery = "match " +
                "{$x isa Q} or {\n" +
                "{$y isa q} or {$y isa t};\n" +
                "{{$x isa p} or {$x isa s}} or {{$x isa r} or {$x isa u}};\n" +
                "($x, $y) isa rel\n" +
                "}; select $x";

        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    /**silently allows multiple isas*/
    @Test
    public void testQuery2() {
        String queryString = "match " +
                        "$yy isa Q;$y isa P;($y, $yy) isa REL; select $yy";
        String explicitQuery = "match " +
                                "{$yy isa Q} or {" +
                                "{$yyy isa q} or {$yyy isa t};\n" +
                                "($yy, $yyy) isa rel;\n" +
                                "{{$yy isa p} or {$yy isa s}} or {{yy isa r} or {$yy isa u}}" +
                                "};" +
                                "$y isa P;\n" +
                                "($y, $yy) isa REL; select $yy";

        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(q1.stream().collect(Collectors.toSet()), q2.stream().collect(Collectors.toSet()));
    }
}
