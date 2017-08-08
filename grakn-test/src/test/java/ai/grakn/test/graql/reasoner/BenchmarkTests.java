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

package ai.grakn.test.graql.reasoner;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.DiagonalGraph;
import ai.grakn.test.graphs.MatrixGraphII;
import ai.grakn.test.graphs.TransitivityChainGraph;
import ai.grakn.test.graphs.TransitivityMatrixGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BenchmarkTests {

    @Rule
    public final GraphContext graphContext = GraphContext.empty();

    @Before
    public void setUpGraph(){
        graphContext.graph().close();
    }

    /**
     * 2-rule transitive test with transitivity expressed in terms of two linear rules
     * The rules are defined as:
     *
     * (Q-from: $x, Q-to: $y) isa Q;
     * ->
     * (P-from: $x, P-to: $y) isa P;
     *
     * (Q-from: $x, Q-to: $z) isa Q;
     * (P-from: $z, P-to: $y) isa P;
     * ->
     * (P-from: $z, P-to: $y) isa P;
     *
     * Each pair of neighbouring grid points is related in the following fashion:
     *
     *  a_{i  , j} -  Q  - a_{i, j + 1}
     *       |                    |
     *       Q                    Q
     *       |                    |
     *  a_{i+1, j} -  Q  - a_{i+1, j+1}
     *
     *  i e [1, N]
     *  j e [1, N]
     */
    @Test
    public void testTransitiveMatrixLinear()  {
        final int N = 10;

        //                         DJ       IC     FO
        //results @N = 15 14400   3-5s
        //results @N = 20 44100    15s     8 s      8s
        //results @N = 25 105625   48s    27 s     31s
        //results @N = 30 216225  132s    65 s

        long startTime = System.currentTimeMillis();
        graphContext.load(MatrixGraphII.get(N, N));
        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("loadTime: " + loadTime);
        GraknGraph graph = graphContext.graph();

        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        String queryString = "match (P-from: $x, P-to: $y) isa P;";
        MatchQuery query = iqb.parse(queryString);

        startTime = System.currentTimeMillis();
        List<Answer> execute = query.execute();
        System.out.println("computeTime: " + (System.currentTimeMillis() - startTime) + " results: " + execute.size());

        int limit = 100;
        startTime = System.currentTimeMillis();
        List<Answer> results = query.limit(limit).execute();
        long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("limit " + limit + " results = " + results.size() + " answerTime: " + answerTime);
    }

    /**
     * single-rule transitivity test with initial data arranged in a chain of length N
     * The rule is given as:
     *
     * (Q-from: $x, Q-to: $z) isa Q;
     * (Q-from: $z, Q-to: $y) isa Q;
     * ->
     * (Q-from: $x, Q-to: $y) isa Q;
     *
     * Each neighbouring grid points are related in the following fashion:
     *
     *  a_{i} -  Q  - a_{i + 1}
     *
     *  i e [0, N)
     */
    @Test
    public void testTransitiveChain()  {
        final int N = 20;
        long startTime = System.currentTimeMillis();
        graphContext.load(TransitivityChainGraph.get(N));
        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("loadTime: " + loadTime);
        GraknGraph graph = graphContext.graph();

        QueryBuilder iqb = graph.graql().infer(true).materialise(false);

        String queryString = "match (Q-from: $x, Q-to: $y) isa Q;";
        MatchQuery query = iqb.parse(queryString);

        String queryString2 = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a';";
        MatchQuery query2 = iqb.parse(queryString2);

        startTime = System.currentTimeMillis();
        List<Answer> execute = query.execute();
        assertEquals(execute.size(), N*N/2 + N/2);
        System.out.println("computeTime: " + (System.currentTimeMillis() - startTime) + " results: " + execute.size());

        startTime = System.currentTimeMillis();
        List<Answer> execute2 = query2.execute();
        assertEquals(execute2.size(), N);
        System.out.println("computeTime with resource: " + (System.currentTimeMillis() - startTime) + " results: " + execute2.size());

        int limit = 10;
        startTime = System.currentTimeMillis();
        List<Answer> results = query.limit(limit).execute();
        long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("limit " + limit + " results = " + results.size() + " answerTime: " + answerTime);

        startTime = System.currentTimeMillis();
        results = query2.limit(limit).execute();
        answerTime = System.currentTimeMillis() - startTime;
        System.out.println("limit " + limit + " results = " + results.size() + " answerTime: " + answerTime);
    }

    /**
     * single-rule transitivity test with initial data arranged in a N x N square grid.
     * The rule is given as:
     *
     * (Q-from: $x, Q-to: $z) isa Q;
     * (Q-from: $z, Q-to: $y) isa Q;
     * ->
     * (Q-from: $x, Q-to: $y) isa Q;
     *
     * Each pair of neighbouring grid points is related in the following fashion:
     *
     *  a_{i  , j} -  Q  - a_{i, j + 1}
     *       |                    |
     *       Q                    Q
     *       |                    |
     *  a_{i+1, j} -  Q  - a_{i+1, j+1}
     *
     *  i e [0, N)
     *  j e [0, N)
     */
    @Test
    public void testTransitiveMatrix(){
        final int N = 5;

        //                         DJ       IC     FO
        //results @N = 15 14400     ?
        //results @N = 20 44100     ?       ?     12s     4 s
        //results @N = 25 105625    ?       ?     50s    11 s
        //results @N = 30 216225    ?       ?      ?     30 s
        //results @N = 35 396900   ?        ?      ?     76 s

        long startTime = System.currentTimeMillis();
        graphContext.load(TransitivityMatrixGraph.get(N, N));
        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("loadTime: " + loadTime);
        GraknGraph graph = graphContext.graph();

        QueryBuilder iqb = graph.graql().infer(true).materialise(false);

        //full result
        String queryString = "match (Q-from: $x, Q-to: $y) isa Q;";
        MatchQuery query = iqb.parse(queryString);

        //with specific resource
        String queryString2 = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a';";
        MatchQuery query2 = iqb.parse(queryString2);

        //with substitution
        Concept id = iqb.<MatchQuery>parse("match $x has index 'a';").execute().iterator().next().get("x");
        String queryString3 = "match (Q-from: $x, Q-to: $y) isa Q;$x id '" + id.getId().getValue() + "';";
        MatchQuery query3 = iqb.parse(queryString3);

        startTime = System.currentTimeMillis();
        List<Answer> execute = query.execute();
        System.out.println("full result computeTime: " + (System.currentTimeMillis() - startTime) + " results: " + execute.size());

        startTime = System.currentTimeMillis();
        List<Answer> execute2 = query2.execute();
        System.out.println("computeTime with resource: " + (System.currentTimeMillis() - startTime) + " results: " + execute2.size());

        startTime = System.currentTimeMillis();
        List<Answer> execute3 = query3.execute();
        System.out.println("computeTime with specific substitution: " + (System.currentTimeMillis() - startTime) + " results: " + execute3.size());

        int limit = 100;
        startTime = System.currentTimeMillis();
        List<Answer> results = query.limit(limit).execute();
        long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("limit " + limit + " results = " + results.size() + " answerTime: " + answerTime);
    }

    /**
     * single-rule mimicking transitivity test rule defined by two-hop relations
     * Initial data arranged in N x N square grid.
     *
     * Rule:
     * (rel-from:$x, rel-to:$y) isa horizontal;
     * (rel-from:$y, rel-to:$z) isa horizontal;
     * (rel-from:$z, rel-to:$u) isa vertical;
     * (rel-from:$u, rel-to:$v) isa vertical;
     * ->
     * (rel-from:$x, rel-to:$v) isa diagonal;
     *
     * Initial data arranged as follows:
     *
     *  a_{i  , j} -  horizontal  - a_{i, j + 1}
     *       |                    |
     *    vertical             vertical
     *       |                    |
     *  a_{i+1, j} -  horizontal  - a_{i+1, j+1}
     *
     *  i e [0, N)
     *  j e [0, N)
     */

    @Test
    public void testDiagonal()  {
        final int N = 10; //9604

        //results @N = 40  1444  3.5s
        //results @N = 50  2304    8s    / 1s
        //results @N = 100 9604  loading takes ages

        long startTime = System.currentTimeMillis();
        graphContext.load(DiagonalGraph.get(N, N));
        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("loadTime: " + loadTime);
        GraknGraph graph = graphContext.graph();

        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        String queryString = "match (rel-from: $x, rel-to: $y) isa diagonal;";
        MatchQuery query = iqb.parse(queryString);

        startTime = System.currentTimeMillis();
        List<Answer> execute = query.execute();
        System.out.println("computeTime: " + (System.currentTimeMillis() - startTime) + " results: " + execute.size());

        int limit = 10;
        startTime = System.currentTimeMillis();
        List<Answer> results = query.limit(limit).execute();
        long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("limit " + limit + " results = " + results.size() + " answerTime: " + answerTime);
    }
}
