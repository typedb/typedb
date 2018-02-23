/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.kbs.DiagonalKB;
import ai.grakn.test.kbs.LinearTransitivityMatrixKB;
import ai.grakn.test.kbs.PathTreeKB;
import ai.grakn.test.kbs.TransitivityChainKB;
import ai.grakn.test.kbs.TransitivityMatrixKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.rule.SessionContext;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BenchmarkTests {

    //Needed to start cass depending on profile
    @ClassRule
    public static final SessionContext sessionContext = SessionContext.create();

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTests.class);

    /**
     * Executes a scalability test defined in terms of the number of rules in the system. Creates a simple rule chain:
     *
     * R_i(x, y) := R_{i-1}(x, y);     i e [1, N]
     *
     * with a single initial relation instance R_0(a ,b)
     *
     */
    @Test
    public void nonRecursiveChainOfRules() {
        final int N = 200;
        LOG.debug(new Object(){}.getClass().getEnclosingMethod().getName());
        GraknSession graknSession = sessionContext.newSession();

        //NB: loading data here as defining it as KB and using graql api leads to circular dependencies
        try(GraknTx tx = graknSession.open(GraknTxType.WRITE)) {
            Role fromRole = tx.putRole("fromRole");
            Role toRole = tx.putRole("toRole");

            RelationshipType relation0 = tx.putRelationshipType("relation0")
                    .relates(fromRole)
                    .relates(toRole);

            for (int i = 1; i <= N; i++) {
                tx.putRelationshipType("relation" + i)
                        .relates(fromRole)
                        .relates(toRole);
            }
            EntityType genericEntity = tx.putEntityType("genericEntity")
                    .plays(fromRole)
                    .plays(toRole);

            Entity fromEntity = genericEntity.addEntity();
            Entity toEntity = genericEntity.addEntity();

            relation0.addRelationship()
                    .addRolePlayer(fromRole, fromEntity)
                    .addRolePlayer(toRole, toEntity);

            for (int i = 1; i <= N; i++) {
                Var fromVar = Graql.var().asUserDefined();
                Var toVar = Graql.var().asUserDefined();
                VarPattern rulePattern = Graql
                        .label("rule" + i)
                        .when(
                                Graql.and(
                                        Graql.var()
                                                .rel(Graql.label(fromRole.getLabel()), fromVar)
                                                .rel(Graql.label(toRole.getLabel()), toVar)
                                                .isa("relation" + (i - 1))
                                )
                        )
                        .then(
                                Graql.and(
                                        Graql.var()
                                                .rel(Graql.label(fromRole.getLabel()), fromVar)
                                                .rel(Graql.label(toRole.getLabel()), toVar)
                                                .isa("relation" + i)
                                )
                        );
                tx.graql().define(rulePattern).execute();
            }
            tx.commit();
        }

        try( GraknTx tx = graknSession.open(GraknTxType.READ)) {
            final long limit = 1;
            String queryPattern = "(fromRole: $x, toRole: $y) isa relation" + N + ";";
            String queryString = "match " + queryPattern + " get;";
            String limitedQueryString = "match " +
                    queryPattern +
                    "limit " + limit +  ";" +
                    "get;";

            assertEquals(executeQuery(queryString, tx, "full").size(), limit);
            assertEquals(executeQuery(limitedQueryString, tx, "limit").size(), limit);
        }
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
        final int limit = 100;
        LOG.debug(new Object(){}.getClass().getEnclosingMethod().getName());

        //                         DJ       IC     FO
        //results @N = 15 14400   3-5s
        //results @N = 20 44100    15s     8 s      8s
        //results @N = 25 105625   48s    27 s     31s
        //results @N = 30 216225  132s    65 s
        SampleKBContext kb = LinearTransitivityMatrixKB.context(N, N);

        String queryString = "match (P-from: $x, P-to: $y) isa P; get;";

        executeQuery(queryString, kb.tx(), "full");
        executeQuery(kb.tx().graql().<GetQuery>parse(queryString).match().limit(limit).get(), "limit " + limit);
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
        final int N = 100;
        final int limit = 10;
        final int answers = (N+1)*N/2;
        LOG.debug(new Object(){}.getClass().getEnclosingMethod().getName());

        SampleKBContext kb = TransitivityChainKB.context(N);
        QueryBuilder iqb = kb.tx().graql().infer(true).materialise(false);

        String queryString = "match (Q-from: $x, Q-to: $y) isa Q; get;";
        GetQuery query = iqb.parse(queryString);

        String queryString2 = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a'; get;";
        GetQuery query2 = iqb.parse(queryString2);

        assertEquals(executeQuery(query, "full").size(), answers);
        assertEquals(executeQuery(query2, "With specific resource").size(), N);

        executeQuery(query.match().limit(limit).get(), "limit " + limit);
        executeQuery(query2.match().limit(limit).get(), "limit " + limit);
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
        LOG.debug(new Object(){}.getClass().getEnclosingMethod().getName());
        final int N = 10;
        int limit = 100;

        //                         DJ       IC     FO
        //results @N = 15 14400     ?
        //results @N = 20 44100     ?       ?     12s     4 s
        //results @N = 25 105625    ?       ?     50s    11 s
        //results @N = 30 216225    ?       ?      ?     30 s
        //results @N = 35 396900   ?        ?      ?     76 s
        SampleKBContext kb = TransitivityMatrixKB.context(N, N);
        QueryBuilder iqb = kb.tx().graql().infer(true).materialise(false);

        //full result
        String queryString = "match (Q-from: $x, Q-to: $y) isa Q; get;";
        GetQuery query = iqb.parse(queryString);

        //with specific resource
        String queryString2 = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a'; get;";
        GetQuery query2 = iqb.parse(queryString2);

        //with substitution
        Concept id = iqb.<GetQuery>parse("match $x has index 'a'; get;").execute().iterator().next().get("x");
        String queryString3 = "match (Q-from: $x, Q-to: $y) isa Q;$x id '" + id.getId().getValue() + "'; get;";
        GetQuery query3 = iqb.parse(queryString3);

        executeQuery(query, "full");
        executeQuery(query2, "With specific resource");
        executeQuery(query3, "Single argument bound");
        executeQuery(query.match().limit(limit).get(), "limit " + limit);
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
        final int limit = 10;
        LOG.debug(new Object(){}.getClass().getEnclosingMethod().getName());

        //results @N = 40  1444  3.5s
        //results @N = 50  2304    8s    / 1s
        //results @N = 100 9604  loading takes ages
        SampleKBContext kb = DiagonalKB.context(N, N);

        QueryBuilder iqb = kb.tx().graql().infer(true).materialise(false);
        String queryString = "match (rel-from: $x, rel-to: $y) isa diagonal; get;";
        GetQuery query = iqb.parse(queryString);

        executeQuery(query, "full");
        executeQuery(query.match().limit(limit).get(), "limit " + limit);
    }

    /**
     * single-rule mimicking transitivity test rule defined by two-hop relations
     * Initial data arranged in N x N square grid.
     *
     * Rules:
     * (arc-from: $x, arc-to: $y) isa arc;},
     * ->
     * (path-from: $x, path-to: $y) isa path;};

     *
     * (path-from: $x, path-to: $z) isa path;
     * (path-from: $z, path-to: $y) isa path;},
     * ->
     * (path-from: $x, path-to: $y) isa path;};
     *
     * Initial data arranged as follows:
     *
     * N - tree heights
     * l - number of links per entity
     *
     *                     a0
      *               /     .   \
     *             arc          arc
     *             /       .       \
     *           a1,1     ...    a1,1^l
     *         /   .  \         /    .  \
     *       arc   .  arc     arc    .  arc
     *       /     .   \       /     .    \
     *     a2,1 ...  a2,l  a2,l+1  ...  a2,2^l
     *            .             .
     *            .             .
     *            .             .
     *   aN,1    ...  ...  ...  ...  ... ... aN,N^l
     *
     */
    @Test
    public void testPathTree(){
        final int N = 5;
        final int linksPerEntity = 4;
        LOG.debug(new Object(){}.getClass().getEnclosingMethod().getName());
        
        int answers = 0;
        for(int i = 1 ; i <= N ; i++) answers += Math.pow(linksPerEntity, i);
        SampleKBContext kb = PathTreeKB.context(N, linksPerEntity);
        GraknTx graph = kb.tx();

        String queryString = "match (path-from: $x, path-to: $y) isa path;" +
                "$x has index 'a0';" +
                "limit " + answers + ";" +
                "get $y;";

        assertEquals(executeQuery(queryString, graph, "tree").size(), answers);
    }

    private List<Answer> executeQuery(String queryString, GraknTx graph, String msg){
        return executeQuery(graph.graql().infer(true).parse(queryString), msg);
    }

    private List<Answer> executeQuery(GetQuery query, String msg) {
        final long startTime = System.currentTimeMillis();
        List<Answer> results = query.execute();
        final long answerTime = System.currentTimeMillis() - startTime;
        LOG.debug(msg + " results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }
}
