/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.reasoner.benchmark;

import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static junit.framework.TestCase.assertEquals;

public class BenchmarkSmallIT {

    private static final boolean PREVENT_HANGING = true;
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("computation-graph-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB).traceInference(false).explain(true);
    private static final String database = "computation-graph-test";
    private static CoreDatabaseManager databaseMgr;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
    }
    @After
    public void tearDown() {
        databaseMgr.close();
    }

    private CoreSession schemaSession() {
        return databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
    }

    private CoreSession dataSession() {
        return databaseMgr.session(database, Arguments.Session.Type.DATA);
    }

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
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());

        try (CoreSession session = schemaSession()) {
            //NB: loading data here as defining it as KB and using graql api leads to circular dependencies
            try (CoreTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {

                RelationType relation0 = tx.concepts().putRelationType("relation0");
                relation0.setRelates("fromRole");
                relation0.setRelates("toRole");

                EntityType genericEntity = tx.concepts().putEntityType("genericEntity");
                genericEntity.setPlays(relation0.getRelates("fromRole"));
                genericEntity.setPlays(relation0.getRelates("toRole"));

                for (int i = 1; i <= N; i++) {
                    RelationType rel = tx.concepts().putRelationType("relation" + i);
                    rel.setRelates("fromRole");
                    rel.setRelates("toRole");
                    genericEntity.setPlays(rel.getRelates("fromRole"));
                    genericEntity.setPlays(rel.getRelates("toRole"));
                }

                for (int i = 1; i <= N; i++) {
                    tx.logic().putRule("rule"+i,
                            TypeQL.parsePattern("{ (fromRole: $f, toRole: $t) isa relation" + (i-1) + "; }").asConjunction(),
                            TypeQL.parseVariable("(fromRole: $f, toRole: $t) isa relation"+ i).asThing());
                }
                tx.commit();
            }
        }

        try (CoreSession session = dataSession()) {
            //NB: loading data here as defining it as KB and using graql api leads to circular dependencies
            try (CoreTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                EntityType genericEntity = tx.concepts().getEntityType("genericEntity");
                Entity fromEntity = genericEntity.create();
                Entity toEntity = genericEntity.create();

                RelationType relation0 = tx.concepts().getRelationType("relation0");
                Relation rel = relation0.create();
                rel.addPlayer(relation0.getRelates("fromRole"), fromEntity);
                rel.addPlayer(relation0.getRelates("toRole"), toEntity);
                tx.commit();
            }

            try (CoreTransaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                final long limit = 1;
                String queryPattern = "(fromRole: $x, toRole: $y) isa relation" + N + ";";
                String queryString = "match " + queryPattern + " get $x, $y;";
                String limitedQueryString = "match " + queryPattern + " get $x, $y; limit " + limit + ";";

                assertEquals(executeQuery(queryString, tx, "full").size(), limit);
                assertEquals(executeQuery(limitedQueryString, tx, "limit").size(), limit);
            }
        }
    }
//
//    /**
//     * 2-rule transitive test with transitivity expressed in terms of two linear rules
//     * The rules are defined as:
//     *
//     * (Q-from: $x, Q-to: $y) isa Q;
//     * ->
//     * (P-from: $x, P-to: $y) isa P;
//     *
//     * (Q-from: $x, Q-to: $z) isa Q;
//     * (P-from: $z, P-to: $y) isa P;
//     * ->
//     * (P-from: $z, P-to: $y) isa P;
//     *
//     * Each pair of neighbouring grid points is related in the following fashion:
//     *
//     *  a_{i  , j} -  Q  - a_{i, j + 1}
//     *       |                    |
//     *       Q                    Q
//     *       |                    |
//     *  a_{i+1, j} -  Q  - a_{i+1, j+1}
//     *
//     *  i e [1, N]
//     *  j e [1, N]
//     */
//    @Test
//    public void testTransitiveMatrixLinear()  {
//        int N = 10;
//        int limit = 100;
//        Session session = server.sessionWithNewKeyspace();
//        LinearTransitivityMatrixGraph linearGraph = new LinearTransitivityMatrixGraph(session);
//
//        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
//
//        //                         DJ       IC     FO
//        //results @N = 15 14400   3-5s
//        //results @N = 20 44100    15s     8 s      8s
//        //results @N = 25 105625   48s    27 s     31s
//        //results @N = 30 216225  132s    65 s
//        linearGraph.load(N, N);
//
//        String queryString = "match (P-from: $x, P-to: $y) isa P; get;";
//        Transaction tx = session.transaction(Transaction.Type.WRITE);
//        executeQuery(queryString, tx, "full");
//        executeQuery(Graql.parse(queryString).asGet().match().get().limit(limit), tx, "limit " + limit);
//        tx.close();
//        session.close();
//    }
//
//    /**
//     * single-rule transitivity test with initial data arranged in a chain of length N
//     * The rule is given as:
//     *
//     * (Q-from: $x, Q-to: $z) isa Q;
//     * (Q-from: $z, Q-to: $y) isa Q;
//     * ->
//     * (Q-from: $x, Q-to: $y) isa Q;
//     *
//     * Each neighbouring grid points are related in the following fashion:
//     *
//     *  a_{i} -  Q  - a_{i + 1}
//     *
//     *  i e [0, N)
//     */
//    @Test
//    public void testTransitiveChain()  {
//        int N = 100;
//        int limit = 10;
//        int answers = (N+1)*N/2;
//        Session session = server.sessionWithNewKeyspace();
//        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
//        TransitivityChainGraph transitivityChainGraph = new TransitivityChainGraph(session);
//        transitivityChainGraph.load(N);
//        Transaction tx = session.transaction(Transaction.Type.WRITE);
//
//        String queryString = "match (Q-from: $x, Q-to: $y) isa Q; get;";
//        GraqlGet query = Graql.parse(queryString).asGet();
//
//        String queryString2 = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a'; get;";
//        GraqlGet query2 = Graql.parse(queryString2).asGet();
//
//        assertEquals(executeQuery(query, tx, "full").size(), answers);
//        assertEquals(executeQuery(query2, tx, "With specific resource").size(), N);
//
//        executeQuery(query.match().get().limit(limit), tx, "limit " + limit);
//        executeQuery(query2.match().get().limit(limit), tx, "limit " + limit);
//        tx.close();
//        session.close();
//    }
//
//    /**
//     * single-rule transitivity test with initial data arranged in a N x N square grid.
//     * The rule is given as:
//     *
//     * (Q-from: $x, Q-to: $z) isa Q;
//     * (Q-from: $z, Q-to: $y) isa Q;
//     * ->
//     * (Q-from: $x, Q-to: $y) isa Q;
//     *
//     * Each pair of neighbouring grid points is related in the following fashion:
//     *
//     *  a_{i  , j} -  Q  - a_{i, j + 1}
//     *       |                    |
//     *       Q                    Q
//     *       |                    |
//     *  a_{i+1, j} -  Q  - a_{i+1, j+1}
//     *
//     *  i e [0, N)
//     *  j e [0, N)
//     */
//    @Test
//    public void testTransitiveMatrix(){
//        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
//        int N = 10;
//        int limit = 100;
//
//        Session session = server.sessionWithNewKeyspace();
//        TransitivityMatrixGraph transitivityMatrixGraph = new TransitivityMatrixGraph(session);
//        //                         DJ       IC     FO
//        //results @N = 15 14400     ?
//        //results @N = 20 44100     ?       ?     12s     4 s
//        //results @N = 25 105625    ?       ?     50s    11 s
//        //results @N = 30 216225    ?       ?      ?     30 s
//        //results @N = 35 396900   ?        ?      ?     76 s
//        transitivityMatrixGraph.load(N, N);
//        Transaction tx = session.transaction(Transaction.Type.WRITE);
//
//
//        //full result
//        String queryString = "match (Q-from: $x, Q-to: $y) isa Q; get;";
//        GraqlGet query = Graql.parse(queryString).asGet();
//
//        //with specific resource
//        String queryString2 = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a'; get;";
//        GraqlGet query2 = Graql.parse(queryString2).asGet();
//
//        //with substitution
//        Concept id = tx.execute(Graql.parse("match $x has index 'a'; get;").asGet()).iterator().next().get("x");
//        String queryString3 = "match (Q-from: $x, Q-to: $y) isa Q;$x id " + id.id().getValue() + "; get;";
//        GraqlGet query3 = Graql.parse(queryString3).asGet();
//
//        executeQuery(query, tx, "full");
//        executeQuery(query2, tx, "With specific resource");
//        executeQuery(query3, tx, "Single argument bound");
//        executeQuery(query.match().get().limit(limit), tx, "limit " + limit);
//        tx.close();
//        session.close();
//    }
//
//    /**
//     * single-rule mimicking transitivity test rule defined by two-hop relations
//     * Initial data arranged in N x N square grid.
//     *
//     * Rule:
//     * (rel-from:$x, rel-to:$y) isa horizontal;
//     * (rel-from:$y, rel-to:$z) isa horizontal;
//     * (rel-from:$z, rel-to:$u) isa vertical;
//     * (rel-from:$u, rel-to:$v) isa vertical;
//     * ->
//     * (rel-from:$x, rel-to:$v) isa diagonal;
//     *
//     * Initial data arranged as follows:
//     *
//     *  a_{i  , j} -  horizontal  - a_{i, j + 1}
//     *       |                    |
//     *    vertical             vertical
//     *       |                    |
//     *  a_{i+1, j} -  horizontal  - a_{i+1, j+1}
//     *
//     *  i e [0, N)
//     *  j e [0, N)
//     */
//    @Test
//    public void testDiagonal()  {
//        int N = 10; //9604
//        int limit = 10;
//
//        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
//
//        Session session = server.sessionWithNewKeyspace();
//        DiagonalGraph diagonalGraph = new DiagonalGraph(session);
//        diagonalGraph.load(N, N);
//        //results @N = 40  1444  3.5s
//        //results @N = 50  2304    8s    / 1s
//        //results @N = 100 9604  loading takes ages
//        Transaction tx = session.transaction(Transaction.Type.WRITE);
//
//        String queryString = "match (rel-from: $x, rel-to: $y) isa diagonal; get;";
//        GraqlGet query = Graql.parse(queryString).asGet();
//
//        executeQuery(query, tx, "full");
//        executeQuery(query.match().get().limit(limit), tx, "limit " + limit);
//        tx.close();
//        session.close();
//    }
//
//    /**
//     * single-rule mimicking transitivity test rule defined by two-hop relations
//     * Initial data arranged in N x N square grid.
//     *
//     * Rules:
//     * (arc-from: $x, arc-to: $y) isa arc;},
//     * ->
//     * (path-from: $x, path-to: $y) isa path;};
//
//     *
//     * (path-from: $x, path-to: $z) isa path;
//     * (path-from: $z, path-to: $y) isa path;},
//     * ->
//     * (path-from: $x, path-to: $y) isa path;};
//     *
//     * Initial data arranged as follows:
//     *
//     * N - tree heights
//     * l - number of links per entity
//     *
//     *                     a0
//     *               /     .   \
//     *             arc          arc
//     *             /       .       \
//     *           a1,1     ...    a1,1^l
//     *         /   .  \         /    .  \
//     *       arc   .  arc     arc    .  arc
//     *       /     .   \       /     .    \
//     *     a2,1 ...  a2,l  a2,l+1  ...  a2,2^l
//     *            .             .
//     *            .             .
//     *            .             .
//     *   aN,1    ...  ...  ...  ...  ... ... aN,N^l
//     *
//     */
//    @Test
//    public void testPathTree(){
//        int N = 5;
//        int linksPerEntity = 4;
//        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
//        Session session = server.sessionWithNewKeyspace();
//        PathTreeGraph pathTreeGraph = new PathTreeGraph(session);
//        pathTreeGraph.load(N, linksPerEntity);
//        int answers = 0;
//        for(int i = 1 ; i <= N ; i++) answers += Math.pow(linksPerEntity, i);
//
//        Transaction tx = session.transaction(Transaction.Type.WRITE);
//
//        String queryString = "match (path-from: $x, path-to: $y) isa path;" +
//                "$x has index 'a0';" +
//                "get $y; limit " + answers + ";";
//
//        assertEquals(executeQuery(queryString, tx, "tree").size(), answers);
//        tx.close();
//        session.close();
//    }

    private List<ConceptMap> executeQuery(String queryString, CoreTransaction transaction, String msg){
        return executeQuery(TypeQL.parseQuery(queryString).asMatch(), transaction, msg);
    }

    private List<ConceptMap> executeQuery(TypeQLMatch query, CoreTransaction transaction, String msg){
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = (List<ConceptMap>) transaction.query().match(query).toList();
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println(msg + " results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }

}