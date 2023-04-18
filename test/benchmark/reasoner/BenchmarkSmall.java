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

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.test.benchmark.generation.DiagonalGraph;
import com.vaticle.typedb.core.test.benchmark.generation.PathTreeGraph;
import com.vaticle.typedb.core.test.benchmark.generation.TransitivityChainGraph;
import com.vaticle.typedb.core.test.benchmark.generation.TransitivityMatrixGraph;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static junit.framework.TestCase.assertEquals;

public class BenchmarkSmall {
    private static final String database = "reasoner-benchmark-small";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("reasoner-benchmark-small");
    private static final Database options = new Database().dataDir(dataDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);

    private static CoreDatabaseManager databaseMgr;

    @Before
    public void setUp() throws IOException {
        com.vaticle.typedb.core.test.integration.util.Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
    }

    @After
    public void tearDown() {
        databaseMgr.close();
    }

    private TypeDB.Session schemaSession() {
        return databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
    }

    private TypeDB.Session dataSession() {
        return databaseMgr.session(database, Arguments.Session.Type.DATA);
    }

    /**
     * Executes a scalability test defined in terms of the number of rules in the system. Creates a simple rule chain:
     * <p>
     * R_i(x, y) := R_{i-1}(x, y);     i e [1, N]
     * <p>
     * with a single initial relation instance R_0(a ,b)
     */
    @Test
    public void nonRecursiveChainOfRules() {
        final int N = 200;
        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());

        try (TypeDB.Session session = schemaSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {

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
                    tx.logic().putRule("rule" + i,
                            TypeQL.parsePattern("{ (fromRole: $f, toRole: $t) isa relation" + (i - 1) + "; }").asConjunction(),
                            TypeQL.parseVariable("(fromRole: $f, toRole: $t) isa relation" + i).asThing());
                }
                tx.commit();
            }
        }

        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                EntityType genericEntity = tx.concepts().getEntityType("genericEntity");
                Entity fromEntity = genericEntity.create();
                Entity toEntity = genericEntity.create();

                RelationType relation0 = tx.concepts().getRelationType("relation0");
                Relation rel = relation0.create();
                rel.addPlayer(relation0.getRelates("fromRole"), fromEntity);
                rel.addPlayer(relation0.getRelates("toRole"), toEntity);
                tx.commit();
            }

            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                final long limit = 1;
                String queryPattern = "(fromRole: $x, toRole: $y) isa relation" + N + ";";
                String queryString = "match " + queryPattern + " get $x, $y;";
                String limitedQueryString = "match " + queryPattern + " get $x, $y; limit " + limit + ";";

                assertEquals(limit, Util.timeQuery(queryString, tx, "full").size());
                assertEquals(limit, Util.timeQuery(limitedQueryString, tx, "limit").size());
            }
        }
    }

    /**
     * 2-rule transitive test with transitivity expressed in terms of two linear rules
     * The rules are defined as:
     * <p>
     * (from: $x, to: $y) isa Q;
     * ->
     * (from: $x, to: $y) isa P;
     * <p>
     * (from: $x, to: $z) isa Q;
     * (from: $z, to: $y) isa P;
     * ->
     * (from: $z, to: $y) isa P;
     * <p>
     * Each pair of neighbouring grid points is related in the following fashion:
     * <p>
     * a_{i  , j} -  Q  - a_{i, j + 1}
     * |                    |
     * Q                    Q
     * |                    |
     * a_{i+1, j} -  Q  - a_{i+1, j+1}
     * <p>
     * i e [1, N]
     * j e [1, N]
     */
    @Test
    public void testTransitiveMatrixLinear() {
        int N = 10;
        int limit = 100;

        TransitivityMatrixGraph linearGraph = new TransitivityMatrixGraph.Linear(databaseMgr, database);

        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());

        linearGraph.load(N, N);

        TypeQLMatch.Unfiltered match = TypeQL.match(TypeQL.parsePattern("(from: $x, to: $y) isa P"));
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                Util.timeQuery(match, tx, "full");
                Util.timeQuery(match.limit(limit), tx, "limit " + limit);
            }
        }
    }

    /**
     * single-rule transitivity test with initial data arranged in a chain of length N
     * The rule is given as:
     * <p>
     * (from: $x, to: $z) isa Q;
     * (from: $z, to: $y) isa Q;
     * ->
     * (from: $x, to: $y) isa Q;
     * <p>
     * Each neighbouring grid points are related in the following fashion:
     * <p>
     * a_{i} -  Q  - a_{i + 1}
     * <p>
     * i e [0, N)
     */
    @Test
    public void testTransitiveChain() {
        int N = 100;
        int limit = 10;
        int answers = (N + 1) * N / 2;

        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());

        TransitivityChainGraph transitivityChainGraph = new TransitivityChainGraph(databaseMgr, database);
        transitivityChainGraph.load(N);

        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {

                TypeQLMatch.Unfiltered query = TypeQL.match(TypeQL.parsePatterns("(from: $x, to: $y) isa Q;"));

                TypeQLMatch.Unfiltered query2 = TypeQL.match(TypeQL.parsePatterns("(from: $x, to: $y) isa Q; $x has index 'a'; "));

                assertEquals(answers, Util.timeQuery(query, tx, "full").size());
                assertEquals(N, Util.timeQuery(query2, tx, "With specific resource").size());

                Util.timeQuery(query.limit(limit), tx, "limit " + limit);
                Util.timeQuery(query2.limit(limit), tx, "limit " + limit);
                tx.close();
                session.close();
            }
        }
    }

    /**
     * single-rule transitivity test with initial data arranged in a N x N square grid.
     * The rule is given as:
     * <p>
     * (Q-from: $x, Q-to: $z) isa Q;
     * (Q-from: $z, Q-to: $y) isa Q;
     * ->
     * (Q-from: $x, Q-to: $y) isa Q;
     * <p>
     * Each pair of neighbouring grid points is related in the following fashion:
     * <p>
     * a_{i  , j} -  Q  - a_{i, j + 1}
     * |                    |
     * Q                    Q
     * |                    |
     * a_{i+1, j} -  Q  - a_{i+1, j+1}
     * <p>
     * i e [0, N)
     * j e [0, N)
     */
    @Test
    public void testTransitiveMatrix() {
        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());
        int N = 10;
        int limit = 100;

        TransitivityMatrixGraph transitivityMatrixGraph = new TransitivityMatrixGraph.Quadratic(databaseMgr, database);
        transitivityMatrixGraph.load(N, N);
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                // full result
                TypeQLMatch.Unfiltered query = TypeQL.match(TypeQL.parsePatterns("(from: $x, to: $y) isa Q;"));

                // with specific resource
                TypeQLMatch.Unfiltered query2 = TypeQL.match(TypeQL.parsePatterns("(from: $x, to: $y) isa Q;$x has index 'a';"));

                // with substitution
                Concept id = tx.query().match(TypeQL.parseQuery("match $x has index 'a';").asMatch()).next().get("x");
                TypeQLMatch.Unfiltered query3 = TypeQL.match(TypeQL.parsePatterns("(from: $x, to: $y) isa Q;$x iid " + id.asThing().getIID().toHexString() + ";"));

                Util.timeQuery(query, tx, "full");
                Util.timeQuery(query2, tx, "With specific resource");
                Util.timeQuery(query3, tx, "Single argument bound");
                Util.timeQuery(query.limit(limit), tx, "limit " + limit);
            }
        }
    }

    /**
     * single-rule mimicking transitivity test rule defined by two-hop relations
     * Initial data arranged in N x N square grid.
     * <p>
     * Rule:
     * (rel-from:$x, rel-to:$y) isa horizontal;
     * (rel-from:$y, rel-to:$z) isa horizontal;
     * (rel-from:$z, rel-to:$u) isa vertical;
     * (rel-from:$u, rel-to:$v) isa vertical;
     * ->
     * (rel-from:$x, rel-to:$v) isa diagonal;
     * <p>
     * Initial data arranged as follows:
     * <p>
     * a_{i  , j} -  horizontal  - a_{i, j + 1}
     * |                    |
     * vertical             vertical
     * |                    |
     * a_{i+1, j} -  horizontal  - a_{i+1, j+1}
     * <p>
     * i e [0, N)
     * j e [0, N)
     */
    @Test
    public void testDiagonal() {
        int N = 10; //9604
        int limit = 10;

        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());

        DiagonalGraph diagonalGraph = new DiagonalGraph(databaseMgr, database);
        diagonalGraph.load(N, N);
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                // full result
                TypeQLMatch.Unfiltered query = TypeQL.match(TypeQL.parsePatterns("(from: $x, to: $y) isa diagonal;"));
                Util.timeQuery(query, tx, "full");
                Util.timeQuery(query.limit(limit), tx, "limit " + limit);
            }
        }
    }

    /**
     * single-rule mimicking transitivity test rule defined by two-hop relations
     * Initial data arranged in N x N square grid.
     * <p>
     * Rules:
     * (from: $x, to: $y) isa arc;},
     * ->
     * (from: $x, to: $y) isa path;};
     * <p>
     * <p>
     * (from: $x, to: $z) isa path;
     * (from: $z, to: $y) isa path;},
     * ->
     * (from: $x, to: $y) isa path;};
     * <p>
     * Initial data arranged as follows:
     * <p>
     * N - tree heights
     * l - number of links per entity
     * <p>
     * a0
     * /     .   \
     * arc          arc
     * /       .       \
     * a1,1     ...    a1,1^l
     * /   .  \         /    .  \
     * arc   .  arc     arc    .  arc
     * /     .   \       /     .    \
     * a2,1 ...  a2,l  a2,l+1  ...  a2,2^l
     * .             .
     * .             .
     * .             .
     * aN,1    ...  ...  ...  ...  ... ... aN,N^l
     */
    @Test
    public void testPathTree() {
        int N = 5;
        int linksPerEntity = 4;
        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());

        PathTreeGraph pathTreeGraph = new PathTreeGraph(databaseMgr, database);
        pathTreeGraph.load(N, linksPerEntity);
        int answers = 0;
        for (int i = 1; i <= N; i++) answers += Math.pow(linksPerEntity, i);

        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {

                String queryString = "match (from: $x, to: $y) isa path;" +
                        "$x has index 'a0,0';" +
                        "get $y; limit " + answers + ";";

                assertEquals(Util.timeQuery(queryString, tx, "tree").size(), answers);
            }
        }
    }

}
