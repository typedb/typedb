/*
 * Copyright (C) 2020 Grakn Labs
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
 */

package com.vaticle.typedb.core.reasoner.benchmark;

// TODO: Make common base relation type so we don't have so many different role types
// TODO: See if i need to name the anonymous variables
// TODO: See if they rely on inference in match-insert queries

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.schema.Rule;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typeql.lang.TypeQL.var;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class BenchmarkBigIT {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("computation-graph-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB).traceInference(false).explain(true);
    private String database;
    private static CoreDatabaseManager databaseMgr;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        database = "a" + UUID.randomUUID().toString().replaceAll("-", "");
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

    final private Random rand = new Random();

    private void loadOntology(String fileName, TypeDB.Session session) {
        String filePath = "test/integration/reasoner/benchmark/resources/" + fileName;
        try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            tx.query().define(com.vaticle.typedb.core.reasoner.benchmark.Util.parseTQL(filePath).asDefine());
            tx.commit();
        }
    }

    private void loadEntities(String entityLabel, int N, TypeDB.Session session) {
        try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            EntityType entityType = tx.concepts().getEntityType(entityLabel);
            for (int i = 0; i < N; i++) {
                entityType.create();
            }
            tx.commit();
        }
    }

    private void loadRandomisedRelationInstances(String entityLabel, String fromRoleLabel, String toRoleLabel,
                                                 String relationLabel, int N, TypeDB.Session session) {
        try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            UnboundVariable entity = var("e");
            Thing[] instances = transaction.query().match(TypeQL.match(entity.isa(entityLabel)))
                    .map(ans -> ans.get(entity.name()))
                    .toList().toArray(new Thing[0]);

            assertEquals(instances.length, N);
            RelationType relationType = transaction.concepts().getRelationType(relationLabel);
            RoleType fromRole = relationType.getRelates(fromRoleLabel);
            RoleType toRole = relationType.getRelates(toRoleLabel);


            Multimap<Integer, Integer> assignmentMap = HashMultimap.create();
            for (int i = 0; i < N; i++) {
                int from = rand.nextInt(N - 1);
                int to = rand.nextInt(N - 1);
                while (to == from && assignmentMap.get(from).contains(to)) to = rand.nextInt(N - 1);

                Relation rel = relationType.create();
                rel.addPlayer(fromRole, instances[from]);
                rel.addPlayer(toRole, instances[to]);
            }
            transaction.commit();
        }
    }

    private void loadJoinData(int N) {
        try (TypeDB.Session session = schemaSession()) {
            loadOntology("multiJoin.tql", session);
        }

        try (TypeDB.Session session = dataSession()) {
            final int M = N / 5;
            loadEntities("genericEntity", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C2", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C3", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C4", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "D1", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "D2", M, session);
        }
    }

    private void loadTransitivityData(int N) {
        try (TypeDB.Session session = schemaSession()) {
            loadOntology("linearTransitivity.tql", session);
        }
        try (TypeDB.Session session = dataSession()) {
            loadEntities("a-entity", N, session);
            loadRandomisedRelationInstances("a-entity", "from", "to", "Q", N, session);
        }
    }

    private void loadRuleChainData(int N) {
        String entityLabel = "genericEntity";
        String attributeLabel = "index";
        String baseRelationLabel = "relation1";
        String genericRelationLabel = "relation";
        String fromRoleLabel = "fromRole";
        String toRoleLabel = "toRole";

        //load ontology
        try (TypeDB.Session session = schemaSession()) {
            try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {

                AttributeType.String index = transaction.concepts().putAttributeType(attributeLabel, AttributeType.ValueType.STRING).asString();
                EntityType entityType = transaction.concepts().putEntityType(entityLabel);
                entityType.setOwns(index);
                //define N relation types
                for (int i = 1; i <= N; i++) {
                    RelationType relationType = transaction.concepts().putRelationType(genericRelationLabel + i);
                    relationType.setRelates(fromRoleLabel);
                    relationType.setRelates(toRoleLabel);
                    entityType.setPlays(relationType.getRelates(fromRoleLabel));
                    entityType.setPlays(relationType.getRelates(toRoleLabel));
                }

                //define N rules
                for (int i = 2; i <= N; i++) {
                    UnboundVariable fromVar = var("from");
                    UnboundVariable intermedVar = var("intermed");
                    UnboundVariable toVar = var("to");
                    Rule rulePattern = TypeQL.rule("rule" + i).when(
                            TypeQL.and(
                                    var()
                                            .rel(fromRoleLabel, fromVar)
                                            .rel(toRoleLabel, intermedVar)
                                            .isa(baseRelationLabel),
                                    var()
                                            .rel(fromRoleLabel, intermedVar)
                                            .rel(toRoleLabel, toVar)
                                            .isa(genericRelationLabel + (i - 1))
                            )
                    )
                            .then(
                                    var()
                                            .rel(fromRoleLabel, fromVar)
                                            .rel(toRoleLabel, toVar)
                                            .isa(genericRelationLabel + i)

                            );
                    transaction.query().define(TypeQL.define(rulePattern));
                }
                transaction.commit();
            }
        }

        try (TypeDB.Session session = dataSession()) {
            //insert N + 1 entities
            loadEntities(entityLabel, N + 1, session);

            try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                UnboundVariable entityVar = var("e");
                Thing[] instances = transaction.query().match(TypeQL.match(entityVar.isa(entityLabel)))
                        .map(ans -> ans.get(entityVar.name()))
                        .toList().toArray(new Thing[0]);

                RelationType baseRelation = transaction.concepts().getRelationType(baseRelationLabel);
                AttributeType.String index = transaction.concepts().getAttributeType(attributeLabel).asString();
                instances[0].setHas(index.put("first"));

                RoleType fromRole = baseRelation.getRelates(fromRoleLabel);
                RoleType toRole = baseRelation.getRelates(toRoleLabel);
                for (int i = 1; i < instances.length; i++) {
                    Relation rel = baseRelation.create();
                    rel.addPlayer(fromRole, instances[i-1]);
                    rel.addPlayer(toRole, instances[i]);
                    instances[i].setHas(index.put(String.valueOf(i)));
                }
                transaction.commit();
            }
        }
    }

    /**
     * 2-rule transitive test with transitivity expressed in terms of two linear rules.
     * Data arranged randomly with N number of db relation instances.
     */
    @Test
    public void testRandomSetLinearTransitivity() {
        final int N = 1000;
        final int limit = 100;
        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());
        loadTransitivityData(N);

        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                Entity entity = tx.concepts().getEntityType("a-entity").getInstances().next();
                String queryPattern = "(from: $x, to: $y) isa P;";
                String queryString = "match " + queryPattern;
                String subbedQueryString = "match " + queryPattern +
                        " $x iid " + entity.getIID().toHexString() + ";";
                String subbedQueryString2 = "match " + queryPattern +
                        " $y iid " + entity.getIID().toHexString() + ";";
                String limitedQueryString = "match " + queryPattern +
                        " limit " + limit + ";";

                executeQuery(queryString, tx, "full");
                executeQuery(subbedQueryString, tx, "first argument bound");
                executeQuery(subbedQueryString2, tx, "second argument bound");
                executeQuery(limitedQueryString, tx, "limit " + limit);
            }
        }
    }

    /**
     * Scalable multi-join test defined as a non-recursive tree of binary joins,
     * which is expressed using the following inference rules:
     * <p>
     * a(X,Y)  :- b1(X,Z), b2(Z,Y).
     * b1(X,Y) :- c1(X,Z), c2(Z,Y).
     * b2(X,Y) :- c3(X,Z), c4(Z,Y).
     * c1(X,Y) :- d1(X,Z), d2(Z,Y).
     * <p>
     * The base relations, c2, c3, c4, d1 and d2 are randomly generated
     * with 1/5 * N instances (reaching a total of N instances) each defined over 1/5 * N entities.
     * The query is based on the final derived predicate.
     */
    @Test
    public void testMultiJoin() {
        final int N = 100;
        final int limit = 100;
        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());
        loadJoinData(N);

        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                Thing entityId = tx.concepts().getEntityType("genericEntity").getInstances().next();
                String queryPattern = "(fromRole: $x, toRole: $y) isa A;";
                String queryString = "match " + queryPattern;
                String subbedQueryString = "match " +
                        queryPattern +
                        " $x iid " + entityId.getIID().toHexString() + ";";
                String subbedQueryString2 = "match " +
                        queryPattern +
                        " $y iid " + entityId.getIID().toHexString() + ";";
                String limitedQueryString = "match " + queryPattern +
                        " limit " + limit + ";";

                executeQuery(queryString, tx, "full");
                executeQuery(subbedQueryString, tx, "first argument bound");
                executeQuery(subbedQueryString2, tx, "second argument bound");
                executeQuery(limitedQueryString, tx, "limit " + limit);
            }
        }
    }

    /**
     * Scalability test defined in terms of number of rules in the system. Creates a simple rule chain based on join operation on two relations:
     * <p>
     * R_i(x, y) := R_1(x, z), R_{i-1}(z, y);    i e [2, N],
     * <p>
     * initialised with a chain of N x base relation R_1instances.
     * <p>
     * The rules are defined in such a way that relation R_j spans j hops of base relations and has (N + 1 - j) instances.
     * Consequently R_N has a single instance linking the first and last entities in the base relation chain.
     */
    @Test
    public void testJoinRuleChain() {
        final int N = 100;  // Stack-size of 200 causes an overflow
        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());
        loadRuleChainData(N);

        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                Thing firstId = tx.query().match(TypeQL.parseQuery("match $x has index 'first';").asMatch()).next().get("x").asThing();
                Thing lastId = tx.query().match(TypeQL.parseQuery("match $x has index '" + N + "';").asMatch()).next().get("x").asThing();
                String queryPattern = "(fromRole: $x, toRole: $y) isa relation" + N + ";";
                String queryString = "match " + queryPattern;
                String subbedQueryString = "match " + queryPattern +
                        "$x iid " + firstId.getIID().toHexString() + ";";
                String subbedQueryString2 = "match " + queryPattern +
                        "$y iid " + lastId.getIID().toHexString() + ";";
                String limitedQueryString = "match " + queryPattern +
                        " limit 1;";
                assertEquals(1, executeQuery(queryString, tx, "full").size());
                assertEquals(1, executeQuery(subbedQueryString, tx, "first argument bound").size());
                assertEquals(1, executeQuery(subbedQueryString2, tx, "second argument bound").size());
                assertEquals(1, executeQuery(limitedQueryString, tx, "limit ").size());
            }
        }
    }

    @Test
    public void whenQueryingConcurrently_reasonerDoesNotThrowException() throws ExecutionException, InterruptedException {
        final int N = 20;
        System.out.println(new Object() {
        }.getClass().getEnclosingMethod().getName());
        loadRuleChainData(N);

        try (TypeDB.Session session = dataSession()) {
            ExecutorService executor = Executors.newFixedThreadPool(8);
            List<CompletableFuture<Void>> asyncMatches = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                CompletableFuture<Void> asyncMatch = CompletableFuture.supplyAsync(() -> {
                    try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                        int randomRelation = rand.nextInt(N - 1);
                        String queryPattern = "(fromRole: $x, toRole: $y) isa relation" + (randomRelation + 1) + ";";
                        String queryString = "match " + queryPattern;
                        executeQuery(queryString, tx, "full");
                    }
                    return null;
                }, executor);
                asyncMatches.add(asyncMatch);
            }

            CompletableFuture.allOf(asyncMatches.toArray(new CompletableFuture[]{})).get();
        }
    }

    private List<ConceptMap> executeQuery(String query, TypeDB.Transaction transaction, String msg) {
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = (List<ConceptMap>) transaction.query().match(TypeQL.parseQuery(query).asMatch()).toList();
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println(msg + " results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }
}