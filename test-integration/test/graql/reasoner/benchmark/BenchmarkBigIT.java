/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.batch.BatchExecutorClient;
import ai.grakn.batch.GraknClient;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import ai.grakn.test.rule.ServerContext;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("CheckReturnValue")
public class BenchmarkBigIT {

    public static EmbeddedCassandraContext cassandraContext = new EmbeddedCassandraContext();

    public static final ServerContext server = new ServerContext();

    @ClassRule
    public static RuleChain chain = RuleChain
            .outerRule(cassandraContext)
            .around(server);

    private GraknSession session;
    private Keyspace keyspace;

    @Before
    public void setupSession() {
        this.session = server.sessionWithNewKeyspace();
        this.keyspace = session.keyspace();
    }

    @After
    public void tearDown(){
        this.session.close();
    }

    private void loadOntology(String fileName, GraknSession session){
        try {
            InputStream inputStream = BenchmarkBigIT.class.getClassLoader().getResourceAsStream("test-integration/test/graql/reasoner/benchmark/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            GraknTx tx = session.transaction(GraknTxType.WRITE);
            tx.graql().parser().parseQuery(s).execute();
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private void loadEntities(String entityLabel, int N, GraknClient graknClient, Keyspace keyspace){
        try(BatchExecutorClient loader = BatchExecutorClient.newBuilder().taskClient(graknClient).build()){
            for(int i = 0 ; i < N ;i++){
                InsertQuery entityInsert = Graql.insert(var().asUserDefined().isa(entityLabel));
                loader.add(entityInsert, keyspace);
            }
        }
    }

    private void loadRandomisedRelationInstances(String entityLabel, String fromRoleLabel, String toRoleLabel, String relationLabel, int N,
                                                 GraknSession session, GraknClient graknClient, Keyspace keyspace){
        try(BatchExecutorClient loader = BatchExecutorClient.newBuilder().taskClient(graknClient).build()) {
            GraknTx tx = session.transaction(GraknTxType.READ);
            Var entityVar = var().asUserDefined();
            ConceptId[] instances = tx.graql().match(entityVar.isa(entityLabel)).get().execute().stream()
                    .map(ans -> ans.get(entityVar).id())
                    .toArray(ConceptId[]::new);

            assertEquals(instances.length, N);
            Role fromRole = tx.getRole(fromRoleLabel);
            Role toRole = tx.getRole(toRoleLabel);
            RelationshipType relationType = tx.getRelationshipType(relationLabel);

            Random rand = new Random();
            Multimap<Integer, Integer> assignmentMap = HashMultimap.create();
            for (int i = 0; i < N; i++) {
                int from = rand.nextInt(N - 1);
                int to = rand.nextInt(N - 1);
                while (to == from && assignmentMap.get(from).contains(to)) to = rand.nextInt(N - 1);

                Var fromRolePlayer = Graql.var();
                Var toRolePlayer = Graql.var();
                Pattern relationInsert = Graql.var()
                        .rel(Graql.label(fromRole.label()), fromRolePlayer)
                        .rel(Graql.label(toRole.label()), toRolePlayer)
                        .isa(Graql.label(relationType.label()))
                        .and(fromRolePlayer.asUserDefined().id(instances[from]))
                        .and(toRolePlayer.asUserDefined().id(instances[to]));
                loader.add(Graql.insert(relationInsert.admin().varPatterns()), keyspace);
            }
            tx.close();
        }
    }

    private void loadJoinData(int N) {
        final GraknClient graknClient = GraknClient.of(server.uri());
        final int M = N/5;
        loadOntology("multiJoin.gql", session);
        loadEntities("genericEntity", M, graknClient, keyspace);
        loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C2", M, session, graknClient, keyspace);
        loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C3", M, session, graknClient, keyspace);
        loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C4", M, session, graknClient, keyspace);
        loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "D1", M, session, graknClient, keyspace);
        loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "D2", M, session, graknClient, keyspace);
    }

    private void loadTransitivityData(int N){
        final GraknClient graknClient = GraknClient.of(server.uri());
        loadOntology("linearTransitivity.gql", session);
        loadEntities("a-entity", N, graknClient, keyspace);
        loadRandomisedRelationInstances("a-entity", "Q-from", "Q-to", "Q", N, session, graknClient, keyspace);
    }

    private void loadRuleChainData(int N){
        final GraknClient graknClient = GraknClient.of(server.uri());
        String entityLabel = "genericEntity";
        String attributeLabel = "index";
        String baseRelationLabel = "relation1";
        String genericRelationLabel = "relation";
        String fromRoleLabel = "fromRole";
        String toRoleLabel = "toRole";

        //load ontology
        try(GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            Role fromRole = tx.putRole(fromRoleLabel);
            Role toRole = tx.putRole(toRoleLabel);
            AttributeType<String> index = tx.putAttributeType(attributeLabel, AttributeType.DataType.STRING);
            tx.putEntityType(entityLabel)
                    .plays(fromRole)
                    .plays(toRole)
                    .has(index);

            //define N relation types
            for (int i = 1; i <= N; i++) {
                tx.putRelationshipType(genericRelationLabel + i)
                        .relates(fromRole)
                        .relates(toRole);
            }

            //define N rules
            for (int i = 2; i <= N; i++) {
                Var fromVar = Graql.var().asUserDefined();
                Var intermedVar = Graql.var().asUserDefined();
                Var toVar = Graql.var().asUserDefined();
                VarPattern rulePattern = Graql
                        .label("rule" + i)
                        .when(
                                Graql.and(
                                        Graql.var()
                                                .rel(Graql.label(fromRole.label()), fromVar)
                                                .rel(Graql.label(toRole.label()), intermedVar)
                                                .isa(baseRelationLabel),
                                        Graql.var()
                                                .rel(Graql.label(fromRole.label()), intermedVar)
                                                .rel(Graql.label(toRole.label()), toVar)
                                                .isa(genericRelationLabel + (i - 1))
                                )
                        )
                        .then(
                                Graql.and(
                                        Graql.var()
                                                .rel(Graql.label(fromRole.label()), fromVar)
                                                .rel(Graql.label(toRole.label()), toVar)
                                                .isa(genericRelationLabel + i)
                                )
                        );
                tx.graql().define(rulePattern).execute();
            }
            tx.commit();
        }

        //insert N + 1 entities
        loadEntities(entityLabel, N+1, graknClient, keyspace);

        //load initial relation instances
        try(BatchExecutorClient loader = BatchExecutorClient.newBuilder().taskClient(graknClient).build()){
            try(GraknTx tx = session.transaction(GraknTxType.READ)) {
                Var entityVar = var().asUserDefined();
                ConceptId[] instances = tx.graql().match(entityVar.isa(entityLabel)).get().execute().stream()
                        .map(ans -> ans.get(entityVar).id())
                        .toArray(ConceptId[]::new);

                RelationshipType baseRelation = tx.getRelationshipType(baseRelationLabel);
                Role fromRole = tx.getRole(fromRoleLabel);
                Role toRole = tx.getRole(toRoleLabel);
                loader.add(Graql.insert(
                        Graql.var().asUserDefined()
                                .has(attributeLabel, "first")
                                .id(instances[0])
                                .admin().varPatterns()
                ), keyspace);

                for(int i = 1; i < instances.length; i++){
                    Var fromRolePlayer = Graql.var();
                    Var toRolePlayer = Graql.var();

                    Pattern relationInsert = Graql.var()
                            .rel(Graql.label(fromRole.label()), fromRolePlayer)
                            .rel(Graql.label(toRole.label()), toRolePlayer)
                            .isa(Graql.label(baseRelation.label()))
                            .and(fromRolePlayer.asUserDefined().id(instances[i - 1]))
                            .and(toRolePlayer.asUserDefined().id(instances[i]));
                    loader.add(Graql.insert(relationInsert.admin().varPatterns()), keyspace);

                    Pattern resourceInsert = Graql.var().asUserDefined()
                            .has(attributeLabel, String.valueOf(i))
                            .id(instances[i]);
                    loader.add(Graql.insert(resourceInsert.admin().varPatterns()), keyspace);
                }
            }
        }
    }

    /**
     * 2-rule transitive test with transitivity expressed in terms of two linear rules.
     * Data arranged randomly with N number of db relation instances.
     */
    @Test
    public void testRandomSetLinearTransitivity()  {
        final int N = 2000;
        final int limit = 100;
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
        loadTransitivityData(N);

        try(GraknTx tx = session.transaction(GraknTxType.READ)) {
            ConceptId entityId = tx.getEntityType("a-entity").instances().findFirst().get().id();
            String queryPattern = "(P-from: $x, P-to: $y) isa P;";
            String queryString = "match " + queryPattern + " get;";
            String subbedQueryString = "match " +
                    queryPattern +
                    "$x id '" + entityId.getValue() + "';" +
                    "get;";
            String subbedQueryString2 = "match " +
                    queryPattern +
                    "$y id '" + entityId.getValue() + "';" +
                    "get;";
            String limitedQueryString = "match " +
                    queryPattern +
                    "limit " + limit + ";" +
                    "get;";

            executeQuery(queryString, tx, "full");
            executeQuery(subbedQueryString, tx, "first argument bound");
            executeQuery(subbedQueryString2, tx, "second argument bound");
            executeQuery(limitedQueryString, tx, "limit " + limit);
        }
    }

    /**
     * Scalable multi-join test defined as a non-recursive tree of binary joins,
     * which is expressed using the following inference rules:
     *
     * a(X,Y)  :- b1(X,Z), b2(Z,Y).
     * b1(X,Y) :- c1(X,Z), c2(Z,Y).
     * b2(X,Y) :- c3(X,Z), c4(Z,Y).
     * c1(X,Y) :- d1(X,Z), d2(Z,Y).
     *
     * The base relations, c2, c3, c4, d1 and d2 are randomly generated
     * with 1/5 * N instances (reaching a total of N instances) each defined over 1/5 * N entities.
     * The query is based on the final derived predicate.
     */
    @Test
    public void testMultiJoin()  {
        final int N = 100;
        final int limit = 100;
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
        loadJoinData(N);

        try(GraknTx tx = session.transaction(GraknTxType.READ)) {
            ConceptId entityId = tx.getEntityType("genericEntity").instances().findFirst().get().id();
            String queryPattern = "(fromRole: $x, toRole: $y) isa A;";
            String queryString = "match " + queryPattern + " get;";
            String subbedQueryString = "match " +
                    queryPattern +
                    "$x id '" + entityId.getValue() + "';" +
                    "get;";
            String subbedQueryString2 = "match " +
                    queryPattern +
                    "$y id '" + entityId.getValue() + "';" +
                    "get;";
            String limitedQueryString = "match " +
                    queryPattern +
                    "limit " + limit + ";" +
                    "get;";

            executeQuery(queryString, tx, "full");
            executeQuery(subbedQueryString, tx, "first argument bound");
            executeQuery(subbedQueryString2, tx, "second argument bound");
            executeQuery(limitedQueryString, tx, "limit " + limit);
        }
    }

    /**
     * Scalability test defined in terms of number of rules in the system. Creates a simple rule chain based on join operation on two relations:
     *
     * R_i(x, y) := R_1(x, z), R_{i-1}(z, y);    i e [2, N],
     *
     * initialised with a chain of N x base relation R_1instances.
     *
     * The rules are defined in such a way that relation R_j spans j hops of base relations and has (N + 1 - j) instances.
     * Consequently R_N has a single instance linking the first and last entities in the base relation chain.
     */
    @Test
    public void testJoinRuleChain() {
        final int N = 100;
        System.out.println(new Object() {}.getClass().getEnclosingMethod().getName());
        loadRuleChainData(N);

        try(GraknTx tx = session.transaction(GraknTxType.READ)) {
            ConceptId firstId = Iterables.getOnlyElement(tx.graql().<GetQuery>parse("match $x has index 'first';get;").execute()).get("x").id();
            ConceptId lastId = Iterables.getOnlyElement(tx.graql().<GetQuery>parse("match $x has index '" + N + "';get;").execute()).get("x").id();
            String queryPattern = "(fromRole: $x, toRole: $y) isa relation" + N + ";";
            String queryString = "match " + queryPattern + " get;";
            String subbedQueryString = "match " +
                    queryPattern +
                    "$x id '" + firstId.getValue() + "';" +
                    "get;";
            String subbedQueryString2 = "match " +
                    queryPattern +
                    "$y id '" + lastId.getValue() + "';" +
                    "get;";
            String limitedQueryString = "match " +
                    queryPattern +
                    "limit 1;" +
                    "get;";
            assertEquals(executeQuery(queryString, tx, "full").size(), 1);
            assertEquals(executeQuery(subbedQueryString, tx, "first argument bound").size(), 1);
            assertEquals(executeQuery(subbedQueryString2, tx, "second argument bound").size(), 1);
            assertEquals(executeQuery(limitedQueryString, tx, "limit ").size(), 1);
        }
    }

    private List<ConceptMap> executeQuery(String queryString, GraknTx graph, String msg){
        return executeQuery(graph.graql().infer(true).parse(queryString), msg);
    }

    private List<ConceptMap> executeQuery(GetQuery query, String msg) {
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = query.execute();
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println(msg + " results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }
}