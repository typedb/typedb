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

package grakn.core.graql.reasoner.benchmark;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import grakn.client.GraknClient;
import grakn.core.concept.ConceptId;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlInsert;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static graql.lang.Graql.var;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class BenchmarkBigIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private String keyspace;

    @Before
    public void randomiseKeyspace() {
        this.keyspace = "a"+ UUID.randomUUID().toString().replaceAll("-", "");
    }

    private void loadOntology(String fileName, GraknClient.Session session){
        try {
            InputStream inputStream = new FileInputStream("test-integration/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            GraknClient.Transaction tx = session.transaction().write();
            tx.execute(Graql.parse(s).asDefine());
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private void loadEntities(String entityLabel, int N, GraknClient.Session session){
        try(GraknClient.Transaction transaction = session.transaction().write()){
            for(int i = 0 ; i < N ;i++){
                GraqlInsert entityInsert = Graql.insert(new Statement(new Variable().asUserDefined()).isa(entityLabel));
                transaction.execute(entityInsert);
            }
            transaction.commit();
        }
    }

    private void loadRandomisedRelationInstances(String entityLabel, String fromRoleLabel, String toRoleLabel,
                                                 String relationLabel, int N, GraknClient.Session session){
        try(GraknClient.Transaction transaction = session.transaction().write()) {
            Statement entity = new Statement(new Variable().asUserDefined());
            ConceptId[] instances = transaction.stream(Graql.match(entity.isa(entityLabel)).get())
                    .map(ans -> ans.get(entity.var()).id())
                    .toArray(ConceptId[]::new);

            assertEquals(instances.length, N);
            Role fromRole = transaction.getRole(fromRoleLabel);
            Role toRole = transaction.getRole(toRoleLabel);
            RelationType relationType = transaction.getRelationType(relationLabel);

            Random rand = new Random();
            Multimap<Integer, Integer> assignmentMap = HashMultimap.create();
            for (int i = 0; i < N; i++) {
                int from = rand.nextInt(N - 1);
                int to = rand.nextInt(N - 1);
                while (to == from && assignmentMap.get(from).contains(to)) to = rand.nextInt(N - 1);

                Statement fromRolePlayer = new Statement(new Variable().asUserDefined());
                Statement toRolePlayer = new Statement(new Variable().asUserDefined());
                Pattern relationInsert = Graql.and(
                        var().rel(Graql.type(fromRole.label().getValue()), fromRolePlayer)
                                .rel(Graql.type(toRole.label().getValue()), toRolePlayer)
                                .isa(Graql.type(relationType.label().getValue())),
                        fromRolePlayer.id(instances[from].getValue()),
                        toRolePlayer.id(instances[to].getValue())
                );
                transaction.execute(Graql.insert(relationInsert.statements()));
            }

            transaction.commit();
        }
    }

    private void loadJoinData(int N) {
        try (GraknClient.Session session = new GraknClient(server.grpcUri().toString()).session(keyspace)) {
            final int M = N/5;
            loadOntology("multiJoin.gql", session);
            loadEntities("genericEntity", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C2", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C3", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "C4", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "D1", M, session);
            loadRandomisedRelationInstances("genericEntity", "fromRole", "toRole", "D2", M, session);
        }
    }

    private void loadTransitivityData(int N){
        try (GraknClient.Session session = new GraknClient(server.grpcUri().toString()).session(keyspace)) {
            loadOntology("linearTransitivity.gql", session);
            loadEntities("a-entity", N, session);
            loadRandomisedRelationInstances("a-entity", "Q-from", "Q-to", "Q", N, session);
        }
    }

    private void loadRuleChainData(int N){
        String entityLabel = "genericEntity";
        String attributeLabel = "index";
        String baseRelationLabel = "relation1";
        String genericRelationLabel = "relation";
        String fromRoleLabel = "fromRole";
        String toRoleLabel = "toRole";

        //load ontology
        try (GraknClient.Session session = new GraknClient(server.grpcUri().toString()).session(keyspace)) {
            try (GraknClient.Transaction transaction = session.transaction().write()) {
                Role fromRole = transaction.putRole(fromRoleLabel);
                Role toRole = transaction.putRole(toRoleLabel);
                AttributeType<String> index = transaction.putAttributeType(attributeLabel, AttributeType.DataType.STRING);
                transaction.putEntityType(entityLabel)
                        .plays(fromRole)
                        .plays(toRole)
                        .has(index);

                //define N relation types
                for (int i = 1; i <= N; i++) {
                    transaction.putRelationType(genericRelationLabel + i)
                            .relates(fromRole)
                            .relates(toRole);
                }

                //define N rules
                for (int i = 2; i <= N; i++) {
                    Statement fromVar = new Statement(new Variable().asUserDefined());
                    Statement intermedVar = new Statement(new Variable().asUserDefined());
                    Statement toVar = new Statement(new Variable().asUserDefined());
                    Statement rulePattern = Graql
                            .type("rule" + i)
                            .when(
                                    Graql.and(
                                            Graql.var()
                                                    .rel(Graql.type(fromRole.label().getValue()), fromVar)
                                                    .rel(Graql.type(toRole.label().getValue()), intermedVar)
                                                    .isa(baseRelationLabel),
                                            Graql.var()
                                                    .rel(Graql.type(fromRole.label().getValue()), intermedVar)
                                                    .rel(Graql.type(toRole.label().getValue()), toVar)
                                                    .isa(genericRelationLabel + (i - 1))
                                    )
                            )
                            .then(
                                    Graql.and(
                                            Graql.var()
                                                    .rel(Graql.type(fromRole.label().getValue()), fromVar)
                                                    .rel(Graql.type(toRole.label().getValue()), toVar)
                                                    .isa(genericRelationLabel + i)
                                    )
                            );
                    transaction.execute(Graql.define(rulePattern));
                }
                transaction.commit();
            }

            //insert N + 1 entities
            loadEntities(entityLabel, N+1, session);

            try (GraknClient.Transaction transaction = session.transaction().write()) {
                Statement entityVar = new Statement(new Variable().asUserDefined());
                ConceptId[] instances = transaction.stream(Graql.match(entityVar.isa(entityLabel)).get())
                        .map(ans -> ans.get(entityVar.var()).id())
                        .toArray(ConceptId[]::new);

                RelationType baseRelation = transaction.getRelationType(baseRelationLabel);
                Role fromRole = transaction.getRole(fromRoleLabel);
                Role toRole = transaction.getRole(toRoleLabel);
                transaction.execute(
                        Graql.insert(
                                new Statement(new Variable().asUserDefined())
                                        .has(attributeLabel, "first")
                                        .id(instances[0].getValue())
                                        .statements()
                        )
                );

                for(int i = 1; i < instances.length; i++){
                    Statement fromRolePlayer = new Statement(new Variable().asUserDefined());
                    Statement toRolePlayer = new Statement(new Variable().asUserDefined());

                    Pattern relationInsert = Graql.and(
                            var().rel(Graql.type(fromRole.label().getValue()), fromRolePlayer)
                                    .rel(Graql.type(toRole.label().getValue()), toRolePlayer)
                                    .isa(Graql.type(baseRelation.label().getValue())),
                            fromRolePlayer.id(instances[i - 1].getValue()),
                            toRolePlayer.id(instances[i].getValue())
                    );
                    transaction.execute(Graql.insert(relationInsert.statements()));

                    Pattern resourceInsert = new Statement(new Variable().asUserDefined())
                            .has(attributeLabel, String.valueOf(i))
                            .id(instances[i].getValue());
                    transaction.execute(Graql.insert(resourceInsert.statements()));
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
    public void testRandomSetLinearTransitivity()  {
        final int N = 200;
        final int limit = 100;
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
        loadTransitivityData(N);

        try (GraknClient.Session session = new GraknClient(server.grpcUri().toString()).session(keyspace)) {
            try(GraknClient.Transaction tx = session.transaction().read()) {
                ConceptId entityId = tx.getEntityType("a-entity").instances().findFirst().get().id();
                String queryPattern = "(P-from: $x, P-to: $y) isa P;";
                String queryString = "match " + queryPattern + " get;";
                String subbedQueryString = "match " + queryPattern +
                        " $x id '" + entityId.getValue() + "';" +
                        " get;";
                String subbedQueryString2 = "match " + queryPattern +
                        " $y id '" + entityId.getValue() + "';" +
                        " get;";
                String limitedQueryString = "match " + queryPattern +
                        " get; limit " + limit + ";";

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

        try (GraknClient.Session session = new GraknClient(server.grpcUri().toString()).session(keyspace)) {
            try(GraknClient.Transaction tx = session.transaction().read()) {
                ConceptId entityId = tx.getEntityType("genericEntity").instances().findFirst().get().id();
                String queryPattern = "(fromRole: $x, toRole: $y) isa A;";
                String queryString = "match " + queryPattern + " get;";
                String subbedQueryString = "match " +
                        queryPattern +
                        " $x id '" + entityId.getValue() + "';" +
                        " get;";
                String subbedQueryString2 = "match " +
                        queryPattern +
                        " $y id '" + entityId.getValue() + "';" +
                        " get;";
                String limitedQueryString = "match " + queryPattern +
                        " get; limit " + limit + ";";

                executeQuery(queryString, tx, "full");
                executeQuery(subbedQueryString, tx, "first argument bound");
                executeQuery(subbedQueryString2, tx, "second argument bound");
                executeQuery(limitedQueryString, tx, "limit " + limit);
            }
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
        final int N = 20; // TODO: Increase this number again to > 100, once we fix issue #4545
        System.out.println(new Object() {}.getClass().getEnclosingMethod().getName());
        loadRuleChainData(N);

        try (GraknClient.Session session = new GraknClient(server.grpcUri().toString()).session(keyspace)) {
            try(GraknClient.Transaction tx = session.transaction().read()) {
                ConceptId firstId = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x has index 'first';get;").asGet())).get("x").id();
                ConceptId lastId = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x has index '" + N + "';get;").asGet())).get("x").id();
                String queryPattern = "(fromRole: $x, toRole: $y) isa relation" + N + ";";
                String queryString = "match " + queryPattern + " get;";
                String subbedQueryString = "match " + queryPattern +
                        "$x id '" + firstId.getValue() + "';" +
                        "get;";
                String subbedQueryString2 = "match " + queryPattern +
                        "$y id '" + lastId.getValue() + "';" +
                        "get;";
                String limitedQueryString = "match " + queryPattern +
                        "get; limit 1;";
                assertEquals(1, executeQuery(queryString, tx, "full").size());
                assertEquals(1, executeQuery(subbedQueryString, tx, "first argument bound").size());
                assertEquals(1, executeQuery(subbedQueryString2, tx, "second argument bound").size());
                 assertEquals(1, executeQuery(limitedQueryString, tx, "limit ").size());
            }
        }
    }

    private List<ConceptMap> executeQuery(String queryString, GraknClient.Transaction transaction, String msg){
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = transaction.execute(Graql.parse(queryString).asGet());
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println(msg + " results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }
}