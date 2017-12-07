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

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknSystemProperty;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.BatchExecutorClient;
import ai.grakn.client.GraknClient;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.test.rule.SessionContext;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import java.io.File;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.SampleKBLoader.randomKeyspace;

public class BenchmarkTests {

    //Needed to start cass depending on profile
    @ClassRule
    public static final SessionContext sessionContext = SessionContext.create();

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTests.class);

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();
    private GraknSession session;
    private Keyspace keyspace;

    @Before
    public void setupSession() {
        keyspace = randomKeyspace();
        this.session = Grakn.session(engine.uri(), keyspace);
    }

    public void load(String fileName, int N){
        GraknClient graknClient = new GraknClient(engine.uri());

        try {
            File graqlFile = new File(GraknSystemProperty.PROJECT_RELATIVE_DIR.value() + "/grakn-test-tools/src/main/graql/" + fileName);
            String s = Files.toString(graqlFile, Charset.forName("UTF-8"));
            GraknTx tx = session.open(GraknTxType.WRITE);
            tx.graql().parser().parseQuery(s).execute();
            tx.commit();
            tx.close();
        } catch (Exception e){
            System.err.println(e);
            System.exit(1);
        }

        try(BatchExecutorClient loader = BatchExecutorClient.newBuilder().taskClient(graknClient).build()){
            for(int i = 0 ; i < N ;i++){
                InsertQuery entityInsert = Graql.insert(var().asUserDefined().isa("a-entity"));
                loader.add(entityInsert, keyspace).subscribe(System.out::println);
            }
        }

        try(BatchExecutorClient loader = BatchExecutorClient.newBuilder().taskClient(graknClient).build()) {
            GraknTx tx = session.open(GraknTxType.READ);
            ConceptId[] aInstances = tx.graql().<GetQuery>parse("match $x isa a-entity; get;").execute().stream()
                    .map(ans -> ans.get("x").getId())
                    .toArray(ConceptId[]::new);

            System.out.println(aInstances.length);
            assert(aInstances.length == N);
            Role Qfrom = tx.getRole("Q-from");
            Role Qto = tx.getRole("Q-to");
            RelationshipType Q = tx.getRelationshipType("Q");

            Random rand = new Random();
            Multimap<Integer, Integer> assignmentMap = HashMultimap.create();
            for (int i = 0; i < N; i++) {
                int from = rand.nextInt(N - 1);
                int to = rand.nextInt(N - 1);
                while (to == from && assignmentMap.get(from).contains(to)) to = rand.nextInt(N - 1);

                Var fromRolePlayer = Graql.var();
                Var toRolePlayer = Graql.var();
                Pattern relationInsert = Graql.var()
                        .rel(Graql.label(Qfrom.getLabel()), fromRolePlayer)
                        .rel(Graql.label(Qto.getLabel()), toRolePlayer)
                        .isa(Graql.label(Q.getLabel()))
                        .and(fromRolePlayer.asUserDefined().id(aInstances[from]))
                        .and(toRolePlayer.asUserDefined().id(aInstances[to]));
                loader.add(Graql.insert(relationInsert.admin().varPatterns()), keyspace).subscribe(System.out::println);
            }
            tx.close();
        }


    }
    /**
     * 2-rule transitive test with transitivity expressed in terms of two linear rules.
     * Data arranged randomly with N number of db relation instances.
     */
    @Test
    public void testRandomSetLinearTransitivity()  {
        final int N = 10000;
        final int limit = 100;
        LOG.debug(new Object(){}.getClass().getEnclosingMethod().getName());
        load("linearTransitivity.gql", N);

        try(GraknTx tx = session.open(GraknTxType.READ)) {

            ConceptId entityId = tx.getEntityType("a-entity").instances().findFirst().get().getId();
            String queryString = "match (P-from: $x, P-to: $y) isa P; get;";
            String subbedQueryString = "match (P-from: $x, P-to: $y) isa P;" +
                    "$x id '" + entityId.getValue() + "';" +
                    "get;";
            String subbedQueryString2 = "match (P-from: $x, P-to: $y) isa P;" +
                    "$y id '" + entityId.getValue() + "';" +
                    "get;";
            String limitedQueryString = "match (P-from: $x, P-to: $y) isa P;" +
                    "limit " + limit + ";" +
                    "get;";

            executeQuery(queryString, tx, "full");
            executeQuery(subbedQueryString, tx, "first argument bound");
            executeQuery(subbedQueryString2, tx, "second argument bound");
            executeQuery(limitedQueryString, tx, "limit " + limit);
        }
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
