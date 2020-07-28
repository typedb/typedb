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

package grakn.core.graql.reasoner.benchmark;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import graql.lang.statement.Statement;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RuleScalingIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Test
    public void ruleScaling() {
        final int N = 60;
        final int populatedChains = 3;
        Session session = server.sessionWithNewKeyspace();
        setup(session, N, populatedChains);

        try( Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            String query = "match " +
                    "$x isa someEntity;" +
                    "(BR-role: $x, BR-anotherRole: $y) isa baseRelation;" +
                    "(ABR-role: $y, ABR-anotherRole: $link) isa anotherBaseRelation;" +
                    "$r (DR-role: $link, DR-anotherRole: $anotherLink) isa derivedRelation;" +
                    "$r has inferredAttribute $value;" +
                    "$r has anotherInferredAttribute $anotherValue;" +
                    "(IR-role: $anotherLink, IR-anotherRole: $index) isa indexingRelation;" +
                    "get;";
            List<ConceptMap> answers = executeQuery(query, tx);
            assertEquals(populatedChains, answers.size());
        }
        session.close();
    }

    private void setup(Session session, int N, int populatedChains){
        setupSchema(session, N);
        setupRules(session, N);


        // note: roles do not have to be shared, specific relation always used to query
        // can generate new roles with relation

        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            for (int k = 0; k < populatedChains; k++) {
                tx.execute(Graql.<GraqlInsert>parse(
                        "insert " +
                                "$x isa someEntity;" +
                                "$y isa intermediateEntity;" +
                                "$link isa intermediateEntity;" +
                                "$index isa intermediateEntity;" +
                                "$anotherLink isa finalEntity;" +
                                "(BR-role: $x, BR-anotherRole: $y) isa baseRelation;" +
                                "(ABR-role: $y, ABR-anotherRole: $link) isa anotherBaseRelation;" +
                                "(SR" + k + "-role: $link, SR" + k + "-anotherRole: $index) isa specificRelation" + k + ";" +
                                "(ASR" + k + "-role: $index, ASR" + k + "-anotherRole: $anotherLink) isa anotherSpecificRelation" + k + ";"
                ));
            }
            tx.commit();
        }
    }

    private void setupSchema(Session session, int N){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.<GraqlDefine>parse(
                    "define " +
                            "baseEntity sub entity," +
                                "plays BR-role, plays BR-anotherRole," +
                                "plays ABR-role, plays ABR-anotherRole," +
                                "plays DR-role, plays DR-anotherRole," +
                                "plays IR-role, plays IR-anotherRole;" +
                            "someEntity sub baseEntity;" +
                            "intermediateEntity sub baseEntity;" +
                            "finalEntity sub baseEntity;" +
                            "baseRelation sub relation, relates BR-role, relates BR-anotherRole;" +
                            "anotherBaseRelation sub relation, relates ABR-role, relates ABR-anotherRole;" +

                            "derivedRelation sub relation, " +
                            "   has inferredAttribute, has anotherInferredAttribute," +
                            "   relates DR-role, relates DR-anotherRole;" +
                            "indexingRelation sub relation, relates IR-role, relates IR-anotherRole;" +
                            "inferredAttribute sub attribute, value string;" +
                            "anotherInferredAttribute sub attribute, value string;"
            ));

            for (int i = 0; i < N; i++) {
                tx.execute(Graql.<GraqlDefine>parse(
                        "define " +
                                "specificRelation" + i + " sub relation," +
                                    "relates SR" + i + "-role," +
                                    "relates SR" + i + "-anotherRole;" +
                                "anotherSpecificRelation" + i + " sub relation," +
                                    "relates ASR" + i + "-role," +
                                    "relates ASR" + i + "-anotherRole;" +
                                "baseEntity sub entity," +
                                    "plays SR" + i + "-role," +
                                    "plays SR" + i + "-anotherRole," +
                                    "plays ASR" + i + "-role," +
                                    "plays ASR" + i + "-anotherRole;"

                ));
            }
            tx.commit();
        }
    }

    private void setupRules(Session session, int N){
        String basePattern =
                "$x isa someEntity;" +
                        "(BR-role: $x, BR-anotherRole: $y) isa baseRelation;" +
                        "(ABR-role: $y, ABR-anotherRole: $link) isa anotherBaseRelation;";

        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            for (int i = 0; i < N; i++) {
                Pattern specificPattern = Graql.parsePattern(
                        "{" +
                                basePattern +
                                "(SR" + i + "-role: $link, SR" + i + "-anotherRole: $index) isa specificRelation" + i + ";" +
                                "(ASR" + i + "-role: $index, ASR" + i + "-anotherRole: $anotherLink) isa anotherSpecificRelation" + i + ";" +
                                "};"
                );
                Statement relationRule = Graql
                        .type("relationRule" + i)
                        .when(Graql.and(specificPattern))
                        .then(Graql.and(Graql.parsePattern("(DR-role: $link, DR-anotherRole: $anotherLink) isa derivedRelation;")));

                tx.execute(Graql.define(relationRule));

                Statement attributeRule = Graql
                        .type("attributeRule" + i)
                        .when(Graql.and(
                                specificPattern,
                                Graql.parsePattern("$r (DR-role: $link, DR-anotherRole: $anotherLink) isa derivedRelation;")
                        ))
                        .then(Graql.and(Graql.parsePattern("$r has inferredAttribute 'inferredValue" + i + "';")));

                tx.execute(Graql.define(attributeRule));

                Statement anotherAttributeRule = Graql
                        .type("anotherAttributeRule" + i)
                        .when(Graql.and(
                                specificPattern,
                                Graql.parsePattern("$r (DR-role: $link, DR-anotherRole: $anotherLink) isa derivedRelation;"),
                                Graql.parsePattern("$r has inferredAttribute 'inferredValue" + i + "';")
                        ))
                        .then(Graql.and(Graql.parsePattern("$r has anotherInferredAttribute 'anotherInferredValue" + i + "';")));

                tx.execute(Graql.define(anotherAttributeRule));

                Statement indexingRelationRule = Graql
                        .type("indexingRelationRule" + i)
                        .when(Graql.and(specificPattern,
                                Graql.parsePattern("$r (DR-role: $link, DR-anotherRole: $anotherLink) isa derivedRelation;"),
                                Graql.parsePattern("$r has inferredAttribute 'inferredValue" + i + "';"),
                                Graql.parsePattern("$r has anotherInferredAttribute 'anotherInferredValue" + i + "';")))
                        .then(Graql.and(Graql.parsePattern("(IR-role: $anotherLink, IR-anotherRole: $index) isa indexingRelation;")));

                tx.execute(Graql.define(indexingRelationRule));

            }
            tx.commit();
        }
    }

    private List<ConceptMap> executeQuery(String queryString, Transaction transaction){
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = transaction.execute(Graql.parse(queryString).asGet());
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("Query results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }
}
