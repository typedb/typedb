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
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import graql.lang.statement.Statement;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RuleScalingIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Test
    public void ruleScaling() {
        final int N = 60;
        final int populatedChains = 3;
        Session session = server.sessionWithNewKeyspace();

        try(Transaction tx = session.writeTransaction()) {
            tx.execute(Graql.<GraqlDefine>parse(
                    "define " +
                            "baseEntity sub entity, plays someRole, plays anotherRole;" +
                            "someEntity sub baseEntity;" +
                            "intermediateEntity sub baseEntity;" +
                            "finalEntity sub baseEntity;" +
                            "baseRelation sub relation, relates someRole, relates anotherRole;" +
                            "anotherBaseRelation sub relation, relates someRole, relates anotherRole;" +

                            "inferredRelation sub relation, " +
                            "   has inferredAttribute, has anotherInferredAttribute," +
                            "   relates someRole, relates anotherRole;" +
                            "indexingRelation sub relation, relates someRole, relates anotherRole;" +
                            "inferredAttribute sub attribute, datatype string;" +
                            "anotherInferredAttribute sub attribute, datatype string;"
            ));

            for (int i = 0; i < N; i++) {
                tx.execute(Graql.<GraqlDefine>parse(
                        "define " +
                                "specificRelation" + i + " sub relation, relates someRole, relates anotherRole;" +
                                "anotherSpecificRelation" + i + " sub relation, relates someRole, relates anotherRole;"
                ));
            }
            tx.commit();
        }

        String basePattern =
                "$x isa someEntity;" +
                        "(someRole: $x, anotherRole: $y) isa baseRelation;" +
                        "(someRole: $y, anotherRole: $link) isa anotherBaseRelation;";

        try(Transaction tx = session.writeTransaction()) {
            for (int i = 0; i < N; i++) {
                Pattern specificPattern = Graql.parsePattern(
                        "{" +
                                basePattern +
                                "(someRole: $link, anotherRole: $index) isa specificRelation" + i + ";" +
                                "(someRole: $index, anotherRole: $anotherLink) isa anotherSpecificRelation" + i + ";" +
                                "};"
                );
                Statement relationRule = Graql
                        .type("relationRule" + i)
                        .when(Graql.and(specificPattern))
                        .then(Graql.and(Graql.parsePattern("(someRole: $link, anotherRole: $anotherLink) isa inferredRelation;")));

                tx.execute(Graql.define(relationRule));

                Statement attributeRule = Graql
                        .type("attributeRule" + i)
                        .when(Graql.and(
                                specificPattern,
                                Graql.parsePattern("$r (someRole: $link, anotherRole: $anotherLink) isa inferredRelation;")
                        ))
                        .then(Graql.and(Graql.parsePattern("$r has inferredAttribute 'inferredValue" + i + "';")));

                tx.execute(Graql.define(attributeRule));

                Statement anotherAttributeRule = Graql
                        .type("anotherAttributeRule" + i)
                        .when(Graql.and(
                                specificPattern,
                                Graql.parsePattern("$r (someRole: $link, anotherRole: $anotherLink) isa inferredRelation;"),
                                Graql.parsePattern("$r has inferredAttribute 'inferredValue" + i + "';")
                        ))
                        .then(Graql.and(Graql.parsePattern("$r has anotherInferredAttribute 'anotherInferredValue" + i + "';")));

                tx.execute(Graql.define(anotherAttributeRule));

                Statement indexingRelationRule = Graql
                        .type("indexingRelationRule" + i)
                        .when(Graql.and(specificPattern,
                                Graql.parsePattern("$r (someRole: $link, anotherRole: $anotherLink) isa inferredRelation;"),
                                Graql.parsePattern("$r has inferredAttribute 'inferredValue" + i + "';"),
                                Graql.parsePattern("$r has anotherInferredAttribute 'anotherInferredValue" + i + "';")))
                        .then(Graql.and(Graql.parsePattern("(someRole: $anotherLink, anotherRole: $index) isa indexingRelation;")));

                tx.execute(Graql.define(indexingRelationRule));

            }
            tx.commit();
        }

        try(Transaction tx = session.writeTransaction()) {
            for (int k = 0; k < populatedChains; k++) {
                tx.execute(Graql.<GraqlInsert>parse(
                        "insert " +
                                "$x isa someEntity;" +
                                "$y isa intermediateEntity;" +
                                "$link isa intermediateEntity;" +
                                "$index isa intermediateEntity;" +
                                "$anotherLink isa finalEntity;" +
                                "(someRole: $x, anotherRole: $y) isa baseRelation;" +
                                "(someRole: $y, anotherRole: $link) isa anotherBaseRelation;" +
                                "(someRole: $link, anotherRole: $index) isa specificRelation" + k + ";" +
                                "(someRole: $index, anotherRole: $anotherLink) isa anotherSpecificRelation" + k + ";"
                ));
            }
            tx.commit();
        }

        try( Transaction tx = session.writeTransaction()) {
            String query = "match " +
                    "$x isa someEntity;" +
                    "(someRole: $x, anotherRole: $y) isa baseRelation;" +
                    "(someRole: $y, anotherRole: $link) isa anotherBaseRelation;" +
                    "$r (someRole: $link, anotherRole: $anotherLink) isa inferredRelation;" +
                    "$r has inferredAttribute $value;" +
                    "$r has anotherInferredAttribute $anotherValue;" +
                    "(someRole: $anotherLink, anotherRole: $index) isa indexingRelation;" +
                    "get;";
            List<ConceptMap> answers = executeQuery(query, tx);
            assertEquals(populatedChains, answers.size());
        }
        session.close();

    }

    private List<ConceptMap> executeQuery(String queryString, Transaction transaction){
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = transaction.execute(Graql.parse(queryString).asGet());
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("Query results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }
}
