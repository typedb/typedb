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
 *
 */

package grakn.core.test.behaviour.graql;

import com.google.common.collect.Iterators;
import grakn.client.GraknClient;
import grakn.client.answer.Answer;
import grakn.client.answer.ConceptMap;
import grakn.client.concept.Concept;
import grakn.client.concept.type.AttributeType;
import grakn.client.test.behaviour.connection.ConnectionSteps;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlQuery;
import graql.lang.query.GraqlUndefine;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraqlSteps {

    private static Session session = null;
    private static Transaction tx = null;

    private static List<ConceptMap> answers;
    private static String answerConceptKey = null;

    @After
    public void close_transaction() {
        tx.close();
    }

    @Given("transaction is initialised")
    public void transaction_is_initialised() {
        session = Iterators.getOnlyElement(ConnectionSteps.sessions.iterator());
        tx = session.transaction().write();
        assertTrue(tx.isOpen());
    }

    @Given("the integrity is validated")
    public void integrity_is_validated(){

        // TODO

    }

    @Given("graql define")
    public void graql_define(String defineQueryStatements) {
        GraqlDefine graqlQuery = Graql.parse(String.join("\n", defineQueryStatements)).asDefine();
        tx.execute(graqlQuery);
        tx.commit();
        tx = session.transaction().write();
    }

    @Given("graql undefine")
    public void graql_undefine(String undefineQueryStatements) {
        GraqlUndefine graqlQuery = Graql.parse(String.join("\n", undefineQueryStatements)).asUndefine();
        tx.execute(graqlQuery);
        tx.commit();
        tx = session.transaction().write();
    }

    @Given("graql undefine throws")
    public void graql_undefine_throws(String undefineQueryStatements) {
        GraqlUndefine graqlQuery = Graql.parse(String.join("\n", undefineQueryStatements)).asUndefine();
        boolean threw = false;
        try {
            tx.execute(graqlQuery);
            tx.commit();
        } catch (RuntimeException e) {
            threw = true;
        } finally {
            tx.close();
            tx = session.transaction().write();
        }

        assertTrue(threw);
    }

    @Given("graql insert")
    public void graql_insert(String insertQueryStatements) {
        GraqlQuery graqlQuery = Graql.parse(String.join("\n", insertQueryStatements));
        tx.execute(graqlQuery);
        tx.commit();
        tx = session.transaction().write();
    }


    @Given("graql insert throws")
    public void graql_insert_throws(String insertQueryStatements) {
        GraqlQuery graqlQuery = Graql.parse(String.join("\n", insertQueryStatements));
        boolean threw = false;
        try {
            tx.execute(graqlQuery);
            tx.commit();
        } catch (RuntimeException e) {
            threw = true;
        } finally {
            tx.close();
            tx = session.transaction().write();
        }
        assertTrue(threw);
    }

    @When("get answers of graql query")
    public void graql_query(String graqlQueryStatements) {
        GraqlQuery graqlQuery = Graql.parse(String.join("\n", graqlQueryStatements));
        if (graqlQuery instanceof GraqlGet) {
            answers = tx.execute(graqlQuery.asGet());
        } else if (graqlQuery instanceof GraqlInsert) {
            answers = tx.execute(graqlQuery.asInsert());
        } else {
            // TODO specialise exception
            throw new RuntimeException("Only match-get and inserted supported for now");
        }
    }

    @Then("answer size is: {number}")
    public void answer_quantity_assertion(int expectedAnswers) {
        assertEquals(expectedAnswers, answers.size());
    }


    @Then("answer concepts all have key: {word}")
    public void answer_concepts_have_key_labeled(String label) {
        answerConceptKey = label;
    }

    @Then("answer keys are")
    public void answer_keys_are(List<Map<String, String>> conceptKeys) {
      assertEquals(conceptKeys.size(), answers.size());

        for (ConceptMap answer : answers) {

            Map<String, String> answerKeys = new HashMap<>();
            AttributeType.Remote<?> keyType = tx.getAttributeType(answerConceptKey);
            // remap each concept and save its key value into the map from variable to key value
            answer.map().forEach((var, concept) -> {
                            answerKeys.put(var.name(), concept.asThing().asRemote(tx)
                                    .attributes(keyType).findFirst().get().value().toString());
            });

            int matchingAnswers = 0;
            for (Map<String, String> expectedKeys : conceptKeys) {
                if (expectedKeys.equals(answerKeys)) {
                    matchingAnswers++;
                }
            }

            // we expect exactly one matching answer from the expected answer keys
            assertEquals(1, matchingAnswers);
        }
    }

    @Then("answers are labeled")
    public void answers_satisfy_labels(List<Map<String, String>> conceptLabels) {
        assertEquals(conceptLabels.size(), answers.size());

        for (ConceptMap answer : answers) {

            // convert the concept map into a map from variable to type label
            Map<String, String> answerAsLabels = new HashMap<>();
            answer.map().forEach((var, concept) -> answerAsLabels.put(var.name(), concept.asSchemaConcept().label().toString()));

            int matchingAnswers = 0;
            for (Map<String, String> expectedLabels : conceptLabels) {
                if (expectedLabels.equals(answerAsLabels)) {
                    matchingAnswers++;
                }
            }

            // we expect exactly one matching answer from the expected answer set
            assertEquals(1, matchingAnswers);
        }
    }

    @Then("each answer satisfies")
    public void each_answer_satisfies(String templatedGraqlQuery) {
        String templatedQuery = String.join("\n", templatedGraqlQuery);
        for (ConceptMap answer : answers) {
            String query = applyQueryTemplate(templatedQuery, answer);
            GraqlQuery graqlQuery = Graql.parse(query);
            List<? extends Answer> answers = tx.execute(graqlQuery);
            assertEquals(1, answers.size());
        }
    }

    private String applyQueryTemplate(String template, ConceptMap templateFiller) {
        // find shortest matching strings between <>
        Pattern pattern = Pattern.compile("<.+?>");
        Matcher matcher = pattern.matcher(template);

        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find()) {
            String matched = matcher.group(0);
            String requiredVariable = variableFromTemplatePlaceholder(matched.substring(1, matched.length() - 1));
            Concept<?> concept = templateFiller.get(requiredVariable);

            builder.append(template, i, matcher.start());
            if (concept == null) {
                throw new RuntimeException(String.format("No ID available for template placeholder: %s", matched));
            } else {
                String conceptId = concept.id().toString();
                builder.append(conceptId);
            }
            i = matcher.end();
        }
        builder.append(template.substring(i));
        return builder.toString();
    }

    private String variableFromTemplatePlaceholder(String placeholder) {
        if (placeholder.endsWith(".id")) {
            String stripped = placeholder.replace(".id", "");
            String withoutPrefix = stripped.replace("answer.", "");
            return withoutPrefix;
        } else {
            throw new RuntimeException("Cannot replace template not based on ID");
        }
    }
}
