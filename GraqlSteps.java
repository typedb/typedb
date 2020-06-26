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
import grakn.core.concept.answer.Answer;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.concept.answer.Numeric;
import grakn.core.graql.reasoner.explanation.RuleExplanation;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.connection.ConnectionSteps;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlQuery;
import graql.lang.query.GraqlUndefine;
import graql.lang.statement.Variable;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GraqlSteps {

    private static Session session = null;
    private static Transaction tx = null;

    private static List<ConceptMap> answers;
    private static List<Numeric> numericAnswers;
    private static List<AnswerGroup<ConceptMap>> answerGroups;
    private static List<AnswerGroup<Numeric>> numericAnswerGroups;
    HashMap<String, UniquenessCheck> identifierChecks = new HashMap<>();
    HashMap<String, String> groupOwnerIdentifiers = new HashMap<>();
    private Map<String, Map<String, String>> rules;

    @After
    public void close_transaction() {
        tx.close();
    }

    @Given("transaction is initialised")
    public void transaction_is_initialised() {
        session = Iterators.getOnlyElement(ConnectionSteps.sessions.iterator());
        tx = session.transaction(Transaction.Type.WRITE);
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
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @Given("graql define without commit")
    public void graql_define_without_commit(String defineQueryStatements) {
        GraqlDefine graqlQuery = Graql.parse(String.join("\n", defineQueryStatements)).asDefine();
        tx.execute(graqlQuery);
    }

    @Given("graql define throws")
    public void graql_define_throws(String defineQueryStatements) {
        boolean threw = false;
        try {
            GraqlDefine graqlQuery = Graql.parse(String.join("\n", defineQueryStatements)).asDefine();
            tx.execute(graqlQuery);
            tx.commit();
        } catch (RuntimeException e) {
            threw = true;
        } finally {
            tx.close();
            tx = session.transaction(Transaction.Type.WRITE);
        }

        assertTrue(threw);
    }

    @Given("graql undefine")
    public void graql_undefine(String undefineQueryStatements) {
        GraqlUndefine graqlQuery = Graql.parse(String.join("\n", undefineQueryStatements)).asUndefine();
        tx.execute(graqlQuery);
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @Given("graql undefine without commit")
    public void graql_undefine_without_commit(String undefineQueryStatements) {
        GraqlUndefine graqlQuery = Graql.parse(String.join("\n", undefineQueryStatements)).asUndefine();
        tx.execute(graqlQuery);
    }

    @Given("graql undefine throws")
    public void graql_undefine_throws(String undefineQueryStatements) {
        boolean threw = false;
        try {
            GraqlUndefine graqlQuery = Graql.parse(String.join("\n", undefineQueryStatements)).asUndefine();
            tx.execute(graqlQuery);
            tx.commit();
        } catch (RuntimeException e) {
            threw = true;
        } finally {
            tx.close();
            tx = session.transaction(Transaction.Type.WRITE);
        }

        assertTrue(threw);
    }

    @Given("graql insert")
    public void graql_insert(String insertQueryStatements) {
        GraqlQuery graqlQuery = Graql.parse(String.join("\n", insertQueryStatements));
        tx.execute(graqlQuery, true, true); // always use inference and have explanations
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @Given("graql insert without commit")
    public void graql_insert_without_commit(String insertQueryStatements) {
        GraqlQuery graqlQuery = Graql.parse(String.join("\n", insertQueryStatements));
        tx.execute(graqlQuery);
    }

    @Given("graql insert throws")
    public void graql_insert_throws(String insertQueryStatements) {
        boolean threw = false;
        try {
            GraqlQuery graqlQuery = Graql.parse(String.join("\n", insertQueryStatements));
            tx.execute(graqlQuery);
            tx.commit();
        } catch (RuntimeException e) {
            threw = true;
        } finally {
            tx.close();
            tx = session.transaction(Transaction.Type.WRITE);
        }
        assertTrue(threw);
    }

    @Given("graql delete")
    public void graql_delete(String deleteQueryStatements) {
        GraqlQuery graqlQuery = Graql.parse(String.join("\n", deleteQueryStatements));
        tx.execute(graqlQuery);
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
    }


    @Given("graql delete throws")
    public void graql_delete_throws(String deleteQueryStatements) {
        boolean threw = false;
        try {
            GraqlQuery graqlQuery = Graql.parse(String.join("\n", deleteQueryStatements));
            tx.execute(graqlQuery);
            tx.commit();
        } catch (RuntimeException e) {
            threw = true;
        } finally {
            tx.close();
            tx = session.transaction(Transaction.Type.WRITE);
        }
        assertTrue(threw);
    }

    @When("get answers of graql insert")
    public void get_answers_of_graql_insert(String graqlQueryStatements) {
        GraqlInsert graqlQuery = Graql.parse(String.join("\n", graqlQueryStatements)).asInsert();
        // Erase answers from previous steps to avoid polluting the result space
        answers = null;
        numericAnswers = null;
        answerGroups = null;
        numericAnswerGroups = null;

        answers = tx.execute(graqlQuery, true, true);
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @When("get answers of graql query")
    public void graql_query(String graqlQueryStatements) {
        GraqlQuery graqlQuery = Graql.parse(String.join("\n", graqlQueryStatements));
        // Erase answers from previous steps to avoid polluting the result space
        answers = null;
        numericAnswers = null;
        answerGroups = null;
        numericAnswerGroups = null;
        if (graqlQuery instanceof GraqlGet) {
            answers = tx.execute(graqlQuery.asGet(), true, true); // always use inference and have explanations
        } else if (graqlQuery instanceof GraqlInsert) {
            throw new ScenarioDefinitionException("Insert is not supported; use `get answers of graql insert` instead");
        } else if (graqlQuery instanceof GraqlGet.Aggregate) {
            numericAnswers = tx.execute(graqlQuery.asGetAggregate());
        } else if (graqlQuery instanceof GraqlGet.Group) {
            answerGroups = tx.execute(graqlQuery.asGetGroup());
        } else if (graqlQuery instanceof GraqlGet.Group.Aggregate) {
            numericAnswerGroups = tx.execute(graqlQuery.asGetGroupAggregate());
        } else {
            throw new ScenarioDefinitionException("Only match-get, aggregate, group and group aggregate supported for now");
        }
    }

    @When("graql get throws")
    public void graql_get_throws(String graqlQueryStatements) {
        boolean threw = true;
        try {
            GraqlQuery graqlQuery = Graql.parse(String.join("\n", graqlQueryStatements));
            if (graqlQuery instanceof GraqlGet) {
                tx.execute(graqlQuery.asGet());
            } else if (graqlQuery instanceof GraqlGet.Aggregate) {
                tx.execute(graqlQuery.asGetAggregate());
            } else if (graqlQuery instanceof GraqlGet.Group) {
                tx.execute(graqlQuery.asGetGroup());
            } else if (graqlQuery instanceof GraqlGet.Group.Aggregate) {
                tx.execute(graqlQuery.asGetGroupAggregate());
            } else {
                throw new ScenarioDefinitionException("Expected a match-get, aggregate, group or group aggregate query, but got a different query type");
            }
            tx.commit();
        } catch (RuntimeException e) {
            threw = true;
        } finally {
            tx.close();
            tx = session.transaction(Transaction.Type.WRITE);
        }
        assertTrue(threw);
    }

    @Then("answer size is: {number}")
    public void answer_quantity_assertion(int expectedAnswers) {
        assertEquals(expectedAnswers, answers.size());
    }


    @Then("concept identifiers are")
    public void concept_identifiers_are(Map<String, Map<String, String>> identifiers) {
        for (Map.Entry<String, Map<String, String>> entry : identifiers.entrySet()) {
            String identifier = entry.getKey();
            String check = entry.getValue().get("check");
            String value = entry.getValue().get("value");

            switch (check) {
                case "key":
                    identifierChecks.put(identifier, new KeyUniquenessCheck(value));
                    break;
                case "value":
                    identifierChecks.put(identifier, new ValueUniquenessCheck(value));
                    break;
                case "label":
                    identifierChecks.put(identifier, new LabelUniquenessCheck(value));
                    break;
                default:
                    throw new ScenarioDefinitionException(String.format("Unrecognised identifier check \"%s\"", check));
            }
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


    @Then("uniquely identify answer concepts")
    public void uniquely_identify_answer_concepts(List<Map<String, String>> answersIdentifiers) {
        assertEquals(
                String.format("The number of identifier entries (rows) should match the number of answers, but found %d identifier entries and %d answers",
                        answersIdentifiers.size(), answers.size()),
                answersIdentifiers.size(), answers.size()
        );

        for (ConceptMap answer : answers) {
            List<Map<String, String>> matchingIdentifiers1 = new ArrayList<>();

            for (Map<String, String> answerIdentifiers : answersIdentifiers) {

                if (matchAnswer(answerIdentifiers, answer)) {
                    matchingIdentifiers1.add(answerIdentifiers);
                }
            }
            assertEquals(
                    String.format("An identifier entry (row) should match 1-to-1 to an answer, but there were %d matching identifier entries for answer with variables %s",
                            matchingIdentifiers1.size(), answer.map().keySet().toString()),
                    1, matchingIdentifiers1.size()
            );
        }
    }

    @Then("order of answer concepts is")
    public void order_of_answer_concepts_is(List<Map<String, String>> answersIdentifiers) {
        assertEquals(
                String.format("The number of identifier entries (rows) should match the number of answers, but found %d identifier entries and %d answers",
                        answersIdentifiers.size(), answers.size()),
                answersIdentifiers.size(), answers.size()
        );

        for (int i = 0; i < answers.size(); i++) {
            final ConceptMap answer = answers.get(i);
            final Map<String, String> answerIdentifiers = answersIdentifiers.get(i);
            assertTrue(
                    String.format("The answer at index %d does not match the identifier entry (row) at index %d", i, i),
                    matchAnswer(answerIdentifiers, answer)
            );
        }
    }

    @Then("aggregate value is: {double}")
    public void aggregate_value_is(double expectedAnswer) {
        assertNotNull("The last executed query was not an aggregate query", numericAnswers);
        assertEquals(String.format("Expected 1 answer, but got %d answers", numericAnswers.size()), 1, numericAnswers.size());
        assertEquals(String.format("Expected answer to equal %f, but it was %f", expectedAnswer, numericAnswers.get(0).number().doubleValue()),
                expectedAnswer,
                numericAnswers.get(0).number().doubleValue(),
                0.01);
    }

    @Then("aggregate answer is empty")
    public void aggregate_answer_is_empty() {
        assertNotNull("The last executed query was not an aggregate query", numericAnswers);
        assertEquals("Aggregate answer is not empty, it has a value", 0, numericAnswers.size());
    }

    @Then("group identifiers are")
    public void group_identifiers_are(Map<String, Map<String, String>> identifiers) {
        for (Map.Entry<String, Map<String, String>> entry : identifiers.entrySet()) {
            String groupIdentifier = entry.getKey();
            Map<String, String> variables = entry.getValue();
            groupOwnerIdentifiers.put(groupIdentifier, variables.get("owner"));
        }
    }

    @Then("answer groups are")
    public void answer_groups_are(List<Map<String, String>> answerIdentifierTable) {
        Set<AnswerIdentifierGroup> answerIdentifierGroups = answerIdentifierTable.stream()
                .collect(Collectors.groupingBy(x -> x.get(AnswerIdentifierGroup.GROUP_COLUMN_NAME)))
                .values()
                .stream()
                .map(answerIdentifiers -> new AnswerIdentifierGroup(answerIdentifiers, groupOwnerIdentifiers))
                .collect(Collectors.toSet());

        assertEquals(String.format("Expected [%d] answer groups, but found [%d]",
                answerIdentifierGroups.size(), answerGroups.size()),
                answerIdentifierGroups.size(), answerGroups.size()
        );

        for (AnswerIdentifierGroup answerIdentifierGroup : answerIdentifierGroups) {
            String groupOwnerIdentifier = answerIdentifierGroup.groupOwnerIdentifier;
            AnswerGroup<ConceptMap> answerGroup = answerGroups.stream()
                    .filter(ag -> identifierChecks.get(groupOwnerIdentifier).check(ag.owner()))
                    .findAny()
                    .orElse(null);
            assertNotNull(String.format("The group identifier [%s] does not match any of the answer group owners", groupOwnerIdentifier), answerGroup);

            List<Map<String, String>> answersIdentifiers = answerIdentifierGroup.answersIdentifiers;
            for (ConceptMap answer : answerGroup.answers()) {
                List<Map<String, String>> matchingIdentifiers = new ArrayList<>();

                for (Map<String, String> answerIdentifiers : answersIdentifiers) {

                    if (matchAnswer(answerIdentifiers, answer)) {
                        matchingIdentifiers.add(answerIdentifiers);
                    }
                }
                assertEquals(
                        String.format("An identifier entry (row) should match 1-to-1 to an answer, but there were [%d] matching identifier entries for answer with variables %s",
                                matchingIdentifiers.size(), answer.map().keySet().toString()),
                        1, matchingIdentifiers.size()
                );
            }
        }
    }

    @Then("group aggregate values are")
    public void group_aggregate_values_are(List<Map<String, String>> answerIdentifierTable) {
        Map<String, Double> expectations = new HashMap<>();
        for (Map<String, String> answerIdentifierRow : answerIdentifierTable) {
            String groupIdentifier = answerIdentifierRow.get(AnswerIdentifierGroup.GROUP_COLUMN_NAME);
            String groupOwnerIdentifier = groupOwnerIdentifiers.get(groupIdentifier);
            double expectedAnswer = Double.parseDouble(answerIdentifierRow.get("value"));
            expectations.put(groupOwnerIdentifier, expectedAnswer);
        }

        assertEquals(String.format("Expected [%d] answer groups, but found [%d]",
                expectations.size(), numericAnswerGroups.size()),
                expectations.size(), numericAnswerGroups.size()
        );

        for (Map.Entry<String, Double> expectation : expectations.entrySet()) {
            String groupIdentifier = expectation.getKey();
            double expectedAnswer = expectation.getValue();
            AnswerGroup<Numeric> answerGroup = numericAnswerGroups.stream()
                    .filter(ag -> identifierChecks.get(groupIdentifier).check(ag.owner()))
                    .findAny()
                    .orElse(null);
            assertNotNull(String.format("The group identifier [%s] does not match any of the answer group owners", groupIdentifier), answerGroup);

            double actualAnswer = answerGroup.answers().get(0).number().doubleValue();
            assertEquals(
                    String.format("Expected answer [%f] for group [%s], but got [%f]",
                            expectedAnswer, groupIdentifier, actualAnswer),
                    expectedAnswer, actualAnswer, 0.01
            );
        }
    }

    @Then("number of groups is: {int}")
    public void number_of_groups_is(int expectedGroupCount) {
        assertEquals(expectedGroupCount, answerGroups.size());
    }

    public static class AnswerIdentifierGroup {
        private final String groupOwnerIdentifier;
        private final List<Map<String, String>> answersIdentifiers;

        private static final String GROUP_COLUMN_NAME = "group";

        public AnswerIdentifierGroup(final List<Map<String, String>> answerIdentifierTable, final Map<String, String> groupOwnerIdentifiers) {
            final String groupIdentifier = answerIdentifierTable.get(0).get(GROUP_COLUMN_NAME);
            groupOwnerIdentifier = groupOwnerIdentifiers.get(groupIdentifier);
            answersIdentifiers = new ArrayList<>();
            for (final Map<String, String> rawAnswerIdentifiers : answerIdentifierTable) {
                answersIdentifiers.add(rawAnswerIdentifiers.entrySet().stream()
                        .filter(e -> !e.getKey().equals(GROUP_COLUMN_NAME))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            }
        }
    }

    private boolean matchAnswer(Map<String, String> answerIdentifiers, ConceptMap answer) {

        if(!answerIdentifiers.keySet().equals(answer.map().keySet().stream().map(Variable::name).collect(Collectors.toSet()))) {
            return false;
        }

        for (Map.Entry<String, String> entry : answerIdentifiers.entrySet()) {
            String varName = entry.getKey();
            String identifier = entry.getValue();

            if(!identifierChecks.containsKey(identifier)) {
                throw new ScenarioDefinitionException(String.format("Identifier \"%s\" hasn't previously been declared", identifier));
            }

            // This concept may have been retrieved in an old transaction, so reload it from the current one
            final Concept staleConcept = answer.get(varName);
            final Concept concept = tx.getConcept(staleConcept.id());

            if (concept.isDeleted()) {
                return false;
            }

            if(!identifierChecks.get(identifier).check(concept)) {
                return false;
            }
        }
        return true;
    }

    @Then("rules are")
    public void rules_are(Map<String, Map<String, String>> rules) {
        this.rules = rules;
    }

    @Then("answers contain explanation tree")
    public void answers_contain_explanation_tree(Map<Integer, Map<String, String>> explanationTree) {
        checkExplanationEntry(answers, explanationTree, 0);
    }

    private void checkExplanationEntry(List<ConceptMap> answers, Map<Integer, Map<String, String>> explanationTree, Integer entryId) {
        Map<String, String> explanationEntry = explanationTree.get(entryId);
        String[] vars = explanationEntry.get("vars").split(", ");
        String[] identifiers = explanationEntry.get("identifiers").split(", ");
        String[] children = explanationEntry.get("children").split(", ");

        if (vars.length != identifiers.length) {
            throw new ScenarioDefinitionException(String.format("vars and identifiers do not correspond for explanation entry %d. Found %d vars and %s identifiers", entryId, vars.length, identifiers.length));
        }

        Map<String, String> answerIdentifiers = IntStream.range(0, vars.length).boxed().collect(Collectors.toMap(i -> vars[i], i -> identifiers[i]));

        Optional<ConceptMap> matchingAnswer = answers.stream().filter(answer -> matchAnswer(answerIdentifiers, answer)).findFirst();

        assertTrue(String.format("No answer found for explanation entry %d that satisfies the vars and identifiers given", entryId), matchingAnswer.isPresent());
        ConceptMap answer = matchingAnswer.get();

        String queryWithIds = applyQueryTemplate(explanationEntry.get("pattern"), answer);
        Pattern queryWithIdsPattern = Graql.parsePattern(queryWithIds);
        assertEquals(
                String.format("Explanation entry %d has an incorrect pattern.\nExpected: %s\nActual: %s", entryId, queryWithIdsPattern, answer.getPattern()),
                queryWithIdsPattern, answer.getPattern());

        String expectedRule = explanationEntry.get("explanation");
        boolean hasExplanation = answer.explanation() != null && !answer.explanation().isEmpty();

        if (expectedRule.equals("lookup")) {

            assertFalse(String.format("Explanation entry %d is declared as a lookup, but an explanation was found", entryId), hasExplanation);

            String[] expectedChildren = {"-"};
            if (!Arrays.equals(expectedChildren, children)) {
                throw new ScenarioDefinitionException(String.format("Explanation entry %d is declared as a lookup, and so it should have no children, indicated as \"-\", but got children %s instead", entryId, Arrays.toString(children)));
            }
        } else {

            Explanation explanation = answer.explanation();
            List<ConceptMap> explAnswers = explanation.getAnswers();

            assertEquals(String.format("Explanation entry %d should have as many children as it has answers. Instead, %d children were declared, and %d answers were found. Note, this entry could be wrongly declared as a rule, when it is a lookup.", entryId, children.length, explAnswers.size()),
                    children.length, explAnswers.size());

            if (expectedRule.equals("join") || expectedRule.equals("negation") || expectedRule.equals("disjunction")) {
                assertNull(String.format("Explanation entry %d is declared as a join, and should not have a rule attached, but one was found", entryId), explanation.isRuleExplanation() ? ((RuleExplanation)explanation).getRule() : null);
            } else {
                // rule
                Rule rule = ((RuleExplanation)explanation).getRule();
                String ruleLabel = rule.label().toString();
                assertEquals(String.format("Incorrect rule label for explanation entry %d with rule %s.\nExpected: %s\nActual: %s", entryId, ruleLabel, expectedRule, ruleLabel), expectedRule, ruleLabel);

                Map<String, String> expectedRuleDefinition = rules.get(expectedRule);
                Pattern when = Graql.parsePattern(Objects.requireNonNull(rule.when()).toString());
                assertEquals(String.format("Incorrect rule body (when) for explanation entry %d with rule %s.\nExpected: %s\nActual: %s", entryId, ruleLabel, expectedRuleDefinition.get("when"), when),
                        Graql.parsePattern(expectedRuleDefinition.get("when")), when);

                Pattern then = Graql.parsePattern(Objects.requireNonNull(rule.then()).toString());
                assertEquals(String.format("Incorrect rule head (then) for explanation entry %d with rule %s.\nExpected: %s\nActual: %s", entryId, ruleLabel, expectedRuleDefinition.get("then"), then),
                        Graql.parsePattern(expectedRuleDefinition.get("then")), then);
            }
            for (String child : children) {
                // Recurse
                checkExplanationEntry(explAnswers, explanationTree, Integer.valueOf(child));
            }
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
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("<.+?>");
        java.util.regex.Matcher matcher = pattern.matcher(template);

        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find()) {
            String matched = matcher.group(0);
            String requiredVariable = variableFromTemplatePlaceholder(matched.substring(1, matched.length() - 1));

            builder.append(template.substring(i, matcher.start()));
            if (templateFiller.map().containsKey(new Variable(requiredVariable))) {
                Concept concept = templateFiller.get(requiredVariable);
                String conceptId = concept.id().toString();
                builder.append(conceptId);
            } else {
                throw new ScenarioDefinitionException(String.format("No ID available for template placeholder: %s", matched));
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
            throw new ScenarioDefinitionException("Cannot replace template not based on ID");
        }
    }

    private static class ScenarioDefinitionException extends RuntimeException {
        ScenarioDefinitionException(String message) {
            super(message);
        }
    }

    private interface UniquenessCheck {
        boolean check(Concept concept);
    }

    public static class LabelUniquenessCheck implements UniquenessCheck {

        private final String label;

        LabelUniquenessCheck(String label) {
            this.label = label;
        }

        @Override
        public boolean check(Concept concept) {
            if (concept.isSchemaConcept()) {
                return label.equals(concept.asSchemaConcept().label().toString());
            } else {
                throw new ScenarioDefinitionException("Concept was checked for label uniqueness, but it is not a schema concept.");
            }
        }
    }

    public static class AttributeUniquenessCheck {

        protected final String type;
        protected final String value;

        AttributeUniquenessCheck(String typeAndValue) {
            String[] s = typeAndValue.split(":");
            assertEquals(
                    String.format("A check for attribute uniqueness should be given in the format \"type:value\", but received %s", typeAndValue),
                    2, s.length
            );
            type = s[0];
            value = s[1];
        }
    }

    public static class ValueUniquenessCheck extends AttributeUniquenessCheck implements UniquenessCheck {
        ValueUniquenessCheck(String typeAndValue) {
            super(typeAndValue);
        }

        public boolean check(Concept concept) {
            return concept.isAttribute()
                    && type.equals(concept.asAttribute().type().label().toString())
                    && value.equals(concept.asAttribute().value().toString());
        }
    }

    public static class KeyUniquenessCheck extends AttributeUniquenessCheck implements UniquenessCheck {
        KeyUniquenessCheck(String typeAndValue) {
            super(typeAndValue);
        }

        /**
         * Check that the given key is in the concept's keys
         * @param concept to check
         * @return whether the given key matches a key belonging to the concept
         */
        @Override
        public boolean check(Concept concept) {
            if(!concept.isThing()) { return false; }

            Set<Attribute> keys = concept.asThing().keys().collect(Collectors.toSet());

            HashMap<String, String> keyMap = new HashMap<>();

            for (Attribute<?> key : keys) {
                keyMap.put(
                        key.type().label().toString(),
                        key.value().toString());
            }
            return value.equals(keyMap.get(type));
        }
    }
}
