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

import grakn.common.collection.Bytes;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Type;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlQuery;
import graql.lang.query.GraqlUndefine;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static grakn.core.test.behaviour.connection.ConnectionSteps.tx;
import static grakn.core.test.behaviour.util.Util.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GraqlSteps {

    private static List<ConceptMap> answers;
    private static Numeric numericAnswer;
    private static List<AnswerGroup<ConceptMap>> answerGroups;
    private static List<AnswerGroup<Numeric>> numericAnswerGroups;
    HashMap<String, UniquenessCheck> identifierChecks = new HashMap<>();
    HashMap<String, String> groupOwnerIdentifiers = new HashMap<>();
    private Map<String, Map<String, String>> rules;

    @Given("the integrity is validated")
    public void integrity_is_validated() {

        // TODO

    }

    @Given("graql define")
    public void graql_define(String defineQueryStatements) {
        final GraqlDefine graqlQuery = Graql.parseQuery(String.join("\n", defineQueryStatements)).asDefine();
        tx().query().define(graqlQuery);
    }

    @Given("graql define; throws exception")
    public void graql_define_throws_exception(String defineQueryStatements) {
        assertThrows(() -> graql_define(defineQueryStatements));
    }

    @Given("graql undefine")
    public void graql_undefine(String undefineQueryStatements) {
        final GraqlUndefine graqlQuery = Graql.parseQuery(String.join("\n", undefineQueryStatements)).asUndefine();
        tx().query().undefine(graqlQuery);
    }

    @Given("graql undefine; throws exception")
    public void graql_undefine_throws_exception(String undefineQueryStatements) {
        assertThrows(() -> graql_undefine(undefineQueryStatements));
    }

    @Given("graql insert")
    public void graql_insert(String insertQueryStatements) {
        final GraqlInsert graqlQuery = Graql.parseQuery(String.join("\n", insertQueryStatements)).asInsert();
        tx().query().insert(graqlQuery);
    }

    @Given("graql insert; throws exception")
    public void graql_insert_throws_exception(String insertQueryStatements) {
        assertThrows(() -> graql_insert(insertQueryStatements));
    }

    @Given("graql delete")
    public void graql_delete(String deleteQueryStatements) {
        final GraqlDelete graqlQuery = Graql.parseQuery(String.join("\n", deleteQueryStatements)).asDelete();
        tx().query().delete(graqlQuery);
    }

    @Given("graql delete; throws exception")
    public void graql_delete_throws_exception(String deleteQueryStatements) {
        assertThrows(() -> graql_delete(deleteQueryStatements));
    }

    @When("get answers of graql insert")
    public void get_answers_of_graql_insert(String graqlQueryStatements) {
        final GraqlInsert graqlQuery = Graql.parseQuery(String.join("\n", graqlQueryStatements)).asInsert();
        // Erase answers from previous steps to avoid polluting the result space
        answers = null;
        numericAnswer = null;
        answerGroups = null;
        numericAnswerGroups = null;

        answers = tx().query().insert(graqlQuery).toList();
    }

    @When("get answers of graql query")
    public void graql_query(String graqlQueryStatements) {
        final GraqlQuery graqlQuery = Graql.parseQuery(String.join("\n", graqlQueryStatements));
        // Erase answers from previous steps to avoid polluting the result space
        answers = null;
        numericAnswer = null;
        answerGroups = null;
        numericAnswerGroups = null;
        if (graqlQuery instanceof GraqlMatch) {
            answers = tx().query().match(graqlQuery.asMatch()).toList();
        } else if (graqlQuery instanceof GraqlInsert) {
            throw new ScenarioDefinitionException("Insert is not supported; use `get answers of graql insert` instead");
        } else if (graqlQuery instanceof GraqlMatch.Aggregate) {
            numericAnswer = tx().query().match(graqlQuery.asMatchAggregate());
        } else if (graqlQuery instanceof GraqlMatch.Group) {
            answerGroups = tx().query().match(graqlQuery.asMatchGroup()).toList();
        } else if (graqlQuery instanceof GraqlMatch.Group.Aggregate) {
            numericAnswerGroups = tx().query().match(graqlQuery.asMatchGroupAggregate()).toList();
        } else {
            throw new ScenarioDefinitionException("Only match and insert supported for now");
        }
    }

    @When("graql match; throws exception")
    public void graql_match_throws_exception(String graqlQueryStatements) {
        assertThrows(() -> graql_query(graqlQueryStatements));
    }

    @Then("answer size is: {number}")
    public void answer_quantity_assertion(int expectedAnswers) {
        assertEquals(String.format("Expected [%d] answers, but got [%d]", expectedAnswers, answers.size()),
                     expectedAnswers, answers.size());
    }

    @Then("concept identifiers are")
    public void concept_identifiers_are(Map<String, Map<String, String>> identifiers) {
        for (Map.Entry<String, Map<String, String>> entry : identifiers.entrySet()) {
            final String identifier = entry.getKey();
            final String check = entry.getValue().get("check");
            final String value = entry.getValue().get("value");

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

    @Then("uniquely identify answer concepts")
    public void uniquely_identify_answer_concepts(List<Map<String, String>> answersIdentifiers) {
        assertEquals(
                String.format("The number of identifier entries (rows) should match the number of answers, but found %d identifier entries and %d answers",
                              answersIdentifiers.size(), answers.size()),
                answersIdentifiers.size(), answers.size()
        );

        for (ConceptMap answer : answers) {
            List<Map<String, String>> matchingIdentifiers = new ArrayList<>();

            for (Map<String, String> answerIdentifier : answersIdentifiers) {

                if (matchAnswer(answerIdentifier, answer)) {
                    matchingIdentifiers.add(answerIdentifier);
                }
            }
            assertEquals(
                    String.format("An identifier entry (row) should match 1-to-1 to an answer, but there were %d matching identifier entries for answer with variables %s",
                                  matchingIdentifiers.size(), answer.concepts().keySet().toString()),
                    1, matchingIdentifiers.size()
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
        assertNotNull("The last executed query was not an aggregate query", numericAnswer);
        assertEquals(String.format("Expected answer to equal %f, but it was %f", expectedAnswer, numericAnswer.number().doubleValue()),
                     expectedAnswer, numericAnswer.number().doubleValue(), 0.01);
    }

    @Then("aggregate answer is empty")
    public void aggregate_answer_is_empty() {
        assertNull(numericAnswer);
    }

    @Then("group identifiers are")
    public void group_identifiers_are(Map<String, Map<String, String>> identifiers) {
        for (Map.Entry<String, Map<String, String>> entry : identifiers.entrySet()) {
            final String groupIdentifier = entry.getKey();
            final Map<String, String> variables = entry.getValue();
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
                                      matchingIdentifiers.size(), answer.concepts().keySet().toString()),
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

        assertEquals(String.format("Expected [%d] answer groups, but found [%d]", expectations.size(), numericAnswerGroups.size()),
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

        public AnswerIdentifierGroup(List<Map<String, String>> answerIdentifierTable, Map<String, String> groupOwnerIdentifiers) {
            final String groupIdentifier = answerIdentifierTable.get(0).get(GROUP_COLUMN_NAME);
            groupOwnerIdentifier = groupOwnerIdentifiers.get(groupIdentifier);
            answersIdentifiers = new ArrayList<>();
            for (Map<String, String> rawAnswerIdentifiers : answerIdentifierTable) {
                answersIdentifiers.add(rawAnswerIdentifiers.entrySet().stream()
                                               .filter(e -> !e.getKey().equals(GROUP_COLUMN_NAME))
                                               .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            }
        }
    }

    private boolean matchAnswer(Map<String, String> answerIdentifiers, ConceptMap answer) {

        for (Map.Entry<String, String> entry : answerIdentifiers.entrySet()) {
            final Reference var = Reference.named(entry.getKey());
            final String identifier = entry.getValue();

            if (!identifierChecks.containsKey(identifier)) {
                throw new ScenarioDefinitionException(String.format("Identifier \"%s\" hasn't previously been declared", identifier));
            }

            if (!identifierChecks.get(identifier).check(answer.get(var))) {
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
        // TODO
        throw new UnsupportedOperationException();
//        checkExplanationEntry(answers, explanationTree, 0);
    }

    /* private void checkExplanationEntry(List<ConceptMap> answers, Map<Integer, Map<String, String>> explanationTree, Integer entryId) {
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
        Conjunction<?> queryWithIdsConj = Graql.and(Graql.parsePatternList(queryWithIds));
        assertEquals(
                String.format("Explanation entry %d has an incorrect pattern.\nExpected: %s\nActual: %s", entryId, queryWithIdsConj, answer.queryPattern()),
                queryWithIdsConj, answer.queryPattern()
        );

        String expectedRule = explanationEntry.get("rule");
        boolean hasExplanation = answer.hasExplanation();

        if (expectedRule.equals("lookup")) {

            assertFalse(String.format("Explanation entry %d is declared as a lookup, but an explanation was found", entryId), hasExplanation);

            String[] expectedChildren = {"-"};
            assertArrayEquals(String.format("Explanation entry %d is declared as a lookup, and so it should have no children, indicated as \"-\", but got children %s instead", entryId, Arrays.toString(children)), expectedChildren, children);
        } else {

            Explanation explanation = answer.explanation();
            List<ConceptMap> explAnswers = explanation.getAnswers();

            assertEquals(String.format("Explanation entry %d should have as many children as it has answers. Instead, %d children were declared, and %d answers were found.", entryId, children.length, explAnswers.size()), children.length, explAnswers.size());

            if (expectedRule.equals("join")) {
                assertNull(String.format("Explanation entry %d is declared as a join, and should not have a rule attached, but one was found", entryId), explanation.getRule());
            } else {
                // rule
                Rule.Remote rule = explanation.getRule();
                String ruleLabel = rule.getLabel();
                assertEquals(String.format("Incorrect rule label for explanation entry %d with rule %s.\nExpected: %s\nActual: %s", entryId, ruleLabel, expectedRule, ruleLabel), expectedRule, ruleLabel);

                Map<String, String> expectedRuleDefinition = rules.get(expectedRule);
                String when = Objects.requireNonNull(rule.getWhen()).toString();
                assertEquals(String.format("Incorrect rule body (when) for explanation entry %d with rule %s.\nExpected: %s\nActual: %s", entryId, ruleLabel, expectedRuleDefinition.get("when"), when), expectedRuleDefinition.get("when"), when);

                String then = Objects.requireNonNull(rule.getThen()).toString();
                assertEquals(String.format("Incorrect rule head (then) for explanation entry %d with rule %s.\nExpected: %s\nActual: %s", entryId, ruleLabel, expectedRuleDefinition.get("then"), then), expectedRuleDefinition.get("then"), then);
            }
            for (String child : children) {
                // Recurse
                checkExplanationEntry(explAnswers, explanationTree, Integer.valueOf(child));
            }
        }
    } */

    @Then("each answer satisfies")
    public void each_answer_satisfies(String templatedGraqlQuery) {
        final String templatedQuery = String.join("\n", templatedGraqlQuery);
        for (ConceptMap answer : answers) {
            final String query = applyQueryTemplate(templatedQuery, answer);
            final GraqlMatch graqlQuery = Graql.parseQuery(query).asMatch();
            final long answerSize = tx().query().match(graqlQuery).toList().size();
            assertEquals(1, answerSize);
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

            builder.append(template, i, matcher.start());
            if (templateFiller.contains(Reference.named(requiredVariable))) {

                Concept concept = templateFiller.get(requiredVariable);
                if (!concept.isThing())
                    throw new ScenarioDefinitionException("Cannot apply IID templating to Type concepts");
                String conceptId = Bytes.bytesToHexString(concept.asThing().getIID());
                builder.append(conceptId);

            } else {
                throw new ScenarioDefinitionException(String.format("No IID available for template placeholder: %s", matched));
            }
            i = matcher.end();
        }
        builder.append(template.substring(i));
        return builder.toString();
    }

    private String variableFromTemplatePlaceholder(String placeholder) {
        if (placeholder.endsWith(".iid")) {
            final String stripped = placeholder.replace(".iid", "");
            final String withoutPrefix = stripped.replace("answer.", "");
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
            if (concept instanceof Type) {
                return label.equals(concept.asType().getLabel().toString());
            }

            throw new ScenarioDefinitionException("Concept was checked for label uniqueness, but it is not a Type.");
        }
    }

    public static class AttributeUniquenessCheck {

        protected final String type;
        protected final String value;

        AttributeUniquenessCheck(String typeAndValue) {
            final String[] s = typeAndValue.split(":");
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
            if (!(concept instanceof Attribute)) {
                return false;
            }

            final Attribute attribute = concept.asThing().asAttribute();

            if (!type.equals(attribute.getType().getLabel().toString())) {
                return false;
            }

            switch (attribute.getType().getValueType()) {
                case BOOLEAN:
                    return value.equals(attribute.asBoolean().getValue().toString());
                case LONG:
                    return value.equals(attribute.asLong().getValue().toString());
                case DOUBLE:
                    return value.equals(attribute.asDouble().getValue().toString());
                case STRING:
                    return value.equals(attribute.asString().getValue());
                case DATETIME:
                    return value.equals(attribute.asDateTime().getValue().toString());
                case OBJECT:
                default:
                    throw new GraqlSteps.ScenarioDefinitionException("Unrecognised value type " + attribute.getType().getValueType());
            }
        }
    }

    public static class KeyUniquenessCheck extends AttributeUniquenessCheck implements UniquenessCheck {
        KeyUniquenessCheck(String typeAndValue) {
            super(typeAndValue);
        }

        @Override
        public boolean check(Concept concept) {
            if (!(concept instanceof Thing)) { return false; }

            final Set<Attribute> keys = concept.asThing().getHas(true).collect(Collectors.toSet());

            final HashMap<String, String> keyMap = new HashMap<>();

            for (Attribute key : keys) {
                final String keyValue;
                switch (key.getType().getValueType()) {
                    case BOOLEAN:
                        keyValue = key.asBoolean().getValue().toString();
                        break;
                    case LONG:
                        keyValue = key.asLong().getValue().toString();
                        break;
                    case DOUBLE:
                        keyValue = key.asDouble().getValue().toString();
                        break;
                    case STRING:
                        keyValue = key.asString().getValue();
                        break;
                    case DATETIME:
                        keyValue = key.asDateTime().getValue().toString();
                        break;
                    case OBJECT:
                    default:
                        throw new GraqlSteps.ScenarioDefinitionException("Unrecognised value type " + key.getType().getValueType());
                }

                keyMap.put(key.getType().getLabel().toString(), keyValue);
            }
            return value.equals(keyMap.get(type));
        }
    }
}
