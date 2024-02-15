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

package com.vaticle.typedb.core.test.behaviour.query;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonValue;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMapGroup;
import com.vaticle.typedb.core.concept.answer.ValueGroup;
import com.vaticle.typedb.core.concept.answer.ReadableConceptTree;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.value.Value;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.test.behaviour.exception.ScenarioDefinitionException;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Modifiers.Filter;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.test.ProcedurePermutator;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.common.Reference;
import com.vaticle.typeql.lang.common.exception.TypeQLException;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLDelete;
import com.vaticle.typeql.lang.query.TypeQLFetch;
import com.vaticle.typeql.lang.query.TypeQLGet;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLUndefine;
import com.vaticle.typeql.lang.query.TypeQLUpdate;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Double.equalsApproximate;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.test.Util.assertThrows;
import static com.vaticle.typedb.core.common.test.Util.assertThrowsWithMessage;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.tx;
import static com.vaticle.typedb.core.common.test.Util.jsonEquals;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TypeQLSteps {

    private static List<? extends ConceptMap> getAnswers;
    private static Optional<Value<?>> aggregateAnswer;
    private static List<ConceptMapGroup> groupAnswers;
    private static List<ValueGroup> groupAggregateAnswers;
    private static List<ReadableConceptTree> fetchAnswers;
    HashMap<String, UniquenessCheck> identifierChecks = new HashMap<>();
    private Map<String, Map<String, String>> rules;

    @Given("typeql define")
    public void typeql_define(String defineQueryStatements) {
        try {
            TypeQLDefine typeQLQuery = TypeQL.parseQuery(String.join("\n", defineQueryStatements)).asDefine();
            tx().query().define(typeQLQuery);
        } catch (TypeQLException e) {
            // NOTE: We manually close transaction here, because we want to align with all non-java drivers,
            // where parsing happens at server-side which closes transaction if they fail
            tx().close();
            throw e;
        }
    }

    @Given("typeql define; throws exception containing {string}")
    public void typeql_define_throws_exception(String exception, String defineQueryStatements) {
        assertThrowsWithMessage(() -> typeql_define(defineQueryStatements), exception);
    }

    @Given("typeql define; throws exception")
    public void typeql_define_throws_exception(String defineQueryStatements) {
        assertThrows(() -> typeql_define(defineQueryStatements));
    }

    @Given("typeql undefine")
    public void typeql_undefine(String undefineQueryStatements) {
        TypeQLUndefine typeQLQuery = TypeQL.parseQuery(String.join("\n", undefineQueryStatements)).asUndefine();
        tx().query().undefine(typeQLQuery);
    }

    @Given("typeql undefine; throws exception")
    public void typeql_undefine_throws_exception(String undefineQueryStatements) {
        assertThrows(() -> typeql_undefine(undefineQueryStatements));
    }

    @Given("typeql insert")
    public void typeql_insert(String insertQueryStatements) {
        TypeQLInsert typeQLQuery = TypeQL.parseQuery(String.join("\n", insertQueryStatements)).asInsert();
        tx().query().insert(typeQLQuery);
    }

    @Given("typeql insert; throws exception")
    public void typeql_insert_throws_exception(String insertQueryStatements) {
        assertThrows(() -> typeql_insert(insertQueryStatements));
    }

    @Given("typeql insert; throws exception containing {string}")
    public void typeql_insert_throws_exception(String exception, String insertQueryStatements) {
        assertThrowsWithMessage(() -> typeql_insert(insertQueryStatements), exception);
    }

    @Given("typeql delete")
    public void typeql_delete(String deleteQueryStatements) {
        TypeQLDelete typeQLQuery = TypeQL.parseQuery(String.join("\n", deleteQueryStatements)).asDelete();
        tx().query().delete(typeQLQuery);
    }

    @Given("typeql delete; throws exception")
    public void typeql_delete_throws_exception(String deleteQueryStatements) {
        assertThrows(() -> typeql_delete(deleteQueryStatements));
    }

    @Given("typeql delete; throws exception containing {string}")
    public void typeql_delete_throws_exception(String exception, String deleteQueryStatements) {
        assertThrowsWithMessage(() -> typeql_delete(deleteQueryStatements), exception);
    }

    @Given("typeql update")
    public void typeql_update(String updateQueryStatements) {
        TypeQLUpdate typeQLQuery = TypeQL.parseQuery(String.join("\n", updateQueryStatements)).asUpdate();
        tx().query().update(typeQLQuery);
    }

    @Given("typeql update; throws exception")
    public void typeql_update_throws_exception(String updateQueryStatements) {
        assertThrows(() -> typeql_update(updateQueryStatements));
    }

    private void clearAnswers() {
        getAnswers = null;
        aggregateAnswer = null;
        groupAnswers = null;
        groupAggregateAnswers = null;
    }

    @When("get answers of typeql insert")
    public void get_answers_of_typeql_insert(String typeQLQueryStatements) {
        TypeQLInsert typeQLQuery = TypeQL.parseQuery(String.join("\n", typeQLQueryStatements)).asInsert();
        clearAnswers();
        getAnswers = tx().query().insert(typeQLQuery).toList();
        if (typeQLQuery.match().isPresent()) assertQueryPlansCorrect(typeQLQuery.match().get().get());
    }

    @When("get answers of typeql get")
    public void typeql_get(String typeQLQueryStatements) {
        try {
            TypeQLGet typeQLQuery = TypeQL.parseQuery(String.join("\n", typeQLQueryStatements)).asGet();
            clearAnswers();
            getAnswers = tx().query().get(typeQLQuery).toList();
            assertQueryPlansCorrect(typeQLQuery);
        } catch (TypeQLException e) {
            // NOTE: We manually close transaction here, because we want to align with all non-java drivers,
            // where parsing happens at server-side which closes transaction if they fail
            tx().close();
            throw e;
        }
    }

    private void assertQueryPlansCorrect(TypeQLGet typeQLQuery) {
        Disjunction disjunction = Disjunction.create(typeQLQuery.match().conjunction().normalise());
        tx().logic().typeInference().applyCombination(disjunction);
        tx().logic().expressionResolver().resolveExpressions(disjunction);
        Set<Identifier.Variable.Retrievable> filter = iterate(typeQLQuery.effectiveFilter())
                .map(unboundVar -> Identifier.Variable.of(unboundVar.reference().asName()))
                .map(Identifier.Variable::asRetrievable).toSet();
        for (Conjunction conjunction : disjunction.conjunctions()) {
            GraphTraversal.Thing traversal = conjunction.traversal(Filter.create(filter));
            // limited permutation space to avoid timeouts
            FunctionalIterator<GraphProcedure> procedurePermutations = ProcedurePermutator.generate(traversal.structure()).limit(40320);
            GraphProcedure procedure = procedurePermutations.next();
            Set<VertexMap> answers = procedure.iterator(tx().concepts().graph(),
                    traversal.parameters(), traversal.modifiers()).toSet();
            for (int i = 0; procedurePermutations.hasNext(); i++) {
                procedure = procedurePermutations.next();
                Set<VertexMap> permutationAnswers = procedure.iterator(tx().concepts().graph(),
                        traversal.parameters(), traversal.modifiers()).toSet();
                assertEquals(answers, permutationAnswers);
            }
        }
    }

    @When("typeql get; throws exception")
    public void typeql_get_throws_exception(String typeQLQueryStatements) {
        assertThrows(() -> typeql_get(typeQLQueryStatements));
    }

    @When("typeql get; throws exception containing {string}")
    public void typeql_get_throws_exception(String exception, String typeQLQueryStatements) {
        assertThrowsWithMessage(() -> typeql_get(typeQLQueryStatements), exception);
    }

    @When("get answer of typeql get aggregate")
    public void typeql_get_aggregate(String typeQLQueryStatements) {
        TypeQLGet.Aggregate typeQLQuery = TypeQL.parseQuery(String.join("\n", typeQLQueryStatements)).asGetAggregate();
        clearAnswers();
        aggregateAnswer = tx().query().get(typeQLQuery);
        assertQueryPlansCorrect(typeQLQuery.get());
    }

    @When("typeql get aggregate; throws exception")
    public void typeql_get_aggregate_throws_exception(String typeQLQueryStatements) {
        assertThrows(() -> typeql_get_aggregate(typeQLQueryStatements));
    }

    @When("get answers of typeql get group")
    public void typeql_get_group(String typeQLQueryStatements) {
        TypeQLGet.Group typeQLQuery = TypeQL.parseQuery(String.join("\n", typeQLQueryStatements)).asGetGroup();
        clearAnswers();
        groupAnswers = tx().query().get(typeQLQuery).toList();
        assertQueryPlansCorrect(typeQLQuery.get());
    }

    @When("typeql get group; throws exception")
    public void typeql_get_group_throws_exception(String typeQLQueryStatements) {
        assertThrows(() -> typeql_get_group(typeQLQueryStatements));
    }

    @When("get answers of typeql get group aggregate")
    public void typeql_get_group_aggregate(String typeQLQueryStatements) {
        TypeQLGet.Group.Aggregate typeQLQuery = TypeQL.parseQuery(String.join("\n", typeQLQueryStatements)).asGetGroupAggregate();
        clearAnswers();
        groupAggregateAnswers = tx().query().get(typeQLQuery).toList();
        assertQueryPlansCorrect(typeQLQuery.group().get());
    }

    @Then("answer size is: {number}")
    public void answer_quantity_assertion(int expectedAnswers) {
        assertEquals(String.format("Expected [%d] answers, but got [%d]", expectedAnswers, getAnswers.size()),
                expectedAnswers, getAnswers.size());
    }

    @Then("uniquely identify answer concepts")
    public void uniquely_identify_answer_concepts(List<Map<String, String>> answerConcepts) {
        assertEquals(
                String.format("The number of identifier entries (rows) should match the number of answers, but found %d identifier entries and %d answers.",
                        answerConcepts.size(), getAnswers.size()),
                answerConcepts.size(), getAnswers.size()
        );

        for (ConceptMap answer : getAnswers) {
            List<Map<String, String>> matchingIdentifiers = new ArrayList<>();

            for (Map<String, String> answerIdentifier : answerConcepts) {

                if (matchAnswerConcept(answerIdentifier, answer)) {
                    matchingIdentifiers.add(answerIdentifier);
                }
            }
            assertEquals(
                    String.format("An identifier entry (row) should match 1-to-1 to an answer, but there were %d matching identifier entries for answer with variables %s.",
                            matchingIdentifiers.size(), answer.concepts().keySet().toString()),
                    1, matchingIdentifiers.size()
            );
        }
    }

    @Then("order of answer concepts is")
    public void order_of_answer_concepts_is(List<Map<String, String>> answersIdentifiers) {
        assertEquals(
                String.format("The number of identifier entries (rows) should match the number of answers, but found %d identifier entries and %d answers.",
                        answersIdentifiers.size(), getAnswers.size()),
                answersIdentifiers.size(), getAnswers.size()
        );
        for (int i = 0; i < getAnswers.size(); i++) {
            ConceptMap answer = getAnswers.get(i);
            Map<String, String> answerIdentifiers = answersIdentifiers.get(i);
            assertTrue(
                    String.format("The answer at index %d does not match the identifier entry (row) at index %d.", i, i),
                    matchAnswerConcept(answerIdentifiers, answer)
            );
        }
    }

    @Then("aggregate value is: {double}")
    public void aggregate_value_is(double expectedAnswer) {
        assertNotNull("The last executed query was not an aggregate query", aggregateAnswer);
        double asDouble = aggregateAnswer.get().isDouble() ? aggregateAnswer.get().asDouble().value() : aggregateAnswer.get().asLong().value();
        assertEquals(String.format("Expected answer to equal %f, but it was %f.", expectedAnswer, asDouble),
                expectedAnswer, asDouble, 0.001);
    }

    @Then("aggregate answer is empty")
    public void aggregate_answer_is_not_a_number() {
        assertTrue(aggregateAnswer.isEmpty());
    }

    @Then("answer groups are")
    public void answer_groups_are(List<Map<String, String>> answerIdentifierTable) {
        Set<AnswerIdentifierGroup> answerIdentifierGroups = answerIdentifierTable.stream()
                .collect(Collectors.groupingBy(x -> x.get(AnswerIdentifierGroup.GROUP_COLUMN_NAME)))
                .values()
                .stream()
                .map(AnswerIdentifierGroup::new)
                .collect(Collectors.toSet());

        assertEquals(String.format("Expected [%d] answer groups, but found [%d].",
                        answerIdentifierGroups.size(), groupAnswers.size()),
                answerIdentifierGroups.size(), groupAnswers.size()
        );

        for (AnswerIdentifierGroup answerIdentifierGroup : answerIdentifierGroups) {
            String[] identifier = answerIdentifierGroup.ownerIdentifier.split(":", 2);
            UniquenessCheck checker;
            switch (identifier[0]) {
                case "label":
                    checker = new LabelUniquenessCheck(identifier[1]);
                    break;
                case "key":
                    checker = new KeyUniquenessCheck(identifier[1]);
                    break;
                case "attr":
                    checker = new AttributeValueUniquenessCheck(identifier[1]);
                    break;
                case "value":
                    checker = new ValueUniquenessCheck(identifier[1]);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + identifier[0]);
            }
            System.out.println(groupAnswers);
            ConceptMapGroup answerGroup = groupAnswers.stream()
                    .filter(ag -> checker.check(ag.owner()))
                    .findAny()
                    .orElse(null);
            assertNotNull(String.format("The group identifier [%s] does not match any of the answer group owners.", answerIdentifierGroup.ownerIdentifier), answerGroup);

            List<Map<String, String>> answersIdentifiers = answerIdentifierGroup.answersIdentifiers;
            for (ConceptMap answer : answerGroup.conceptMaps()) {
                List<Map<String, String>> matchingIdentifiers = new ArrayList<>();

                for (Map<String, String> answerIdentifiers : answersIdentifiers) {

                    if (matchAnswerConcept(answerIdentifiers, answer)) {
                        matchingIdentifiers.add(answerIdentifiers);
                    }
                }
                assertEquals(
                        String.format("An identifier entry (row) should match 1-to-1 to an answer, but there were [%d] matching identifier entries for answer with variables %s.",
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
            String groupOwnerIdentifier = answerIdentifierRow.get(AnswerIdentifierGroup.GROUP_COLUMN_NAME);
            double expectedAnswer = Double.parseDouble(answerIdentifierRow.get("value"));
            expectations.put(groupOwnerIdentifier, expectedAnswer);
        }

        assertEquals(String.format("Expected [%d] answer groups, but found [%d].", expectations.size(), groupAggregateAnswers.size()),
                expectations.size(), groupAggregateAnswers.size()
        );

        for (Map.Entry<String, Double> expectation : expectations.entrySet()) {
            String[] identifier = expectation.getKey().split(":", 2);
            UniquenessCheck checker;
            switch (identifier[0]) {
                case "label":
                    checker = new LabelUniquenessCheck(identifier[1]);
                    break;
                case "key":
                    checker = new KeyUniquenessCheck(identifier[1]);
                    break;
                case "attr":
                    checker = new AttributeValueUniquenessCheck(identifier[1]);
                    break;
                case "value":
                    checker = new ValueUniquenessCheck(identifier[1]);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + identifier[0]);
            }
            double expectedAnswer = expectation.getValue();
            ValueGroup answerGroup = groupAggregateAnswers.stream()
                    .filter(ag -> checker.check(ag.owner()))
                    .findAny()
                    .orElse(null);
            assertNotNull(String.format("The group identifier [%s] does not match any of the answer group owners.", expectation.getKey()), answerGroup);

            Value<?> value = answerGroup.value().get();
            double actualAnswer = value.isDouble() ? value.asDouble().value() : value.asLong().value();
            assertEquals(
                    String.format("Expected answer [%f] for group [%s], but got [%f]",
                            expectedAnswer, expectation.getKey(), actualAnswer),
                    expectedAnswer, actualAnswer, 0.001
            );
        }
    }

    @Then("number of groups is: {int}")
    public void number_of_groups_is(int expectedGroupCount) {
        assertEquals(expectedGroupCount, groupAnswers.size());
    }

    public static class AnswerIdentifierGroup {
        private final String ownerIdentifier;
        private final List<Map<String, String>> answersIdentifiers;

        private static final String GROUP_COLUMN_NAME = "owner";

        public AnswerIdentifierGroup(List<Map<String, String>> answerIdentifierTable) {
            ownerIdentifier = answerIdentifierTable.get(0).get(GROUP_COLUMN_NAME);
            answersIdentifiers = new ArrayList<>();
            for (Map<String, String> rawAnswerIdentifiers : answerIdentifierTable) {
                answersIdentifiers.add(rawAnswerIdentifiers.entrySet().stream()
                        .filter(e -> !e.getKey().equals(GROUP_COLUMN_NAME))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            }
        }
    }

    @Then("group aggregate answer value is empty")
    public void group_aggregate_answer_value_not_a_number() {
        assertEquals("Step requires exactly 1 grouped answer", 1, groupAggregateAnswers.size());
        assertTrue(groupAggregateAnswers.get(0).value().isEmpty());
    }

    private boolean matchAnswer(Map<String, String> answerIdentifiers, ConceptMap answer) {

        Map<String, Reference.Name> nameReferenceMap = new HashMap<>();
        iterate(answer.concepts().keySet()).filter(Identifier.Variable.Retrievable::isName)
                .forEachRemaining(v -> nameReferenceMap.put(v.asName().name(), v.asName().reference().asName()));
        for (Map.Entry<String, String> entry : answerIdentifiers.entrySet()) {
            Reference.Name var = nameReferenceMap.get(entry.getKey());
            String identifier = entry.getValue();

            if (!identifierChecks.containsKey(identifier)) {
                throw new ScenarioDefinitionException(String.format("Identifier \"%s\" hasn't previously been declared.", identifier));
            }

            if (!identifierChecks.get(identifier).check(answer.get(var))) {
                return false;
            }
        }
        return true;
    }

    private boolean matchAnswerConcept(Map<String, String> answerIdentifiers, ConceptMap answer) {
        for (Map.Entry<String, String> entry : answerIdentifiers.entrySet()) {
            String[] identifier = entry.getValue().split(":", 2);
            switch (identifier[0]) {
                case "label":
                    if (!new LabelUniquenessCheck(identifier[1]).check(answer.get(Reference.concept(entry.getKey())))) {
                        return false;
                    }
                    break;
                case "key":
                    if (!new KeyUniquenessCheck(identifier[1]).check(answer.get(Reference.concept(entry.getKey())))) {
                        return false;
                    }
                    break;
                case "attr":
                    if (!new AttributeValueUniquenessCheck(identifier[1]).check(answer.get(Reference.concept(entry.getKey())))) {
                        return false;
                    }
                    break;
                case "value":
                    if (!new ValueUniquenessCheck(identifier[1]).check(answer.get(Reference.value(entry.getKey())))) {
                        return false;
                    }
                    break;
                default:
                    throw new ScenarioDefinitionException("Unrecognised concept type " + identifier[0]);

            }
        }
        return true;
    }

    @Then("get answers of typeql fetch")
    public void get_answers_of_typeql_fetch(String query) {
        TypeQLFetch fetchQuery = TypeQL.parseQuery(query).asFetch();
        fetchAnswers = tx().query().fetch(fetchQuery).toList();
    }

    @Then("typeql fetch; throws exception")
    public void typeql_fetch_throws_exception(String query) {
        assertThrows(() -> {
            TypeQLFetch fetchQuery = TypeQL.parseQuery(query).asFetch();
            tx().query().fetch(fetchQuery).toList();
        });
    }

    @Then("fetch answers are")
    public void fetch_answers_are(String answerJSON) {
        JsonValue json = Json.parse(answerJSON);
        String fetchJSON = fetchAnswers.stream().map(ReadableConceptTree::toJSON)
                .collect(Collectors.joining(",", "[", "]"));
        assertTrue(jsonEquals(json, Json.parse(fetchJSON), false));
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
        Conjunction<?> queryWithIdsConj = TypeQL.and(TypeQL.parsePatternList(queryWithIds));
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
    public void each_answer_satisfies(String templatedTypeQLQuery) {
        String templatedQuery = String.join("\n", templatedTypeQLQuery);
        for (ConceptMap answer : getAnswers) {
            String query = applyQueryTemplate(templatedQuery, answer);
            TypeQLGet typeQLQuery = TypeQL.parseQuery(query).asGet();
            long answerSize = tx().query().get(typeQLQuery).toList().size();
            assertEquals(1, answerSize);
        }
    }

    @Then("templated typeql get; throws exception")
    public void templated_typeql_get_throws_exception(String templatedTypeQLQuery) {
        String templatedQuery = String.join("\n", templatedTypeQLQuery);
        for (ConceptMap answer : getAnswers) {
            String queryString = applyQueryTemplate(templatedQuery, answer);
            assertThrows(() -> {
                TypeQLGet query = TypeQL.parseQuery(queryString).asGet();
                tx().query().get(query).toList();
            });
        }
    }

    @Then("each answer does not satisfy")
    public void each_answer_does_not_satisfy(String templatedTypeQLQuery) {
        String templatedQuery = String.join("\n", templatedTypeQLQuery);
        for (ConceptMap answer : getAnswers) {
            String queryString = applyQueryTemplate(templatedQuery, answer);
            TypeQLGet query = TypeQL.parseQuery(queryString).asGet();
            long answerSize = tx().query().get(query).toList().size();
            assertEquals(0, answerSize);
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
            if (templateFiller.contains(Reference.concept(requiredVariable))) {
                Concept concept = templateFiller.getConcept(requiredVariable);
                if (!concept.isThing())
                    throw new ScenarioDefinitionException("Cannot apply IID templating to Type concepts");
                String conceptId = concept.asThing().getIID().toHexString();
                builder.append(conceptId);

            } else {
                throw new ScenarioDefinitionException(String.format("No IID available for template placeholder: %s.", matched));
            }
            i = matcher.end();
        }
        builder.append(template.substring(i));
        return builder.toString();
    }

    private String variableFromTemplatePlaceholder(String placeholder) {
        if (placeholder.endsWith(".iid")) {
            String stripped = placeholder.replace(".iid", "");
            String withoutPrefix = stripped.replace("answer.", "");
            return withoutPrefix;
        } else {
            throw new ScenarioDefinitionException("Cannot replace template not based on ID.");
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
            if (concept.isType()) {
                return label.equals(concept.asType().getLabel().toString());
            }

            throw new ScenarioDefinitionException("Concept was checked for label uniqueness, but it is not a Type.");
        }
    }

    public static class AttributeUniquenessCheck {

        protected final String type;
        protected final String value;

        AttributeUniquenessCheck(String typeAndValue) {
            String[] s = typeAndValue.split(":", 2);
            assertEquals(
                    String.format("A check for attribute uniqueness should be given in the format \"type:value\", but received %s.", typeAndValue),
                    2, s.length
            );
            type = s[0];
            value = s[1];
        }
    }

    public static class AttributeValueUniquenessCheck extends AttributeUniquenessCheck implements UniquenessCheck {
        AttributeValueUniquenessCheck(String typeAndValue) {
            super(typeAndValue);
        }

        public boolean check(Concept concept) {
            if (!concept.isAttribute()) {
                return false;
            }

            Attribute attribute = concept.asAttribute();

            if (!type.equals(attribute.getType().getLabel().toString())) {
                return false;
            }

            switch (attribute.getType().getValueType()) {
                case BOOLEAN:
                    return Boolean.valueOf(value).equals(attribute.asBoolean().getValue());
                case LONG:
                    return Long.valueOf(value).equals(attribute.asLong().getValue());
                case DOUBLE:
                    return equalsApproximate(Double.parseDouble(value), attribute.asDouble().getValue());
                case STRING:
                    return value.equals(attribute.asString().getValue());
                case DATETIME:
                    LocalDateTime dateTime;
                    try {
                        dateTime = LocalDateTime.parse(value);
                    } catch (DateTimeParseException e) {
                        dateTime = LocalDate.parse(value).atStartOfDay();
                    }
                    return dateTime.equals(attribute.asDateTime().getValue());
                case OBJECT:
                default:
                    throw new ScenarioDefinitionException("Unrecognised value type " + attribute.getType().getValueType());
            }
        }
    }

    public static class KeyUniquenessCheck extends AttributeUniquenessCheck implements UniquenessCheck {
        KeyUniquenessCheck(String typeAndValue) {
            super(typeAndValue);
        }

        @Override
        public boolean check(Concept concept) {
            if (!concept.isThing()) {
                return false;
            }

            Optional<? extends Attribute> keyOpt = concept.asThing().getHas(set(KEY))
                    .filter(attr -> attr.getType().getLabel().toString().equals(type)).first();
            if (keyOpt.isEmpty()) return false;
            Attribute key = keyOpt.get().asAttribute();
            switch (key.getType().getValueType()) {
                case BOOLEAN:
                    return Boolean.valueOf(value).equals(key.asBoolean().getValue());
                case LONG:
                    return Long.valueOf(value).equals(key.asLong().getValue());
                case DOUBLE:
                    return equalsApproximate(Double.parseDouble(value), key.asDouble().getValue());
                case STRING:
                    return value.equals(key.asString().getValue());
                case DATETIME:
                    LocalDateTime dateTime;
                    try {
                        dateTime = LocalDateTime.parse(value);
                    } catch (DateTimeParseException e) {
                        dateTime = LocalDate.parse(value).atStartOfDay();
                    }
                    return dateTime.equals(key.asDateTime().getValue());
                case OBJECT:
                default:
                    throw new ScenarioDefinitionException("Unrecognised value type " + key.getType().getValueType());
            }
        }
    }

    public static class ValueUniquenessCheck implements UniquenessCheck {
        private final String valueType;
        private final String value;

        ValueUniquenessCheck(String valueTypeAndValue) {
            String[] s = valueTypeAndValue.split(":", 2);
            this.valueType = s[0].toLowerCase().strip();
            this.value = s[1].strip();
        }

        public boolean check(Concept concept) {
            if (!concept.isValue()) {
                return false;
            }

            Value<?> value = concept.asValue();

            switch (valueType) {
                case "boolean":
                    assertTrue(value.isBoolean());
                    return this.value.equals(value.asBoolean().value().toString());
                case "long":
                    assertTrue(value.isLong());
                    return this.value.equals(value.asLong().value().toString());
                case "double":
                    assertTrue(value.isDouble());
                    return this.value.equals(value.asDouble().value().toString());
                case "string":
                    assertTrue(value.isString());
                    return this.value.equals(value.asString().value());
                case "datetime":
                    assertTrue(value.isDateTime());
                    LocalDateTime dateTime;
                    try {
                        dateTime = LocalDateTime.parse(this.value);
                    } catch (DateTimeParseException e) {
                        dateTime = LocalDate.parse(this.value).atStartOfDay();
                    }
                    return dateTime.equals(value.asDateTime().value());
                case "object":
                default:
                    throw new ScenarioDefinitionException("Unrecognised value type specified in test " + this.valueType);
            }
        }
    }
}
