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

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.graql.reasoner.graph.GeoGraph;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.cache.ExplanationCache;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.property.IdProperty;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Variable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.var;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ExplanationIT {

    private static String resourcePath = "test/integration/graql/reasoner/stubs/";

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session geoSession;
    private static Session explanationSession;

    @BeforeClass
    public static void loadContext() {
        geoSession = emptySession();
        GeoGraph geoGraph = new GeoGraph(geoSession);
        geoGraph.load();
        explanationSession = emptySession();
        loadFromFileAndCommit(resourcePath, "explanations.gql", explanationSession);
    }

    @AfterClass
    public static void closeSession() {
        geoSession.close();
        explanationSession.close();
    }

    private static Session emptySession() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        return SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
    }

    @Test
    public void whenExplainingNonRuleResolvableQuery_explanationsAreEmpty() {
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            String queryString = "match $x isa city, has name $n; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query, true, true);
            answers.forEach(ans -> Assert.assertTrue(ans.explanation().isEmpty()));
        }
    }

    @Test
    public void whenQueryingWithReasoningOff_explanationsAreEmpty() {
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            String queryString = "match $x isa city, has name $n; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query, false, true);
            answers.forEach(ans -> Assert.assertTrue(ans.explanation().isEmpty()));
        }
    }

    @Test
    public void whenExplainingTransitiveClosure_explanationsAreCorrectlyNested() {
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";

            Concept polibuda = getConcept(tx, "name", "Warsaw-Polytechnics");
            Concept warsaw = getConcept(tx, "name", "Warsaw");
            Concept masovia = getConcept(tx, "name", "Masovia");
            Concept poland = getConcept(tx, "name", "Poland");
            Concept europe = getConcept(tx, "name", "Europe");
            ConceptMap answer1 = new ConceptMap(ImmutableMap.of(var("x").var(), polibuda, var("y").var(), warsaw));
            ConceptMap answer2 = new ConceptMap(ImmutableMap.of(var("x").var(), polibuda, var("y").var(), masovia));
            ConceptMap answer3 = new ConceptMap(ImmutableMap.of(var("x").var(), polibuda, var("y").var(), poland));
            ConceptMap answer4 = new ConceptMap(ImmutableMap.of(var("x").var(), polibuda, var("y").var(), europe));

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet(), true, true);
            testExplanation(answers);

            ConceptMap queryAnswer1 = findAnswer(answer1, answers);
            ConceptMap queryAnswer2 = findAnswer(answer2, answers);
            ConceptMap queryAnswer3 = findAnswer(answer3, answers);
            ConceptMap queryAnswer4 = findAnswer(answer4, answers);

            assertEquals(queryAnswer1, answer1);
            assertEquals(queryAnswer2, answer2);
            assertEquals(queryAnswer3, answer3);
            assertEquals(queryAnswer4, answer4);

            assertEquals(0, queryAnswer1.explanation().deductions().size());
            assertEquals(3, queryAnswer2.explanation().deductions().size());
            assertEquals(6, queryAnswer3.explanation().deductions().size());
            assertEquals(9, queryAnswer4.explanation().deductions().size());

            assertTrue(queryAnswer1.explanation().isLookupExplanation());

            assertTrue(queryAnswer2.explanation().isRuleExplanation());
            assertEquals(2, getLookupExplanations(queryAnswer2).size());
            assertEquals(1, getRuleExplanations(queryAnswer2).size());
            assertEquals(1, getJoinExplanations(queryAnswer2).size());
            assertEquals(2, queryAnswer2.explanation().explicit().size());

            assertTrue(queryAnswer3.explanation().isRuleExplanation());
            assertEquals(2, getRuleExplanations(queryAnswer3).size());
            assertEquals(2, getJoinExplanations(queryAnswer3).size());
            assertEquals(3, queryAnswer3.explanation().explicit().size());

            assertTrue(queryAnswer4.explanation().isRuleExplanation());
            assertEquals(3, getRuleExplanations(queryAnswer4).size());
            assertEquals(3, getJoinExplanations(queryAnswer4).size());
            assertEquals(4, queryAnswer4.explanation().explicit().size());
        }
    }

    @Test
    public void whenExplainingTransitiveClosureWithSpecificResourceAndTypes_explanationsAreCorrect() {
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            String queryString = "match $x isa university;" +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "$y isa country;$y has name 'Poland'; get;";

            Concept polibuda = getConcept(tx, "name", "Warsaw-Polytechnics");
            Concept uw = getConcept(tx, "name", "University-of-Warsaw");
            Concept poland = getConcept(tx, "name", "Poland");
            ConceptMap answer1 = new ConceptMap(ImmutableMap.of(var("x").var(), polibuda, var("y").var(), poland));
            ConceptMap answer2 = new ConceptMap(ImmutableMap.of(var("x").var(), uw, var("y").var(), poland));

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet(), true, true);
            testExplanation(answers);

            ConceptMap queryAnswer1 = findAnswer(answer1, answers);
            ConceptMap queryAnswer2 = findAnswer(answer2, answers);
            assertEquals(queryAnswer1, answer1);
            assertEquals(queryAnswer2, answer2);

            assertTrue(queryAnswer1.explanation().isJoinExplanation());
            assertTrue(queryAnswer2.explanation().isJoinExplanation());

        /*
        (res), (uni, ctr) - (region, ctr)
                          - (uni, region) - {(city, region), (uni, city)
                          */
            assertEquals(8, queryAnswer1.explanation().deductions().size());
            assertEquals(4, queryAnswer1.explanation().explicit().size());
            assertEquals(4, getLookupExplanations(queryAnswer1).size());
            assertEquals(2, getRuleExplanations(queryAnswer1).size());
            assertEquals(3, getJoinExplanations(queryAnswer1).size());

            assertEquals(8, queryAnswer2.explanation().deductions().size());
            assertEquals(4, queryAnswer2.explanation().explicit().size());
            assertEquals(4, getLookupExplanations(queryAnswer2).size());
            assertEquals(2, getRuleExplanations(queryAnswer2).size());
            assertEquals(3, getJoinExplanations(queryAnswer2).size());
        }
    }

    @Test
    public void whenExplainingAGroundQuery_explanationsAreCorrect() {
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            Concept polibuda = getConcept(tx, "name", "Warsaw-Polytechnics");
            Concept europe = getConcept(tx, "name", "Europe");
            String queryString = "match " +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "$x id " + polibuda.id() + ";" +
                    "$y id " + europe.id() + "; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query, true, true);
            assertEquals(answers.size(), 1);

            ConceptMap answer = answers.iterator().next();
            assertTrue(answer.explanation().isRuleExplanation());
            assertEquals(1, answer.explanation().getAnswers().size());
            assertEquals(3, getRuleExplanations(answer).size());
            assertEquals(3, getJoinExplanations(answer).size());
            assertEquals(4, answer.explanation().explicit().size());
            testExplanation(answers);
        }
    }

    @Test
    public void whenExplainingConjunctiveQueryWithTwoIdPredicates_explanationsAreCorrect() {
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            Concept polibuda = getConcept(tx, "name", "Warsaw-Polytechnics");
            Concept masovia = getConcept(tx, "name", "Masovia");
            String queryString = "match " +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "(geo-entity: $y, entity-location: $z) isa is-located-in;" +
                    "$x id " + polibuda.id() + ";" +
                    "$z id " + masovia.id() + ";" +
                    "get $y;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query, true, true);
            assertEquals(answers.size(), 1);
            testExplanation(answers);
        }
    }

    @Test
    public void whenExplainingConjunctions_explanationsAreCorrect() {
        try (Transaction tx = explanationSession.transaction(Transaction.Type.READ)) {
            String queryString = "match " +
                    "(object: $obj, subject: $company) isa carried-relation;" +
                    "$obj has value $obj-value; " +
                    "$company has value $company-value; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query, true, true);
            testExplanation(answers);
        }
    }

    @Test
    public void whenExplainingMixedAtomicQueries_explanationsAreCorrect() {
        try (Transaction tx = explanationSession.transaction(Transaction.Type.READ)) {
            String queryString = "match " +
                    "$x has value 'high';" +
                    "($x, $y) isa carried-relation;" +
                    "get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query, true, true);
            testExplanation(answers);
            answers.stream()
                    .filter(ans -> ans.explanations().stream().anyMatch(Explanation::isRuleExplanation))
                    .forEach(inferredAnswer -> {
                        Set<Explanation> explanations = inferredAnswer.explanations();
                        assertEquals(explanations.stream().filter(Explanation::isRuleExplanation).count(), 2);
                        assertEquals(explanations.stream().filter(Explanation::isLookupExplanation).count(), 4);
                    });
        }
    }

    @Test
    public void whenExplainingEquivalentPartialQueries_explanationsAreCorrect() {
        try (Transaction tx = explanationSession.transaction(Transaction.Type.WRITE)) {
            String queryString = "match $x isa same-tag-column-link; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query,true, true);
            testExplanation(answers);
            answers.stream()
                    .filter(ans -> ans.explanations().stream().anyMatch(Explanation::isRuleExplanation))
                    .forEach(inferredAnswer -> {
                        Set<Explanation> explanations = inferredAnswer.explanations();
                        assertEquals(explanations.stream().filter(Explanation::isRuleExplanation).count(), 1);
                        assertEquals(explanations.stream().filter(Explanation::isLookupExplanation).count(), 3);
                    });
        }
    }


    @Test
    public void whenQueryingWithExplainFlag_explanationIsCached() {
        try (Transaction tx = explanationSession.transaction(Transaction.Type.WRITE)) {
            String query = "match $x isa operates; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(query).asGet(), true, true);

            TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
            ExplanationCache explanationCache = testTx.explanationCache();

            // each answer should have an entry in the explanationCache
            for (ConceptMap answer : answers) {
                Explanation explanation = explanationCache.get(answer);
                assertNotNull(explanation);
                assertEquals(answer.explanation(), explanation);
            }
        }
    }


    @Test
    public void whenQueryingWithDefaults_explanationIsNotCached() {
        try (Transaction tx = explanationSession.transaction(Transaction.Type.WRITE)) {
            String query = "match $x isa operates; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(query).asGet());

            TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
            ExplanationCache explanationCache = testTx.explanationCache();

            // each answer should NOT have an entry in the explanationCache, even if inferred an answer
            for (ConceptMap answer : answers) {
                assertNull(explanationCache.get(answer));
            }
        }
    }

    @Test
    public void whenQueryingWithNoExplain_explanationIsNotCached() {
        try (Transaction tx = explanationSession.transaction(Transaction.Type.WRITE)) {
            String query = "match $x isa operates; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(query).asGet(), true, false);

            TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
            ExplanationCache explanationCache = testTx.explanationCache();

            // each answer should NOT have an entry in the explanationCache, even if inferred an answer
            for (ConceptMap answer : answers) {
                assertNull(explanationCache.get(answer));
            }
        }
    }

    @Test
    public void whenRequestingSubExplanationViaTransaction_subExplanationsAreCachedLazily() {
        try (Transaction tx = explanationSession.transaction(Transaction.Type.WRITE)) {
            String query = "match $x isa operates; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(query).asGet(), true, true);

            TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
            ExplanationCache explanationCache = testTx.explanationCache();

            for (ConceptMap answer : answers) {
                Explanation explanation = testTx.explanation(answer);

                // each sub-explanation should now be in the cache
                for (ConceptMap subAnswer : explanation.getAnswers()) {
                    Explanation subExplanation = explanationCache.get(subAnswer);
                    assertNotNull(subExplanation);
                    assertEquals(subExplanation, subAnswer.explanation());
                }
            }
        }
    }

    @Test
    public void onDelete_explanationCacheIsCleared() {
        try (Transaction tx = explanationSession.transaction(Transaction.Type.WRITE)) {
            String query = "match $x isa operates; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(query).asGet(), true, true);

            TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
            ExplanationCache explanationCache = testTx.explanationCache();

            for (ConceptMap answer : answers) {
                assertNotNull(explanationCache.get(answer));
            }

            // execute a delete query
            tx.execute(Graql.parse("match $x isa value; delete $x isa value;").asDelete());

            for (ConceptMap answer : answers) {
                assertNull(explanationCache.get(answer));
            }
        }
    }

    /**
     * Validates issue#3061 is fixed.
     * <p>
     * The test illustrates the following issue. The `pair` and `has name` rules are mutually recursive - one fires the other.
     * As a result, when resolving the second top atom of the input conjunctive query the answer is already in the cache.
     * The answer contains a `RuleExplanation`.
     * However, the query to be checked in cache is:
     * <p>
     * `$p isa pair;`,
     * <p>
     * whereas the cache contains:
     * <p>
     * `$p (prep: $prep, pobj: $pobj) isa pair;`,
     * <p>
     * which are not equivalent.
     * Although the cache contained query will contain all answers to `$p isa pair;`, this is a special case.
     * If we had an instance of pair relation with only a single roleplayer, recognising a subsumption relation between
     * the queries would lead to incomplete answers.
     */
    @Ignore("We cannot solve this with current answer handling/cache implementation. Maybe cardinality constraints would help?")
    @Test
    public void whenRulesAreMutuallyRecursive_explanationsAreRecognisedAsRuleOnes() {
        try (Session session = emptySession()) {
            loadFromFileAndCommit(resourcePath, "testSet30.gql", session);
            for (int i = 0; i < 10; i++) {
                try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                    String queryString = "match $p isa pair, has name 'ff'; get;";
                    List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet(), true, true);
                    answers.forEach(joinedAnswer -> {
                        testExplanation(joinedAnswer);
                        joinedAnswer.explanation().getAnswers()
                                .forEach(inferredAnswer -> {
                                            assertTrue(inferredAnswer.explanation().isRuleExplanation());
                                        }
                                );
                    });
                }
            }
        }
    }

    private void testExplanation(Collection<ConceptMap> answers) {
        answers.forEach(this::testExplanation);
    }

    private void testExplanation(ConceptMap answer) {
        answerHasConsistentExplanations(answer);
        checkExplanationCompleteness(answer);
        checkAnswerConnectedness(answer);
        if (answer.getPattern() != null) {
            patternContainsIdPerVariable(answer.getPattern());
        }
    }

    //ensures that each branch ends up with an lookup explanation
    private void checkExplanationCompleteness(ConceptMap answer) {
        assertFalse("Non-lookup explanation misses children",
                answer.explanations().stream()
                        .filter(e -> !e.isLookupExplanation())
                        .anyMatch(e -> e.getAnswers().isEmpty())
        );
    }

    private void checkAnswerConnectedness(ConceptMap answer) {
        Explanation explanation = answer.explanation();
        if (!explanation.isRuleExplanation()) {
            List<ConceptMap> answers = explanation.getAnswers();
            answers.forEach(a -> {
                assertTrue("Disconnected answer in explanation",
                        answers.stream()
                                .filter(a2 -> !a2.equals(a))
                                .anyMatch(a2 -> !Sets.intersection(a.vars(), a2.vars()).isEmpty())
                );
            });
        }
    }

    private void answerHasConsistentExplanations(ConceptMap answer) {
        if (answer.explanation() == null) {
            throw new RuntimeException("answer has null explanation");
        }
        Set<ConceptMap> answers = answer.explanation().deductions().stream()
                .filter(a -> !a.explanation().isJoinExplanation())
                .collect(Collectors.toSet());

        answers.forEach(a -> assertTrue("Answer has inconsistent explanations", explanationConsistentWithAnswer(a)));
    }

    private static Concept getConcept(Transaction tx, String typeLabel, String val) {
        return tx.stream(Graql.match(var("x").has(typeLabel, val)).get("x"))
                .map(ans -> ans.get("x")).findAny().orElse(null);
    }

    private ConceptMap findAnswer(ConceptMap a, List<ConceptMap> list) {
        for (ConceptMap ans : list) {
            if (ans.equals(a)) return ans;
        }
        return new ConceptMap();
    }

    private Set<Explanation> getRuleExplanations(ConceptMap a) {
        return a.explanations().stream().filter(Explanation::isRuleExplanation).collect(Collectors.toSet());
    }

    private Set<Explanation> getJoinExplanations(ConceptMap a) {
        return a.explanations().stream().filter(Explanation::isJoinExplanation).collect(Collectors.toSet());
    }

    private Set<Explanation> getLookupExplanations(ConceptMap a) {
        return a.explanations().stream().filter(Explanation::isLookupExplanation).collect(Collectors.toSet());
    }

    private boolean explanationConsistentWithAnswer(ConceptMap ans) {
        Pattern queryPattern = ans.getPattern();
        Map<Variable, String> varIds = new HashMap<>();
        if (queryPattern != null) {
            queryPattern.statements().stream()
                    .filter(statement -> statement.hasProperty(IdProperty.class))
                    .forEach(statement -> {
                        String id = statement.getProperty(IdProperty.class).get().id();
                        Variable var = statement.var();
                        varIds.put(var, id);
                    });
        }
        boolean allVariableIdsMatch = true;
        for (Map.Entry<Variable, Concept> mapping : ans.map().entrySet()) {
            allVariableIdsMatch &= mapping.getValue().id().toString().equals(varIds.get(mapping.getKey()));
        }
        return allVariableIdsMatch;
    }

    private void patternContainsIdPerVariable(Pattern pattern) {
        Set<Variable> variables = pattern.variables();
        long variablesWithIds = pattern.statements().stream()
                .filter(statement -> statement.hasProperty(IdProperty.class))
                .count();
        assertEquals(variables.size(), variablesWithIds);
    }
}
