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

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.graql.reasoner.graph.GeoGraph;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Variable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.var;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ExplanationIT {

    private static String resourcePath = "test-integration/graql/reasoner/stubs/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl geoSession;
    private static SessionImpl explanationSession;

    @BeforeClass
    public static void loadContext(){
        geoSession = server.sessionWithNewKeyspace();
        GeoGraph geoGraph = new GeoGraph(geoSession);
        geoGraph.load();
        explanationSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"explanations.gql", explanationSession);
    }

    @AfterClass
    public static void closeSession(){
        geoSession.close();
        explanationSession.close();
    }

    @Test
    public void whenExplainingNonRuleResolvableQuery_explanationsAreEmpty(){
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            String queryString = "match $x isa city, has name $n; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query);
            answers.forEach(ans -> Assert.assertTrue(ans.explanation().isEmpty()));
        }
    }

    @Test
    public void whenQueryingWithReasoningOff_explanationsAreEmpty(){
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            String queryString = "match $x isa city, has name $n; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query, false);
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

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
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
            assertEquals(2, queryAnswer2.explanation().deductions().size());
            assertEquals(4, queryAnswer3.explanation().deductions().size());
            assertEquals(6, queryAnswer4.explanation().deductions().size());

            assertTrue(queryAnswer1.explanation().isLookupExplanation());

            assertTrue(queryAnswer2.explanation().isRuleExplanation());
            assertEquals(2, getLookupExplanations(queryAnswer2).size());
            assertEquals(2, queryAnswer2.explanation().explicit().size());

            assertTrue(queryAnswer3.explanation().isRuleExplanation());
            assertEquals(2, getRuleExplanations(queryAnswer3).size());
            assertEquals(3, queryAnswer3.explanation().explicit().size());

            assertTrue(queryAnswer4.explanation().isRuleExplanation());
            assertEquals(3, getRuleExplanations(queryAnswer4).size());
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

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
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
            assertEquals(6, queryAnswer1.explanation().deductions().size());
            assertEquals(6, queryAnswer2.explanation().deductions().size());

            assertEquals(4, getLookupExplanations(queryAnswer1).size());
            assertEquals(4, queryAnswer1.explanation().explicit().size());

            assertEquals(4, getLookupExplanations(queryAnswer2).size());
            assertEquals(4, queryAnswer2.explanation().explicit().size());
        }
    }

    @Test
    public void whenExplainingAGroundQuery_explanationsAreCorrect(){
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            Concept polibuda = getConcept(tx, "name", "Warsaw-Polytechnics");
            Concept europe = getConcept(tx, "name", "Europe");
            String queryString = "match " +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "$x id '" + polibuda.id() + "';" +
                    "$y id '" + europe.id() + "'; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query);
            assertEquals(answers.size(), 1);

            ConceptMap answer = answers.iterator().next();
            assertTrue(answer.explanation().isRuleExplanation());
            assertEquals(2, answer.explanation().getAnswers().size());
            assertEquals(3, getRuleExplanations(answer).size());
            assertEquals(4, answer.explanation().explicit().size());
            testExplanation(answers);
        }
    }

    @Test
    public void whenExplainingConjunctiveQueryWithTwoIdPredicates_explanationsAreCorrect(){
        try (Transaction tx = geoSession.transaction(Transaction.Type.READ)) {
            Concept polibuda = getConcept(tx, "name", "Warsaw-Polytechnics");
            Concept masovia = getConcept(tx, "name", "Masovia");
            String queryString = "match " +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "(geo-entity: $y, entity-location: $z) isa is-located-in;" +
                    "$x id '" + polibuda.id() + "';" +
                    "$z id '" + masovia.id() + "';" +
                    "get $y;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query);
            assertEquals(answers.size(), 1);
            testExplanation(answers);
        }
    }

    @Test
    public void whenExplainingConjunctions_explanationsAreCorrect(){
        try (Transaction tx = explanationSession.transaction(Transaction.Type.READ)) {
            String queryString = "match " +
                    "(role1: $x, role2: $w) isa inferredRelation;" +
                    "$x has name $xName;" +
                    "$w has name $wName; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query);
            testExplanation(answers);
        }
    }

    @Test
    public void whenExplainingMixedAtomicQueries_explanationsAreCorrect(){
        try (Transaction tx = explanationSession.transaction(Transaction.Type.READ)) {
            String queryString = "match " +
                    "$x has value 'high';" +
                    "($x, $y) isa carried-relation;" +
                    "get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query);
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
    public void whenExplainingEquivalentPartialQueries_explanationsAreCorrect(){
        try (Transaction tx = explanationSession.transaction(Transaction.Type.WRITE)) {
            String queryString = "match $x isa same-tag-column-link; get;";

            GraqlGet query = Graql.parse(queryString);
            List<ConceptMap> answers = tx.execute(query);
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


    private void testExplanation(Collection<ConceptMap> answers){
        answers.forEach(ans -> testExplanation(ans));
    }

    private void testExplanation(ConceptMap answer){
        answerHasConsistentExplanations(answer);
        checkExplanationCompleteness(answer);
        checkAnswerConnectedness(answer);
    }

    //ensures that each branch ends up with an lookup explanation
    private void checkExplanationCompleteness(ConceptMap answer){
        assertFalse("Non-lookup explanation misses children",
                answer.explanations().stream()
                        .filter(e -> !e.isLookupExplanation())
                        .anyMatch(e -> e.getAnswers().isEmpty())
        );
    }

    private void checkAnswerConnectedness(ConceptMap answer){
        List<ConceptMap> answers = answer.explanation().getAnswers();
        answers.forEach(a -> {
            assertTrue("Disconnected answer in explanation",
                    answers.stream()
                            .filter(a2 -> !a2.equals(a))
                            .anyMatch(a2 -> !Sets.intersection(a.vars(), a2.vars()).isEmpty())
            );
        });
    }

    private void answerHasConsistentExplanations(ConceptMap answer){
        Set<ConceptMap> answers = answer.explanation().deductions().stream()
                .filter(a -> !a.explanation().isJoinExplanation())
                .collect(Collectors.toSet());

        answers.forEach(a -> assertTrue("Answer has inconsistent explanations", explanationConsistentWithAnswer(a)));
    }

    private static Concept getConcept(Transaction tx, String typeLabel, String val){
        return tx.stream(Graql.match(var("x").has(typeLabel, val)).get("x"))
                .map(ans -> ans.get("x")).findAny().orElse(null);
    }

    private ConceptMap findAnswer(ConceptMap a, List<ConceptMap> list){
        for(ConceptMap ans : list) {
            if (ans.equals(a)) return ans;
        }
        return new ConceptMap();
    }

    private Set<Explanation> getRuleExplanations(ConceptMap a){
        return a.explanations().stream().filter(Explanation::isRuleExplanation).collect(Collectors.toSet());
    }

    private Set<Explanation> getLookupExplanations(ConceptMap a){
        return a.explanations().stream().filter(Explanation::isLookupExplanation).collect(Collectors.toSet());
    }

    private boolean explanationConsistentWithAnswer(ConceptMap ans){
        Pattern queryPattern = ans.explanation().getPattern();
        Set<Variable> vars = new HashSet<>();
        if (queryPattern != null){
            queryPattern.statements().forEach(s -> vars.addAll(s.variables()));
        }
        return vars.containsAll(ans.map().keySet());
    }
}