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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.test.kbs.GeoKB;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class ExplanationTest {


    @ClassRule
    public static final SampleKBContext geoKB = GeoKB.context();

    @ClassRule
    public static final SampleKBContext genealogyKB = GenealogyKB.context();

    @ClassRule
    public static final SampleKBContext explanationKB = SampleKBContext.load("explanationTest.gql");

    private static Concept polibuda, uw;
    private static Concept warsaw;
    private static Concept masovia;
    private static Concept poland;
    private static Concept europe;
    private static QueryBuilder iqb;

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestSetup.usingTinker());
        GraknTx tx = geoKB.tx();
        iqb = tx.graql().infer(true).materialise(false);
        polibuda = getConcept(tx, "name", "Warsaw-Polytechnics");
        uw = getConcept(tx, "name", "University-of-Warsaw");
        warsaw = getConcept(tx, "name", "Warsaw");
        masovia = getConcept(tx, "name", "Masovia");
        poland = getConcept(tx, "name", "Poland");
        europe = getConcept(tx, "name", "Europe");
    }

    @Test
    public void testExplanationTreeCorrect_TransitiveClosure() {
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";

        Answer answer1 = new QueryAnswer(ImmutableMap.of(Graql.var("x"), polibuda, Graql.var("y"), warsaw));
        Answer answer2 = new QueryAnswer(ImmutableMap.of(Graql.var("x"), polibuda, Graql.var("y"), masovia));
        Answer answer3 = new QueryAnswer(ImmutableMap.of(Graql.var("x"), polibuda, Graql.var("y"), poland));
        Answer answer4 = new QueryAnswer(ImmutableMap.of(Graql.var("x"), polibuda, Graql.var("y"), europe));

        List<Answer> answers = iqb.<GetQuery>parse(queryString).execute();
        answers.forEach(a -> assertTrue(answerHasConsistentExplanations(a)));

        Answer queryAnswer1 = findAnswer(answer1, answers);
        Answer queryAnswer2 = findAnswer(answer2, answers);
        Answer queryAnswer3 = findAnswer(answer3, answers);
        Answer queryAnswer4 = findAnswer(answer4, answers);

        assertEquals(queryAnswer1, answer1);
        assertEquals(queryAnswer2, answer2);
        assertEquals(queryAnswer3, answer3);
        assertEquals(queryAnswer4, answer4);

        assertEquals(queryAnswer1.getAnswers().size(), 1);
        assertEquals(queryAnswer2.getAnswers().size(), 3);
        assertEquals(queryAnswer3.getAnswers().size(), 5);
        assertEquals(queryAnswer4.getAnswers().size(), 7);

        assertTrue(queryAnswer1.getExplanation().isLookupExplanation());
        assertTrue(answerHasConsistentExplanations(queryAnswer1));

        assertTrue(queryAnswer2.getExplanation().isRuleExplanation());
        assertEquals(2, getLookupExplanations(queryAnswer2).size());
        assertEquals(2, queryAnswer2.getExplicitPath().size());
        assertTrue(answerHasConsistentExplanations(queryAnswer2));

        assertTrue(queryAnswer3.getExplanation().isRuleExplanation());
        assertEquals(2, getRuleExplanations(queryAnswer3).size());
        assertEquals(3, queryAnswer3.getExplicitPath().size());
        assertTrue(answerHasConsistentExplanations(queryAnswer3));


        assertTrue(queryAnswer4.getExplanation().isRuleExplanation());
        assertEquals(3, getRuleExplanations(queryAnswer4).size());
        assertEquals(4, queryAnswer4.getExplicitPath().size());
        assertTrue(answerHasConsistentExplanations(queryAnswer4));
    }

    @Test
    public void testExplanationTreeCorrect_TransitiveClosureWithSpecificResourceAndTypes() {
        String queryString = "match $x isa university;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y isa country;$y has name 'Poland'; get;";

        Answer answer1 = new QueryAnswer(ImmutableMap.of(Graql.var("x"), polibuda, Graql.var("y"), poland));
        Answer answer2 = new QueryAnswer(ImmutableMap.of(Graql.var("x"), uw, Graql.var("y"), poland));

        List<Answer> answers = iqb.<GetQuery>parse(queryString).execute();
        answers.forEach(a -> assertTrue(answerHasConsistentExplanations(a)));

        Answer queryAnswer1 = findAnswer(answer1, answers);
        Answer queryAnswer2 = findAnswer(answer2, answers);
        assertEquals(queryAnswer1, answer1);
        assertEquals(queryAnswer2, answer2);

        assertTrue(queryAnswer1.getExplanation().isJoinExplanation());
        assertTrue(queryAnswer2.getExplanation().isJoinExplanation());

        assertEquals(queryAnswer1.getAnswers().size(), 7);
        assertEquals(queryAnswer2.getAnswers().size(), 7);

        assertEquals(4, getLookupExplanations(queryAnswer1).size());
        assertEquals(4, queryAnswer1.getExplicitPath().size());
        assertTrue(answerHasConsistentExplanations(queryAnswer1));

        assertEquals(4, getLookupExplanations(queryAnswer2).size());
        assertEquals(4, queryAnswer2.getExplicitPath().size());
        assertTrue(answerHasConsistentExplanations(queryAnswer2));
    }

    @Test
    public void testExplanationTreeCorrect_QueryingSpecificAnswer(){
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$x id '" + polibuda.getId() + "';" +
                "$y id '" + europe.getId() + "'; get;";

        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.execute();
        assertEquals(answers.size(), 1);

        Answer answer = answers.iterator().next();
        assertTrue(answer.getExplanation().isRuleExplanation());
        assertEquals(2, answer.getExplanation().getAnswers().size());
        assertEquals(3, getRuleExplanations(answer).size());
        assertEquals(4, answer.getExplicitPath().size());
        answers.forEach(a -> assertTrue(answerHasConsistentExplanations(a)));
    }

    @Test
    public void testExplainingConjunctiveQueryWithTwoIdPredicates(){
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "(geo-entity: $y, entity-location: $z) isa is-located-in;" +
                "$x id '" + polibuda.getId() + "';" +
                "$z id '" + masovia.getId() + "';" +
                "get $y;";

        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.execute();
        assertEquals(answers.size(), 1);
        answers.forEach(a -> assertTrue(answerHasConsistentExplanations(a)));
    }

    @Test
    public void testExplainingQueryContainingContradiction(){
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$x id '" + polibuda.getId() + "';" +
                "$y id '" + uw.getId() + "'; get;";

        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.execute();
        assertEquals(answers.size(), 0);
    }

    @Test
    public void testExplainingNonRuleResolvableQuery(){
        String queryString = "match $x isa city, has name $n; get;";

        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.execute();
        answers.forEach(ans -> assertEquals(ans.getExplanation().isEmpty(), true));
    }

    @Test
    public void testExplainingQueryContainingContradiction2(){
        GraknTx expGraph = explanationKB.tx();
        QueryBuilder eiqb = expGraph.graql().infer(true);

        Concept a1 = getConcept(expGraph, "name", "a1");
        Concept a2 = getConcept(expGraph, "name", "a2");
        String queryString = "match " +
                "(role1: $x, role2: $y) isa relation1;" +
                "$x id '" + a1.getId() + "';" +
                "$y id '" + a2.getId() + "'; get;";

        GetQuery query = eiqb.parse(queryString);
        List<Answer> answers = query.execute();
        assertEquals(answers.size(), 0);
    }

    @Test
    public void testExplainingConjunctions(){
        GraknTx expGraph = explanationKB.tx();
        QueryBuilder eiqb = expGraph.graql().infer(true);

        String queryString = "match " +
                "(role1: $x, role2: $w) isa inferredRelation;" +
                "$x has name $xName;" +
                "$w has name $wName; get;";

        GetQuery query = eiqb.parse(queryString);
        List<Answer> answers = query.execute();
        answers.forEach(a -> assertTrue(answerHasConsistentExplanations(a)));
    }

    private static Concept getConcept(GraknTx graph, String typeLabel, Object val){
        return graph.graql().match(Graql.var("x").has(typeLabel, val)).get("x").findAny().get();
    }

    private Answer findAnswer(Answer a, List<Answer> list){
        for(Answer ans : list) {
            if (ans.equals(a)) return ans;
        }
        return new QueryAnswer();
    }

    private Set<AnswerExplanation> getRuleExplanations(Answer a){
        return a.getExplanations().stream().filter(AnswerExplanation::isRuleExplanation).collect(Collectors.toSet());
    }

    private Set<AnswerExplanation> getLookupExplanations(Answer a){
        return a.getExplanations().stream().filter(AnswerExplanation::isLookupExplanation).collect(Collectors.toSet());
    }

    private boolean answerHasConsistentExplanations(Answer ans){
        Set<Answer> answers = ans.getAnswers().stream()
                .filter(a -> !a.getExplanation().isJoinExplanation())
                .collect(Collectors.toSet());
        for (Answer a : answers){
            if (!isExplanationConsistentWithAnswer(a)){
                return false;
            }
        }
        return true;
    }

    private boolean isExplanationConsistentWithAnswer(Answer ans){
        ReasonerQuery query = ans.getExplanation().getQuery();
        Set<Var> vars = query != null? query.getVarNames() : new HashSet<>();
        return vars.containsAll(ans.map().keySet());
    }
}
