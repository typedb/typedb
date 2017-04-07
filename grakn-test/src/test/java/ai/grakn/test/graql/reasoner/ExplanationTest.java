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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graphs.GenealogyGraph;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.test.GraphContext;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class ExplanationTest {


    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext genealogyGraph = GraphContext.preLoad(GenealogyGraph.get());

    @ClassRule
    public static final GraphContext explanationGraph = GraphContext.preLoad("explanationTest.gql");

    private static Concept polibuda;
    private static Concept uw;
    private static Concept warsaw;
    private static Concept masovia;
    private static Concept poland;
    private static Concept europe;
    private static QueryBuilder iqb;

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
        GraknGraph graph = geoGraph.graph();
        iqb = graph.graql().infer(true).materialise(false);
        polibuda = getConcept(graph, "name", "Warsaw-Polytechnics");
        uw = getConcept(graph, "name", "University-of-Warsaw");
        warsaw = getConcept(graph, "name", "Warsaw");
        masovia = getConcept(graph, "name", "Masovia");
        poland = getConcept(graph, "name", "Poland");
        europe = getConcept(graph, "name", "Europe");
    }

    @Test
    public void testTransitiveExplanation() {
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";

        Answer answer1 = new QueryAnswer(ImmutableMap.of(VarName.of("x"), polibuda, VarName.of("y"), warsaw));
        Answer answer2 = new QueryAnswer(ImmutableMap.of(VarName.of("x"), polibuda, VarName.of("y"), masovia));
        Answer answer3 = new QueryAnswer(ImmutableMap.of(VarName.of("x"), polibuda, VarName.of("y"), poland));
        Answer answer4 = new QueryAnswer(ImmutableMap.of(VarName.of("x"), polibuda, VarName.of("y"), europe));

        List<Answer> answers = iqb.<MatchQuery>parse(queryString).admin().streamWithAnswers().collect(Collectors.toList());

        Answer queryAnswer1 = findAnswer(answer1, answers);
        Answer queryAnswer2 = findAnswer(answer2, answers);
        Answer queryAnswer3 = findAnswer(answer3, answers);
        Answer queryAnswer4 = findAnswer(answer4, answers);

        assertTrue(queryAnswer1.getExplanation().isLookupExplanation());

        assertTrue(queryAnswer2.getExplanation().isRuleExplanation());
        assertEquals(2, getLookupExplanations(queryAnswer2).size());
        assertEquals(2, queryAnswer2.getExplicitPath().size());

        assertTrue(queryAnswer3.getExplanation().isRuleExplanation());
        assertEquals(2, getRuleExplanations(queryAnswer3).size());
        assertEquals(3, queryAnswer3.getExplicitPath().size());

        assertTrue(queryAnswer4.getExplanation().isRuleExplanation());
        assertEquals(3, getRuleExplanations(queryAnswer4).size());
        assertEquals(4, queryAnswer4.getExplicitPath().size());
    }

    @Test
    public void testExplainingSpecificAnswer(){
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$x id '" + polibuda.getId() + "';" +
                "$y id '" + europe.getId() + "';";

        MatchQuery query = iqb.parse(queryString);
        List<Answer> answers = query.admin().streamWithAnswers().collect(Collectors.toList());
        assertEquals(answers.size(), 1);

        Answer answer = answers.iterator().next();
        assertTrue(answer.getExplanation().isRuleExplanation());
        assertEquals(2, answer.getExplanation().getAnswers().size());
        assertEquals(3, getRuleExplanations(answer).size());
        assertEquals(4, answer.getExplicitPath().size());
    }

    @Test
    public void testExplainingConjunctiveQueryWithTwoIdPredicates(){
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "(geo-entity: $y, entity-location: $z) isa is-located-in;" +
                "$x id '" + polibuda.getId() + "';" +
                "$z id '" + masovia.getId() + "';" +
                "select $y;";

        MatchQuery query = iqb.parse(queryString);
        List<Answer> answers = query.admin().streamWithAnswers().collect(Collectors.toList());
        assertEquals(answers.size(), 1);
    }

    @Test
    public void testExplainingQueryContainingContradiction(){
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$x id '" + polibuda.getId() + "';" +
                "$y id '" + uw.getId() + "';";

        MatchQuery query = iqb.parse(queryString);
        List<Answer> answers = query.admin().streamWithAnswers().collect(Collectors.toList());
        assertEquals(answers.size(), 0);
    }

    @Test
    public void testExplainingQueryContainingContradiction2(){
        GraknGraph expGraph = explanationGraph.graph();
        QueryBuilder eiqb = expGraph.graql().infer(true);

        Concept a1 = getConcept(expGraph, "name", "a1");
        Concept a2 = getConcept(expGraph, "name", "a2");
        String queryString = "match " +
                "(role1: $x, role2: $y) isa relation1;" +
                "$x id '" + a1.getId() + "';" +
                "$y id '" + a2.getId() + "';";

        MatchQuery query = eiqb.parse(queryString);
        List<Answer> answers = query.admin().streamWithAnswers().collect(Collectors.toList());
        assertEquals(answers.size(), 0);
    }

    private static Concept getConcept(GraknGraph graph, String typeLabel, Object val){
        return graph.graql().match(Graql.var("x").has(typeLabel, val).admin()).execute().iterator().next().get("x");
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

}
