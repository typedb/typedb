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
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.internal.reasoner.Reasoner;
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

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void testTransitiveExplanation() {
        GraknGraph graph = geoGraph.graph();
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";

        Concept polibuda = getConcept(graph, "name", "Warsaw-Polytechnics");
        Concept warsaw = getConcept(graph, "name", "Warsaw");
        Concept masovia = getConcept(graph, "name", "Masovia");
        Concept poland = getConcept(graph, "name", "Poland");
        Concept europe = getConcept(graph, "name", "Europe");

        Answer answer1 = new QueryAnswer(ImmutableMap.of(VarName.of("x"), polibuda, VarName.of("y"), warsaw));
        Answer answer2 = new QueryAnswer(ImmutableMap.of(VarName.of("x"), polibuda, VarName.of("y"), masovia));
        Answer answer3 = new QueryAnswer(ImmutableMap.of(VarName.of("x"), polibuda, VarName.of("y"), poland));
        Answer answer4 = new QueryAnswer(ImmutableMap.of(VarName.of("x"), polibuda, VarName.of("y"), europe));

        MatchQuery query = graph.graql().parse(queryString);

        List<Answer> answers = Reasoner.resolveWithExplanation(query, false).collect(Collectors.toList());

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
        GraknGraph graph = geoGraph.graph();
        Concept polibuda = getConcept(graph, "name", "Warsaw-Polytechnics");
        Concept europe = getConcept(graph, "name", "Europe");

        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$x id '" + polibuda.getId() + "';" +
                "$y id '" + europe.getId() + "';";

        MatchQuery query = graph.graql().parse(queryString);
        List<Answer> answers = Reasoner.resolveWithExplanation(query, false).collect(Collectors.toList());
        assertEquals(answers.size(), 1);

        Answer answer = answers.iterator().next();
        assertTrue(answer.getExplanation().isRuleExplanation());
        assertEquals(3, getRuleExplanations(answer).size());
        assertEquals(4, answer.getExplicitPath().size());
    }

    private Concept getConcept(GraknGraph graph, String typeName, Object val){
        return graph.graql().match(Graql.var("x").has(typeName, val).admin()).execute().iterator().next().get("x");
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
