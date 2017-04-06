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

package ai.grakn.test.graql.reasoner.inference;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.test.GraphContext;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.test.GraknTestEnv.usingTinker;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class GeoInferenceTest {

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Rule
    public final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @Test
    public void testTransitiveQueryWithTypes() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $x isa city;"+
                        "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                        "$y isa country;$y has name 'Poland'; select $x;";

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name val 'Warsaw';} or {$name val 'Wroclaw';};select $x;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransitiveQueryWithTypes_NoRoles() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match " +
                "$z1 isa city;$z1 has name $name;" +
                "($z1, $z2) isa is-located-in;" +
                "$z2 isa country;$z2 has name 'Poland';" +
                "select $z1, $name;";
        String queryString2 = "match " +
                "$z1 isa country;$z1 has name 'Poland';" +
                "$z2 isa city;$z2 has name $name;"+
                "($z1, $z2) isa is-located-in;" +
                "select $z2, $name;";
        String explicitQuery = "match " +
                "$z1 isa city;$z1 has name $name;{$name val 'Warsaw';} or {$name val 'Wroclaw';};select $z1, $name;";
        String explicitQuery2 = "match " +
                "$z2 isa city;$z2 has name $name;{$name val 'Warsaw';} or {$name val 'Wroclaw';};select $z2, $name;";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        QueryAnswers explicitAnswers = queryAnswers(qb.parse(explicitQuery));

        assertEquals(answers, explicitAnswers);
        //assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        //assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));

        assertQueriesEqual(iqb.materialise(false).parse(queryString2), qb.parse(explicitQuery2));
        assertQueriesEqual(iqb.materialise(true).parse(queryString2), qb.parse(explicitQuery2));
    }

    @Test
    public void testTransitiveQueryWithTypes2() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match " +
                "$x isa university;$x has name $name;"+
                "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland';" +
                "select $x, $name;";
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        QueryAnswers explicitAnswers = queryAnswers(qb.parse(explicitQuery));
        assertEquals(answers.size(), explicitAnswers.size());
        assertEquals(answers, explicitAnswers);
        QueryAnswers answers2 = queryAnswers(iqb.materialise(true).parse(queryString));
        assertEquals(answers, answers2);
    }

    @Test
    public void testTransitiveQueryWithTypes2_NoRoles() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $z1 isa university;$z1 has name $name;"+
                "($z1, $z2) isa is-located-in;$z2 isa country;$z2 has name 'Poland'; select $z1, $name;";
        String queryString2 = "match $z2 isa university;$z2 has name $name;"+
                "($z1, $z2) isa is-located-in;$z1 isa country;$z1 has name 'Poland'; select $z2, $name;";
        String explicitQuery = "match " +
                "$z1 isa university;$z1 has name $name;" +
                "{$z1 has name 'University-of-Warsaw';} or {$z1 has name'Warsaw-Polytechnics';};";
        String explicitQuery2 = "match " +
                "$z2 isa university;$z2 has name $name;" +
                "{$z2 has name 'University-of-Warsaw';} or {$z2 has name'Warsaw-Polytechnics';};";
        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(false).parse(queryString2), qb.parse(explicitQuery2));
        assertQueriesEqual(iqb.materialise(true).parse(queryString2), qb.parse(explicitQuery2));
    }

    @Test
    public void testSpecificTransitiveQuery() {
        GraknGraph graph = geoGraph.graph();
        QueryBuilder iqb = graph.graql().infer(true);
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y has name 'Poland';";

        String queryString2 = "match (geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y has name 'Europe';";

        Concept poland = getConcept(graph, "name", "Poland");
        Concept europe = getConcept(graph, "name", "Europe");

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        answers.forEach(ans -> assertEquals(ans.size(), 2));
        answers.forEach(ans -> assertEquals(ans.get(VarName.of("y")).getId().getValue(), poland.getId().getValue()));
        assertEquals(answers.size(), 6);

        QueryAnswers answers2 = queryAnswers(iqb.materialise(false).parse(queryString2));
        answers2.forEach(ans -> assertEquals(ans.size(), 2));
        answers2.forEach(ans -> assertEquals(ans.get(VarName.of("y")).getId().getValue(), europe.getId().getValue()));
        assertEquals(answers2.size(), 21);
    }

    @Test
    public void testSpecificTransitiveQueryWithIds() {
        GraknGraph graph = geoGraph.graph();
        QueryBuilder iqb = graph.graql().infer(true);
        Concept poland = getConcept(graph, "name", "Poland");
        Concept europe = getConcept(graph, "name", "Europe");
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y id '" + poland.getId().getValue() + "';";

        String queryString2 = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y id '" + europe.getId().getValue() + "';";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        answers.forEach(ans -> assertEquals(ans.size(), 2));
        answers.forEach(ans -> assertEquals(ans.get(VarName.of("y")).getId().getValue(), poland.getId().getValue()));
        assertEquals(answers.size(), 6);

        QueryAnswers answers2 = queryAnswers(iqb.materialise(false).parse(queryString2));
        answers2.forEach(ans -> assertEquals(ans.size(), 2));
        answers2.forEach(ans -> assertEquals(ans.get(VarName.of("y")).getId().getValue(), europe.getId().getValue()));
        assertEquals(answers2.size(), 21);
    }

    @Test
    public void testSpecificTransitiveQuery_NoRoles() {
        GraknGraph graph = geoGraph.graph();
        QueryBuilder iqb = graph.graql().infer(true);
        Concept masovia = getConcept(graph, "name", "Masovia");
        String queryString = "match " +
                "($x, $y) isa is-located-in;" +
                "$y has name 'Masovia';";
        String queryString2 = "match " +
                "{(geo-entity: $x, entity-location: $y) isa is-located-in or " +
                "(geo-entity: $y, entity-location: $x) isa is-located-in;};" +
                "$y has name 'Masovia';";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        assertEquals(answers.size(), 5);
        answers.forEach(ans -> assertEquals(ans.size(), 2));
        answers.forEach(ans -> assertEquals(ans.get(VarName.of("y")).getId().getValue(), masovia.getId().getValue()));
        QueryAnswers answers2 = queryAnswers(iqb.materialise(false).parse(queryString2));
        assertEquals(answers.size(), answers2.size());
    }

    @Test
    public void testSpecificTransitiveQueryWithIds_NoRoles() {
        GraknGraph graph = geoGraph.graph();
        QueryBuilder iqb = graph.graql().infer(true);
        Concept masovia = getConcept(graph, "name", "Masovia");
        String queryString = "match " +
                "($x, $y) isa is-located-in;" +
                "$y id '" + masovia.getId().getValue() + "';";

        String queryString2 = "match " +
                "{(geo-entity: $x, entity-location: $y) isa is-located-in or " +
                "(geo-entity: $y, entity-location: $x) isa is-located-in;};" +
                "$y id '" + masovia.getId().getValue() + "';";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        answers.forEach(ans -> assertEquals(ans.size(), 2));
        answers.forEach(ans -> assertEquals(ans.get(VarName.of("y")).getId().getValue(), masovia.getId().getValue()));
        assertEquals(answers.size(), 5);
        QueryAnswers answers2 = queryAnswers(iqb.materialise(false).parse(queryString2));
        assertEquals(answers.size(), answers2.size());
    }

    @Test
    public void testTransitiveClosureQuery() {
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        assertEquals(answers.size(), 51);
        QueryAnswers answers2 = queryAnswers(iqb.materialise(true).parse(queryString));
        assertEquals(answers, answers2);
    }

    @Test
    public void testTransitiveClosureQuery_NoRoles() {
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match ($x, $y) isa is-located-in;";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        QueryAnswers answers2 = queryAnswers(iqb.materialise(true).parse(queryString));
        assertEquals(answers.size(), 102);
        assertEquals(answers, answers2);
    }

    @Test
    public void testTransitiveClosureQueryWithRelationVar() {
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in;";

        QueryAnswers answers = new QueryAnswers(iqb.materialise(false).<MatchQuery>parse(queryString).admin().streamWithAnswers().collect(Collectors.toSet()));
        QueryAnswers answers2 = queryAnswers(iqb.materialise(true).parse(queryString));
        assertEquals(answers.size(), 51);
        assertEquals(answers, answers2);
    }

    @Test
    public void testRelationTypeQuery() {
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $x isa is-located-in;";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        QueryAnswers answers2 = queryAnswers(iqb.materialise(true).parse(queryString));
        assertEquals(answers.size(), 51);
        assertEquals(answers, answers2);
    }

    @Test
    public void testRelationVarQuery_WithAndWithoutRelationPlayers() {
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $x isa is-located-in;";
        String queryString2 = "match $x ($x1, $x2)isa is-located-in;select $x;";

        QueryAnswers answers = queryAnswers(iqb.materialise(true).parse(queryString));
        QueryAnswers answers2 = queryAnswers(iqb.materialise(true).parse(queryString2));
        assertEquals(answers.size(), 51);
        assertEquals(answers, answers2);
    }

    @Test
    public void testLazy() {
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; limit 1;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y) isa is-located-in; limit 22;";
        String queryString3 = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        QueryAnswers answers2 = queryAnswers(iqb.materialise(false).parse(queryString2));
        QueryAnswers answers3 = queryAnswers(iqb.materialise(false).parse(queryString3));
        assertTrue(answers3.containsAll(answers));
        assertTrue(answers3.containsAll(answers2));
    }

    private Concept getConcept(GraknGraph graph, String typeName, Object val){
        return graph.graql().match(Graql.var("x").has(typeName, val).admin()).execute().iterator().next().get("x");
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().streamWithVarNames().map(QueryAnswer::new).collect(toSet()));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        QueryAnswers answers = queryAnswers(q1);
        QueryAnswers answers2 = queryAnswers(q2);
        assertEquals(answers, answers2);
    }
}
