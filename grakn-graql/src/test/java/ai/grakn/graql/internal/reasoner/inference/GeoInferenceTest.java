/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.inference;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.kbs.GeoKB;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.rule.SampleKBContext;
import java.util.List;

import ai.grakn.util.GraknTestUtil;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.hamcrest.Matchers.empty;

public class GeoInferenceTest {

    @Rule
    public final SampleKBContext geoKB = GeoKB.context();

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Test
    public void testEntitiesLocatedInThemselves(){
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match (geo-entity: $x, entity-location: $x) isa is-located-in; get;";

        GetQuery query = iqb.materialise(false).parse(queryString);
        List<Answer> answers = query.execute();
        assertThat(answers, empty());
    }

    @Test
    public void testTransitiveQuery_withGuards() {
        QueryBuilder qb = geoKB.tx().graql().infer(false);
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa university;$x has name $name;"+
                "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland';" +
                "get $x, $name;";
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';}; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransitiveQuery_withGuards_noRoles() {
        QueryBuilder qb = geoKB.tx().graql().infer(false);
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match " +
                "$z1 isa university;$z1 has name $name;"+
                "($z1, $z2) isa is-located-in;" +
                "$z2 isa country;$z2 has name 'Poland';" +
                "get $z1, $name;";
        String queryString2 = "match " +
                "$z2 isa university;$z2 has name $name;"+
                "($z1, $z2) isa is-located-in;" +
                "$z1 isa country;$z1 has name 'Poland';" +
                "get $z2, $name;";
        String explicitQuery = "match " +
                "$z1 isa university;$z1 has name $name;" +
                "{$z1 has name 'University-of-Warsaw';} or {$z1 has name'Warsaw-Polytechnics';}; get;";
        String explicitQuery2 = "match " +
                "$z2 isa university;$z2 has name $name;" +
                "{$z2 has name 'University-of-Warsaw';} or {$z2 has name'Warsaw-Polytechnics';}; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(false).parse(queryString2), qb.parse(explicitQuery2));
        assertQueriesEqual(iqb.materialise(true).parse(queryString2), qb.parse(explicitQuery2));
    }

    @Test
    public void testTransitiveQuery_withSpecificResource() {
        GraknTx graph = geoKB.tx();
        QueryBuilder iqb = graph.graql().infer(true);
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y has name 'Poland'; get;";

        String queryString2 = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y has name 'Europe'; get;";

        Concept poland = getConcept(graph, "name", "Poland");
        Concept europe = getConcept(graph, "name", "Europe");

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        answers.forEach(ans -> assertEquals(ans.size(), 2));
        answers.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), poland.getId().getValue()));
        assertEquals(answers.size(), 6);

        List<Answer> answers2 = iqb.materialise(false).<GetQuery>parse(queryString2).execute();
        answers2.forEach(ans -> assertEquals(ans.size(), 2));
        answers2.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), europe.getId().getValue()));
        assertEquals(answers2.size(), 21);
    }

    @Test
    public void testTransitiveQuery_withSpecificResource_noRoles() {
        GraknTx graph = geoKB.tx();
        QueryBuilder iqb = graph.graql().infer(true);
        Concept masovia = getConcept(graph, "name", "Masovia");
        String queryString = "match " +
                "($x, $y) isa is-located-in;" +
                "$y has name 'Masovia'; get;";
        String queryString2 = "match " +
                "{(geo-entity: $x, entity-location: $y) isa is-located-in or " +
                "(geo-entity: $y, entity-location: $x) isa is-located-in;};" +
                "$y has name 'Masovia'; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();

        answers.forEach(ans -> assertEquals(ans.size(), 2));
        answers.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), masovia.getId().getValue()));
        assertEquals(answers.size(), 5);
        List<Answer> answers2 = iqb.materialise(false).<GetQuery>parse(queryString2).execute();
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_withSubstitution() {
        GraknTx graph = geoKB.tx();
        QueryBuilder iqb = graph.graql().infer(true);
        Concept poland = getConcept(graph, "name", "Poland");
        Concept europe = getConcept(graph, "name", "Europe");
        String queryString = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y id '" + poland.getId().getValue() + "'; get;";

        String queryString2 = "match " +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "$y id '" + europe.getId().getValue() + "'; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        answers.forEach(ans -> assertEquals(ans.size(), 2));
        answers.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), poland.getId().getValue()));
        assertEquals(answers.size(), 6);


        List<Answer> answers2 = iqb.materialise(false).<GetQuery>parse(queryString2).execute();
        answers2.forEach(ans -> assertEquals(ans.size(), 2));
        answers2.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), europe.getId().getValue()));
        assertEquals(answers2.size(), 21);
    }

    @Test
    public void testTransitiveQuery_withSubstitution_noRoles() {
        GraknTx graph = geoKB.tx();
        QueryBuilder iqb = graph.graql().infer(true);
        Concept masovia = getConcept(graph, "name", "Masovia");
        String queryString = "match " +
                "($x, $y) isa is-located-in;" +
                "$y id '" + masovia.getId().getValue() + "'; get;";

        String queryString2 = "match " +
                "{(geo-entity: $x, entity-location: $y) isa is-located-in or " +
                "(geo-entity: $y, entity-location: $x) isa is-located-in;};" +
                "$y id '" + masovia.getId().getValue() + "'; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        answers.forEach(ans -> assertEquals(ans.size(), 2));
        answers.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), masovia.getId().getValue()));
        assertEquals(answers.size(), 5);
        List<Answer> answers2 = iqb.materialise(false).<GetQuery>parse(queryString2).execute();
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_withSubstitution_variableRoles() {
        GraknTx graph = geoKB.tx();
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        Concept masovia = getConcept(graph, "name", "Masovia");
        String queryString = "match " +
                "($r1: $x, $r2: $y) isa is-located-in;" +
                "$y id '" + masovia.getId().getValue() + "'; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();

        answers.forEach(ans -> assertEquals(ans.size(), 4));
        answers.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), masovia.getId().getValue()));
        answers2.forEach(ans -> assertEquals(ans.size(), 4));
        answers2.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), masovia.getId().getValue()));
        assertEquals(answers.size(), 20);
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_Closure() {
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 51);
        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_Closure_NoRoles() {
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa is-located-in; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 102);
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_Closure_NoRoles_withSubstitution() {
        GraknTx graph = geoKB.tx();
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        Concept masovia = getConcept(graph, "name", "Masovia");
        String queryString = "match " +
                "($x, $y) isa is-located-in;" +
                "$y id '" + masovia.getId().getValue() + "'; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();

        answers.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), masovia.getId().getValue()));
        assertEquals(answers.size(), 5);
        answers2.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), masovia.getId().getValue()));
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_Closure_variableRoles() {
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match ($r1: $x, $r2: $y) isa is-located-in; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        answers.forEach(ans -> assertEquals(ans.size(), 4));
        assertEquals(answers.size(), 408);

        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();
        answers2.forEach(ans -> assertEquals(ans.size(), 4));
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_variableRoles_withSubstitution_withRelationVar() {
        GraknTx graph = geoKB.tx();
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        Concept masovia = getConcept(graph, "name", "Masovia");
        String queryString = "match " +
                "$x ($r1: $x1, $r2: $x2) isa is-located-in;" +
                "$x2 id '" + masovia.getId().getValue() + "'; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 20);
        answers.forEach(ans -> assertEquals(ans.size(), 5));

        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();
        answers2.forEach(ans -> assertEquals(ans.size(), 5));
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_Closure_variableSpecificRoles() {
        QueryBuilder iqb = geoKB.tx().graql().infer(true);

        VarPattern rolePattern = var()
                .rel(var("r1").label("geo-entity"), var("x"))
                .rel(var("r2").label("entity-location"), var("y"));

        List<Answer> answers = iqb.match(rolePattern).get().execute();
        List<Answer> answers2 = iqb.materialise(true).match(rolePattern).get().execute();

        answers.forEach(ans -> assertEquals(ans.size(), 4));
        assertEquals(answers.size(), 51);
        answers2.forEach(ans -> assertEquals(ans.size(), 4));
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_Closure_singleVariableRole() {
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match ($x, $r2: $y) isa is-located-in; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();

        answers.forEach(ans -> assertEquals(ans.size(), 3));
        assertEquals(answers.size(), 204);
        answers2.forEach(ans -> assertEquals(ans.size(), 3));
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_Closure_singleVariableRole_withSubstitution() {
        GraknTx graph = geoKB.tx();
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        Concept masovia = getConcept(graph, "name", "Masovia");
        String queryString = "match " +
                "($x, $r2: $y) isa is-located-in;" +
                "$y id '" + masovia.getId().getValue() + "'; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();

        answers.forEach(ans -> assertEquals(ans.size(), 3));
        answers.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), masovia.getId().getValue()));
        assertEquals(answers.size(), 10);

        answers2.forEach(ans -> assertEquals(ans.size(), 3));
        answers2.forEach(ans -> assertEquals(ans.get(var("y")).getId().getValue(), masovia.getId().getValue()));
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testTransitiveQuery_Closure_withRelationVar() {
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 51);
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testRelationVarQuery_Closure_withAndWithoutRelationPlayers() {
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match $x isa is-located-in; get;";
        String queryString2 = "match $x ($x1, $x2) isa is-located-in;get $x;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.materialise(true).<GetQuery>parse(queryString).execute();
        List<Answer> answers3 = iqb.materialise(false).<GetQuery>parse(queryString2).execute();
        List<Answer> answers4 = iqb.materialise(true).<GetQuery>parse(queryString2).execute();
        assertCollectionsEqual(answers, answers2);
        assertCollectionsEqual(answers3, answers4);
        assertEquals(answers.size(), 51);
        assertEquals(answers3.size(), 51);
    }

    @Test
    public void testLazy() {
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; limit 1; get;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y) isa is-located-in; limit 22; get;";
        String queryString3 = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.materialise(false).<GetQuery>parse(queryString2).execute();
        List<Answer> answers3 = iqb.materialise(false).<GetQuery>parse(queryString3).execute();
        assertTrue(answers3.containsAll(answers));
        assertTrue(answers3.containsAll(answers2));
    }

    private Concept getConcept(GraknTx graph, String typeName, Object val){
        return graph.graql().match(Graql.var("x").has(typeName, val).admin()).get("x").findAny().orElse(null);
    }
}
