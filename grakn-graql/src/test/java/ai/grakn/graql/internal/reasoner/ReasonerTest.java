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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.Rule;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.kbs.GeoKB;
import ai.grakn.test.kbs.SNBKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.Schema;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Suite of tests focused on reasoning expressivity and various edge cases.
 */
public class ReasonerTest {

    @ClassRule
    public static final SampleKBContext testGeoKB = GeoKB.context();

    @ClassRule
    public static final SampleKBContext geoKB = GeoKB.context();

    @ClassRule
    public static final SampleKBContext nonMaterialisedGeoKB = GeoKB.context();

    @ClassRule
    public static final SampleKBContext snbKB = SNBKB.context();

    @ClassRule
    public static final SampleKBContext nonMaterialisedSnbKB = SNBKB.context();
    

    @Test
    public void testTwoRulesOnlyDifferingByVarNamesAreEquivalent() {
        EmbeddedGraknTx<?> tx = testGeoKB.tx();

        Rule rule1 = tx.getRule("Geo Rule");

        Pattern body2 = Graql.and(tx.graql().parser().parsePatterns("(geo-entity: $l1, entity-location: $l2) isa is-located-in;" +
                "(geo-entity: $l2, entity-location: $l3) isa is-located-in;"));
        Pattern head2 = Graql.and(tx.graql().parser().parsePatterns("(geo-entity: $l1, entity-location: $l3) isa is-located-in;"));
        Rule rule2 = tx.putRule("Rule 2", body2, head2);

        InferenceRule R1 = new InferenceRule(rule1, tx);
        InferenceRule R2 = new InferenceRule(rule2, tx);
        assertEquals(R1, R2);
    }

    @Test
    public void testParsingQueryWithComma(){
        String queryString = "match $x isa person, has firstname 'Bob', has name 'Bob', 'Bob', has age <21; get;";
        String queryString2 = "match $x isa person; $x has firstname 'Bob';$x has name 'Bob';$x 'Bob';$x has age <21; get;";
        QueryBuilder iqb = snbKB.tx().graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testParsingQueryWithResourceVariable(){
        String patternString = "{$x isa person, has firstname $y;}";
        String patternString2 = "{$x isa person;$x has firstname $y;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, snbKB.tx()), snbKB.tx());
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, snbKB.tx()), snbKB.tx());
        assertEquals(query, query2);
    }

    @Test
    public void testParsingQueryWithResourceVariable_BoundWithNonSpecificVP(){
        String patternString = "{$x isa person;$x has age < 10;}";
        String patternString2 = "{$x isa person;$x has age $y;$y < 10;}";
        ReasonerQueryImpl query = ReasonerQueries.atomic(conjunction(patternString, snbKB.tx()), snbKB.tx());
        ReasonerQueryImpl query2 = ReasonerQueries.atomic(conjunction(patternString2, snbKB.tx()), snbKB.tx());
        assertEquals(query, query2);
    }

    @Test
    public void testParsingQueryWithResourceVariable_BoundWithSpecificVP(){
        EmbeddedGraknTx<?> graph = snbKB.tx();
        String patternString = "{$x has firstname 'Bob', has lastname 'Geldof';}";
        String patternString2 = "{$x has firstname 'Bob';$x has lastname 'Geldof';}";
        String patternString3 = "{$x has firstname $x1;$x has lastname $x2;$x1 == 'Bob';$x2 == 'Geldof';}";
        String patternString4 = "{$x has firstname $x2;$x has lastname $x1;$x2 == 'Bob';$x1 == 'Geldof';}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        ReasonerQueryImpl query3 = ReasonerQueries.create(conjunction(patternString3, graph), graph);
        ReasonerQueryImpl query4 = ReasonerQueries.create(conjunction(patternString4, graph), graph);

        assertEquals(query, query3);
        assertEquals(query, query4);
        assertEquals(query2, query3);
        assertEquals(query2, query4);
    }

    @Test
    public void testParsingQueryContainingIsAbstract(){
        String queryString = "match $x is-abstract; get;";
        GetQuery query = snbKB.tx().graql().infer(true).parse(queryString);
        GetQuery query2 = snbKB.tx().graql().infer(false).parse(queryString);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testParsingQueryContainingTypeRegex(){
        String queryString = " match $x sub " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + ", regex /name/; get;";
        GetQuery query = snbKB.tx().graql().infer(true).parse(queryString);
        GetQuery query2 = snbKB.tx().graql().infer(false).parse(queryString);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testParsingQueryContainingDataType(){
        String queryString = " match $x sub " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + ", datatype string; get;";
        GetQuery query = snbKB.tx().graql().infer(true).parse(queryString);
        GetQuery query2 = snbKB.tx().graql().infer(false).parse(queryString);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryWithNoRelationType(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $x isa city;$y isa country;($x, $y);$y has name 'Poland';$x has name $name; get;";
        String queryString2 = "match $x isa city;$y isa country;$y has name 'Poland';$x has name $name;" +
                "($x, $y) isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryWithNoRelationTypeWithRoles(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);$y has name 'Poland'; get;";
        String queryString2 = "match $x isa city;$y isa country;" +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;$y has name 'Poland'; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryWithNoRelationTypeWithRoles2(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y); get;";
        String queryString2 = "match $x isa city;$y isa country;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVar(){
        String thing = Schema.MetaSchema.THING.getLabel().getValue();
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $x isa $type;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland'; get;";
        String explicitQuery = "match $y has name 'Poland';$x isa $type;$x has " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + " $name;" +
                "{" +
                "{$name == 'Warsaw-Polytechnics' or $name == 'University-of-Warsaw';};" +
                "{$type label 'university' or $type label 'entity' or $type label '" + thing + "';};" +
                "} or {" +
                "{$name == 'Warsaw' or $name == 'Wroclaw';};" +
                "{$type label 'city' or $type label 'geoObject' or $type label 'entity' or $type label '" + thing + "';};" +
                "} or {" +
                "{$name == 'Masovia' or $name == 'Silesia';};" +
                "{$type label 'region' or $type label 'geoObject' or $type label 'entity' or $type label '" + thing + "';};" +
                "}; get $x, $y, $type;";
        GetQuery query = graph.graql().infer(true).parse(queryString);
        GetQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVar2(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $x isa $type;$type label 'university';" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland'; get;";
        String explicitQuery = "match $y has name 'Poland';" +
                "{$x isa $type;$type label 'university';$x has name 'Warsaw-Polytechnics';} or" +
                "{$x isa $type;$type label 'university';$x has name 'University-of-Warsaw';}; get;";
        GetQuery query = graph.graql().infer(true).parse(queryString);
        GetQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingSub(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match " +
                "$x isa $type;$type sub geoObject;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;" +
                "$y has name 'Poland';" +
                "$x has name $name; get;";
        String queryString2 = "match " +
                "$x isa $type;{$type label 'region';} or {$type label 'city';} or {$type label 'geoObject';};" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;$y isa country;" +
                "$y has name 'Poland';" +
                "$x has name $name; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingSub2(){
        String queryString = "match $x isa person;$y isa $type;$type sub recommendable;($x, $y) isa recommendation; get;";
        String explicitQuery = "match $x isa person, has name $xName;$y isa $type;$y has name $yName;" +
                "{$type label 'recommendable' or $type label 'product' or $type label 'tag';};" +
                "{$xName == 'Alice';$yName == 'War of the Worlds';} or" +
                "{$xName == 'Bob';{$yName == 'Ducatti 1299';} or {$yName == 'The Good the Bad the Ugly';};} or" +
                "{$xName == 'Charlie';{$yName == 'Blizzard of Ozz';} or {$yName == 'Stratocaster';};} or " +
                "{$xName == 'Denis';{$yName == 'Colour of Magic';} or {$yName == 'Dorian Gray';};} or"+
                "{$xName == 'Frank';$yName == 'Nocturnes';} or" +
                "{$xName == 'Karl Fischer';{$yName == 'Faust';} or {$yName == 'Nocturnes';};} or " +
                "{$xName == 'Gary';$yName == 'The Wall';} or" +
                "{$xName == 'Charlie';" +
                "{$yName == 'Yngwie Malmsteen';} or {$yName == 'Cacophony';} or {$yName == 'Steve Vai';} or {$yName == 'Black Sabbath';};} or " +
                "{$xName == 'Gary';$yName == 'Pink Floyd';};get $x, $y, $type;";
        GetQuery query = snbKB.tx().graql().infer(true).parse(queryString);
        GetQuery query2 = snbKB.tx().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTautology(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match ($x, $y) isa is-located-in;city sub geoObject; get;";
        String queryString2 = "match ($x, $y) isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingContradiction(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        //geoObject sub city always returns an empty set
        String queryString = "match ($x, $y) isa is-located-in;geoObject sub city; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
        assertThat(answers, empty());
    }

    @Test
    public void testReasoningWithQueryContainingPlays(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match " +
                "$x isa $type;$type plays geo-entity;"+
                "$y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in; get;";
        String explicitQuery = "match $y has name 'Poland';$x isa $type;$x has " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + " $name;" +
                "{" +
                "{$name == 'Europe';};" +
                "{$type label 'continent' or $type label 'geoObject';};" +
                "} or {" +
                "{$name == 'Warsaw-Polytechnics' or $name == 'University-of-Warsaw';};" +
                "{$type label 'university';};" +
                "} or {" +
                "{$name == 'Warsaw' or $name == 'Wroclaw';};" +
                "{$type label 'city' or $type label 'geoObject';};" +
                "} or {" +
                "{$name == 'Masovia' or $name == 'Silesia';};" +
                "{$type label 'region' or $type label 'geoObject';};" +
                "}; get $x, $y, $type;";
        GetQuery query = graph.graql().infer(true).parse(queryString);
        GetQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingPlays2(){
        String queryString = "match $x isa person;$y isa $type;$type plays recommended-product;($x, $y) isa recommendation; get;";
        String queryString2 = "match $x isa person;$y isa $type;{$type label 'product';} or {$type label 'tag';};($x, $y) isa recommendation; get;";
        QueryBuilder iqb = snbKB.tx().graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeHas(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $x isa $type;$type has name;$y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;get $x, $y;";
        String queryString2 = "match $y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeHas2(){
        String queryString = "match $x isa $type;$type has name;$y isa product;($x, $y) isa recommendation; get;";
        String explicitQuery = "match $x isa person, has name $xName;$x isa $type;$y has name $yName;" +
                "{$type label 'person' or $type label 'entity2';};" +
                "{$xName == 'Alice';$yName == 'War of the Worlds';} or" +
                "{$xName == 'Bob';{$yName == 'Ducatti 1299';} or {$yName == 'The Good the Bad the Ugly';};} or" +
                "{$xName == 'Charlie';{$yName == 'Blizzard of Ozz';} or {$yName == 'Stratocaster';};} or " +
                "{$xName == 'Denis';{$yName == 'Colour of Magic';} or {$yName == 'Dorian Gray';};} or"+
                "{$xName == 'Frank';$yName == 'Nocturnes';} or" +
                "{$xName == 'Karl Fischer';{$yName == 'Faust';} or {$yName == 'Nocturnes';};} or " +
                "{$xName == 'Gary';$yName == 'The Wall';};get $x, $y, $type;";
        GetQuery query = snbKB.tx().graql().infer(true).parse(queryString);
        GetQuery query2 = snbKB.tx().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingRegex(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name /.*(.*)land(.*).*/;($x, $y) isa is-located-in;get $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingContains(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name contains 'land';($x, $y) isa is-located-in;get $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingIndirectRelation(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match ($x, $y) isa $rel;$rel label is-located-in;get $x, $y;";
        String queryString2 = "match ($x, $y) isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVariable(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $x isa $type;$type label 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;get $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVariable2(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match $x isa $type;$type label 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;$y has name 'Poland';get $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in;$y has name 'Poland'; $y isa country; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingRelationTypeVar(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match (geo-entity: $x) isa $type;$type label 'is-located-in'; get $x;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in; get $x;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingUnspecifiedCastings(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match (geo-entity: $x) isa is-located-in; get;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;get $x;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingUnspecifiedCastings2(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match (geo-entity: $x); get;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;get $x;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingLimit(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String limitQueryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in;limit 5; get;";
        String queryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery limitQuery = iqb.parse(limitQueryString);
        GetQuery query = iqb.parse(queryString);

        List<ConceptMap> limitedAnswers = limitQuery.execute();
        List<ConceptMap> answers = query.execute();
        assertEquals(limitedAnswers.size(), 5);
        assertTrue(answers.size() > limitedAnswers.size());
        assertTrue(answers.containsAll(limitedAnswers));
    }

    @Test
    public void testReasoningWithQueryContainingOrder(){
        String queryString = "match $p isa person, has age $a;$pr isa product;($p, $pr) isa recommendation;order by $a; get;";
        GetQuery query = snbKB.tx().graql().infer(true).parse(queryString);

        List<ConceptMap> answers = query.execute();
        assertEquals(answers.iterator().next().get("a").asAttribute().value().toString(), "19");
    }

    @Test
    public void testReasoningWithQueryContainingOrderAndOffset(){
        String queryString = "match $p isa person, has age $a, has name $n;$pr isa product;($p, $pr) isa recommendation; get;";
        GetQuery query = nonMaterialisedSnbKB.tx().graql().infer(true).parse(queryString);

        final int offset = 4;
        List<ConceptMap> fullAnswers = query.execute();
        List<ConceptMap> answers = query.match().orderBy(Graql.var("a")).get().execute();
        List<ConceptMap> answers2 = query.match().orderBy(Graql.var("a")).offset(offset).get().execute();

        assertEquals(fullAnswers.size(), answers2.size() + offset);
        assertEquals(answers.size(), answers2.size() + offset);
        assertEquals(answers.iterator().next().get("a").asAttribute().value().toString(), "19");
        assertEquals(answers2.iterator().next().get("a").asAttribute().value().toString(), "23");
    }

    @Test
    public void testReasoningWithQueryContainingResourceComparison(){
        //recommendations of products for people older than Denis - Frank, Karl and Gary
        String queryString = "match " +
                "$b has name 'Denis', has age $x;" +
                "$p has name $name, has age $y; $y > $x;"+
                "$pr isa product;($p, $pr) isa recommendation;" +
                "get $p, $y, $pr, $name;";
        String explicitQuery = "match $p isa person, has age $y, has name $name;$pr isa product, has name $yName;" +
                "{$name == 'Frank';$yName == 'Nocturnes';} or" +
                "{$name == 'Karl Fischer';{$yName == 'Faust';} or {$yName == 'Nocturnes';};} or " +
                "{$name == 'Gary';$yName == 'The Wall';};get $p, $pr, $y, $name;";
        GetQuery query = snbKB.tx().graql().infer(true).parse(queryString);
        GetQuery query2 = snbKB.tx().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingResourceComparison2(){
        String queryString = "match " +
                "$p has name $name, has age $x;$x < $y;" +
                "$p2 has name 'Denis', has age $y;" +
                "$t isa tag;($p, $t) isa recommendation; get $p, $name, $x, $t;";
        String explicitQuery = "match " +
                "$p isa person, has age $x, has name $name;$t isa tag, has name $yName;" +
                "{$name == 'Charlie';" +
                "{$yName == 'Yngwie Malmsteen';} or {$yName == 'Cacophony';} or" +
                "{$yName == 'Steve Vai';} or {$yName == 'Black Sabbath';};};get $p, $name, $x, $t;";
        GetQuery query = snbKB.tx().graql().infer(true).parse(queryString);
        GetQuery query2 = snbKB.tx().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingAmbiguousRolePlayers(){
        GraknTx graph = nonMaterialisedGeoKB.tx();
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";
        String queryString2 = "match ($x, $y) isa is-located-in; get;";
        QueryBuilder iqb = graph.graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        List<ConceptMap> answers = query.execute();
        List<ConceptMap> answers2 = query2.execute();
        assertTrue(answers2.containsAll(answers));
        assertEquals(2*answers.size(), answers2.size());
    }

    @Test
    public void testReasoningWithQueryContainingRelationVariable(){
        String queryString = "match $x isa is-located-in; get;";
        String queryString2 = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in; get $x;";
        String queryString3 = "match $x ($x1, $x2) isa is-located-in; get $x;";
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        GetQuery query3 = iqb.parse(queryString3);

        assertQueriesEqual(query, query2);
        assertQueriesEqual(query2, query3);
    }

    @Test
    public void testReasoningWithQueryContainingRelationVariableWithMaterialisation_requeryingDoesntCreateDuplicates(){
        QueryBuilder iqb = geoKB.tx().graql().infer(true);
        String queryString = "match $x isa is-located-in; get;";
        String queryString2 = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in; get $x;";
        String queryString3 = "match $x ($x1, $x2) isa is-located-in; get $x;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        GetQuery query3 = iqb.parse(queryString3);

        List<ConceptMap> answers = query.execute();
        List<ConceptMap> answers2 = query2.execute();
        List<ConceptMap> answers3 = query3.execute();

        assertCollectionsEqual(answers, answers2);
        assertCollectionsEqual(answers2, answers3);

        List<ConceptMap> requeriedAnswers = query.execute();
        List<ConceptMap> requeriedAnswers2 = query2.execute();
        List<ConceptMap> requeriedAnswers3 = query3.execute();

        assertCollectionsEqual(answers, requeriedAnswers);
        assertCollectionsEqual(answers2, requeriedAnswers2);
        assertCollectionsEqual(answers3, requeriedAnswers3);

        assertCollectionsEqual(requeriedAnswers, requeriedAnswers2);
        assertCollectionsEqual(requeriedAnswers2, requeriedAnswers3);
    }

    @Test
    public void testReasoningWithMatchAllQuery(){
        String queryString = "match $y isa product;$r($x, $y);$x isa entity2; get;";
        String queryString2 = "match $y isa product;$x isa entity2;{" +
                "$r($x, $y) isa recommendation or " +
                "$r($x, $y) isa typing or " +
                "$r($x, $y) isa made-in;}; get;";
        QueryBuilder iqb = snbKB.tx().graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingHas(){
        String queryString = "match $x isa person has name $y; get;";
        String queryString2 = "match $x isa person has " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + " $y; $y isa name; get;";
        QueryBuilder iqb = snbKB.tx().graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingMultiPredResource(){
        String queryString = "match $p isa person, has age $a;$a > 23; $a < 27;$pr isa product;" +
                "($p, $pr) isa recommendation; get $p, $pr;";
        String queryString2 = "match $p isa person, has age > 23, has age < 27;$pr isa product;" +
                "($p, $pr) isa recommendation; get;";
        QueryBuilder iqb = snbKB.tx().graql().infer(true);
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}











