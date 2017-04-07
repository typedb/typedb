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
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graphs.SNBGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.test.GraphContext;
import com.google.common.collect.Sets;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Suite of tests focused on reasoning expressivity and various edge cases.
 */
public class ReasonerTest {

    @ClassRule
    public static final GraphContext snbGraph = GraphContext.preLoad(SNBGraph.get());

    @ClassRule
    public static final GraphContext snbGraph2 = GraphContext.preLoad(SNBGraph.get());

    @ClassRule
    public static final GraphContext snbGraph3 = GraphContext.preLoad(SNBGraph.get());

    @ClassRule
    public static final GraphContext testGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext nonMaterialisedGeoGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext nonMaterialisedsnbGraph = GraphContext.preLoad(SNBGraph.get());

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext geoGraph2 = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext geoGraph3 = GraphContext.preLoad(GeoGraph.get());

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void testSubPropertyRuleCreation() {
        GraknGraph graph = snbGraph.graph();
        Map<TypeLabel, TypeLabel> roleMap = new HashMap<>();
        RelationType parent = graph.getRelationType("sublocate");
        RelationType child = graph.getRelationType("resides");

        roleMap.put(graph.getRoleType("member-location").getLabel(), graph.getRoleType("subject-location").getLabel());
        roleMap.put(graph.getRoleType("container-location").getLabel(), graph.getRoleType("located-subject").getLabel());

        Pattern body = and(graph.graql().parsePatterns("(subject-location: $x, located-subject: $x1) isa resides;"));
        Pattern head = and(graph.graql().parsePatterns("(member-location: $x, container-location: $x1) isa sublocate;"));

        InferenceRule R2 = new InferenceRule(graph.admin().getMetaRuleInference().putRule(body, head), graph);
        Rule rule = Utility.createSubPropertyRule(parent, child, roleMap, graph);
        InferenceRule R = new InferenceRule(rule, graph);

        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testTransitiveRuleCreation() {
        GraknGraph graph = snbGraph.graph();
        Rule rule = Utility.createTransitiveRule(
                graph.getRelationType("sublocate"),
                graph.getRoleType("member-location").getLabel(),
                graph.getRoleType("container-location").getLabel(),
                graph);
        InferenceRule R = new InferenceRule(rule, graph);

        Pattern body = and(graph.graql().parsePatterns(
                "(member-location: $x, container-location: $z) isa sublocate;" +
                "(member-location: $z, container-location: $y) isa sublocate;"));
        Pattern head = and(graph.graql().parsePatterns("(member-location: $x, container-location: $y) isa sublocate;"));

        InferenceRule R2 = new InferenceRule(graph.admin().getMetaRuleInference().putRule(body, head), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testReflexiveRuleCreation() {
        GraknGraph graph = snbGraph.graph();
        Rule rule = Utility.createReflexiveRule(
                graph.getRelationType("knows"),
                graph.getRoleType("acquaintance1").getLabel(),
                graph.getRoleType("acquaintance2").getLabel(),
                graph);
        InferenceRule R = new InferenceRule(rule, graph);

        Pattern body = and(graph.graql().parsePatterns("(acquaintance1: $x, acquaintance2: $y) isa knows;"));
        Pattern head = and(graph.graql().parsePatterns("(acquaintance1: $x, acquaintance2: $x) isa knows;"));

        InferenceRule R2 = new InferenceRule(graph.admin().getMetaRuleInference().putRule(body, head), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testPropertyChainRuleCreation() {
        GraknGraph graph = snbGraph.graph();
        RelationType resides = graph.getRelationType("resides");
        RelationType sublocate = graph.getRelationType("sublocate");

        LinkedHashMap<RelationType, Pair<TypeLabel, TypeLabel>> chain = new LinkedHashMap<>();

        chain.put(resides, new Pair<>(graph.getRoleType("located-subject").getLabel(), graph.getRoleType("subject-location").getLabel()));
        chain.put(sublocate, new Pair<>(graph.getRoleType("member-location").getLabel(), graph.getRoleType("container-location").getLabel()));

        Rule rule = Utility.createPropertyChainRule(
                resides,
                graph.getRoleType("located-subject").getLabel(),
                graph.getRoleType("subject-location").getLabel(),
                chain,
                graph);
        InferenceRule R = new InferenceRule(rule, graph);

        Pattern body = and(graph.graql().parsePatterns(
                "(located-subject: $x, subject-location: $y) isa resides;" +
                "(member-location: $z, container-location: $y) isa sublocate;"));
        Pattern head = and(graph.graql().parsePatterns("(located-subject: $x, subject-location: $z) isa resides;"));

        InferenceRule R2 = new InferenceRule(graph.admin().getMetaRuleInference().putRule(body, head), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testAddingRuleWithHeadWithoutRoleTypesNotAllowed() {
        GraknGraph graph = testGraph.graph();
        Pattern body = Graql.and(graph.graql().parsePatterns(
                        "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                        "(geo-entity: $y, entity-location: $z) isa is-located-in;"));
        Pattern head = Graql.and(graph.graql().parsePatterns("($x, $z) isa is-located-in;"));
        exception.expect(IllegalArgumentException.class);
        Rule rule = graph.admin().getMetaRuleInference().putRule(body, head);
        InferenceRule irule = new InferenceRule(graph.admin().getMetaRuleInference().putRule(body, head), graph);
    }

    @Test
    public void testTwoRulesOnlyDifferingByVarNamesAreEquivalent() {
        GraknGraph graph = testGraph.graph();
        RuleType inferenceRule = graph.admin().getMetaRuleInference();

        Pattern body1 = Graql.and(graph.graql().parsePatterns(
                        "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                        "(geo-entity: $y, entity-location: $z) isa is-located-in;"));
        Pattern head1 = Graql.and(graph.graql().parsePatterns("(geo-entity: $x, entity-location: $z) isa is-located-in;"));
        Rule rule1 = inferenceRule.putRule(body1, head1);

        Pattern body2 = Graql.and(graph.graql().parsePatterns(
                        "(geo-entity: $l1, entity-location: $l2) isa is-located-in;" +
                        "(geo-entity: $l2, entity-location: $l3) isa is-located-in;"));
        Pattern head2 = Graql.and(graph.graql().parsePatterns("(geo-entity: $l1, entity-location: $l3) isa is-located-in;"));
        Rule rule2 = inferenceRule.putRule(body2, head2);

        InferenceRule R1 = new InferenceRule(rule1, graph);
        InferenceRule R2 = new InferenceRule(rule2, graph);
        assertEquals(R1, R2);
    }

    @Test
    public void testParsingQueryWithComma(){
        String queryString = "match $x isa person, has firstname 'Bob', has name 'Bob', val 'Bob', has age <21;";
        String queryString2 = "match $x isa person; $x has firstname 'Bob';$x has name 'Bob';$x val 'Bob';$x has age <21;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertEquals(query.execute(), query2.execute());
    }

    @Test
    public void testParsingQueryWithComma2(){
        String queryString = "match $x isa person, val <21, val >18;";
        String queryString2 = "match $x isa person;$x val <21;$x val >18;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertEquals(query.execute(), query2.execute());
    }

    @Test
    public void testParsingQueryWithResourceVariable(){
        String patternString = "{$x isa person, has firstname $y;}";
        String patternString2 = "{$x isa person;$x has firstname $y;}";
        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, snbGraph.graph()), snbGraph.graph());
        assertTrue(query.isEquivalent(query2));
    }

    //TODO: problems with engine connection seem to be encountered in this test
    @Ignore
    @Test
    public void testParsingQueryWithResourceVariable2(){
        String queryString = "match $x has firstname $y;";
        Pattern body = and(snbGraph.graph().graql().parsePatterns("$x isa person;$x has name 'Bob';"));
        Pattern head = and(snbGraph.graph().graql().parsePatterns("$x has firstname 'Bob';"));
        snbGraph.graph().admin().getMetaRuleInference().putRule(body, head);

        Reasoner.commitGraph(snbGraph.graph());
        snbGraph.graph(); //Reopen transaction

        QueryBuilder qb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = qb.parse(queryString);
        QueryAnswers answers = new QueryAnswers(queryAnswers(query));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testParsingQueryWithResourceVariable3(){
        String patternString = "{$x isa person;$x has age <10;}";
        String patternString2 = "{$x isa person;$x has age $y;$y val <10;}";
        ReasonerQueryImpl query = new ReasonerAtomicQuery(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerAtomicQuery(conjunction(patternString2, snbGraph.graph()), snbGraph.graph());
        assertTrue(query.equals(query2));
    }

    @Test
    public void testParsingQueryWithResourceVariable4(){
        String patternString = "{$x has firstname 'Bob';}";
        String patternString2 = "{$x has firstname $y;$y val 'Bob';}";
        ReasonerQueryImpl query = new ReasonerAtomicQuery(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerAtomicQuery(conjunction(patternString2, snbGraph.graph()), snbGraph.graph());
        assertTrue(query.equals(query2));
    }

    @Test
    public void testParsingQueryWithResourceVariable5(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{$x has firstname 'Bob', has lastname 'Geldof';}";
        String patternString2 = "{$x has firstname 'Bob';$x has lastname 'Geldof';}";
        String patternString3 = "{$x has firstname $x1;$x has lastname $x2;$x1 val 'Bob';$x2 val 'Geldof';}";
        String patternString4 = "{$x has firstname $x2;$x has lastname $x1;$x2 val 'Bob';$x1 val 'Geldof';}";
        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, graph), graph);
        ReasonerQueryImpl query3 = new ReasonerQueryImpl(conjunction(patternString3, graph), graph);
        ReasonerQueryImpl query4 = new ReasonerQueryImpl(conjunction(patternString4, graph), graph);

        assertTrue(query.equals(query3));
        assertTrue(query.equals(query4));
        assertTrue(query2.equals(query3));
        assertTrue(query2.equals(query4));
    }

    @Test
    public void testParsingQueryContainingScope(){
        String queryString = "match $r ($p, $pr) isa recommendation;$r has-scope $s;";
        QueryAnswers answers = queryAnswers(snbGraph.graph().graql().infer(true).materialise(false).parse(queryString));
    }

    @Test
    public void testParsingQueryContainingIsAbstract(){
        String queryString = "match $x is-abstract;";
        QueryAnswers answers = queryAnswers(snbGraph.graph().graql().infer(true).materialise(false).parse(queryString));
        QueryAnswers expAnswers = queryAnswers(snbGraph.graph().graql().infer(false).parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testParsingQueryContainingTypeRegex(){
        String queryString = " match $x sub resource, regex /name/;";
        QueryAnswers answers = queryAnswers(snbGraph.graph().graql().infer(true).materialise(false).parse(queryString));
        QueryAnswers expAnswers = queryAnswers(snbGraph.graph().graql().infer(false).parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testParsingQueryContainingDataType(){
        String queryString = " match $x sub resource, datatype string;";
        QueryAnswers answers = queryAnswers(snbGraph.graph().graql().infer(true).materialise(false).parse(queryString));
        QueryAnswers expAnswers = queryAnswers(snbGraph.graph().graql().infer(false).parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testReasoningWithQueryWithNoRelationType(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $x isa city;$y isa country;($x, $y);$y has name 'Poland';$x has name $name;";
        String queryString2 = "match $x isa city;$y isa country;$y has name 'Poland';$x has name $name;" +
                "($x, $y) isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryWithNoRelationTypeWithRoles(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);$y has name 'Poland';";
        String queryString2 = "match $x isa city;$y isa country;" +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;$y has name 'Poland';";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryWithNoRelationTypeWithRoles2(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);";
        String queryString2 = "match $x isa city;$y isa country;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVar(){
        String queryString = "match $x isa person;$y isa $type;($x, $y) isa recommendation;";
        String explicitQuery = "match $y isa $type;" +
                "{$x has name 'Alice';$y has name 'War of the Worlds';} or" +
                "{$x has name 'Bob';" +
                "{$y has name 'Ducatti 1299';} or " +
                "{$y has name 'The Good the Bad the Ugly';};} or" +
                "{$x has name 'Charlie';" +
                "{$y has name 'Blizzard of Ozz';} or " +
                "{$y has name 'Stratocaster';};} or " +
                "{$x has name 'Denis';" +
                "{$y has name 'Colour of Magic';} or " +
                "{$y has name 'Dorian Gray';};} or"+
                "{$x has name 'Frank';$y has name 'Nocturnes';} or" +
                "{$x has name 'Karl Fischer';" +
                "{$y has name 'Faust';} or " +
                "{$y has name 'Nocturnes';};} or " +
                "{$x has name 'Gary';$y has name 'The Wall';} or" +
                "{$x has name 'Charlie';"+
                "{$y has name 'Yngwie Malmsteen';} or " +
                "{$y has name 'Cacophony';} or " +
                "{$y has name 'Steve Vai';} or " +
                "{$y has name 'Black Sabbath';};} or " +
                "{$x has name 'Gary';$y has name 'Pink Floyd';};";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVar2(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $x isa $type;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String explicitQuery = "match $y has name 'Poland';$x isa $type;$x has resource $name;" +
                "{" +
                "{$name val 'Warsaw-Polytechnics' or $name val 'University-of-Warsaw';};" +
                "{$type label 'university' or $type label 'entity' or $type label 'concept';};" +
                "} or {" +
                "{$name val 'Warsaw' or $name val 'Wroclaw';};" +
                "{$type label 'city' or $type label 'geoObject' or $type label 'entity' or $type label 'concept';};" +
                "} or {" +
                "{$name val 'Masovia' or $name val 'Silesia';};" +
                "{$type label 'region' or $type label 'geoObject' or $type label 'entity' or $type label 'concept';};" +
                "}; select $x, $y, $type;";
        MatchQuery query = graph.graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVar3(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $x isa $type;$type label 'university';" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String explicitQuery = "match $y has name 'Poland';" +
                "{$x isa $type;$type label 'university';$x has name 'Warsaw-Polytechnics';} or" +
                "{$x isa $type;$type label 'university';$x has name 'University-of-Warsaw';};";
        MatchQuery query = graph.graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingSub(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match " +
                "$x isa $type;$type sub geoObject;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;" +
                "$y has name 'Poland';" +
                "$x has name $name;";
        String queryString2 = "match " +
                "$x isa $type;{$type label 'region';} or {$type label 'city';} or {$type label 'geoObject';};" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;$y isa country;" +
                "$y has name 'Poland';" +
                "$x has name $name;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
    }

    @Test
    public void testReasoningWithQueryContainingSub2(){
        String queryString = "match $x isa person;$y isa $type;$type sub recommendable;($x, $y) isa recommendation;";
        String explicitQuery = "match $x isa person, has name $xName;$y isa $type;$y has name $yName;" +
                "{$type label 'recommendable' or $type label 'product' or $type label 'tag';};" +
                "{$xName val 'Alice';$yName val 'War of the Worlds';} or" +
                "{$xName val 'Bob';{$yName val 'Ducatti 1299';} or {$yName val 'The Good the Bad the Ugly';};} or" +
                "{$xName val 'Charlie';{$yName val 'Blizzard of Ozz';} or {$yName val 'Stratocaster';};} or " +
                "{$xName val 'Denis';{$yName val 'Colour of Magic';} or {$yName val 'Dorian Gray';};} or"+
                "{$xName val 'Frank';$yName val 'Nocturnes';} or" +
                "{$xName val 'Karl Fischer';{$yName val 'Faust';} or {$yName val 'Nocturnes';};} or " +
                "{$xName val 'Gary';$yName val 'The Wall';} or" +
                "{$xName val 'Charlie';" +
                "{$yName val 'Yngwie Malmsteen';} or {$yName val 'Cacophony';} or {$yName val 'Steve Vai';} or {$yName val 'Black Sabbath';};} or " +
                "{$xName val 'Gary';$yName val 'Pink Floyd';};select $x, $y, $type;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTautology(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match ($x, $y) isa is-located-in;city sub geoObject;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
    }

    //TODO work on atom partitioning criteria
    @Ignore
    @Test
    public void testReasoningWithQueryContainingContradiction(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        //geoObject sub city always returns empty set
        String queryString = "match ($x, $y) isa is-located-in;geoObject sub city;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 0);
    }

    @Test
    public void testReasoningWithQueryContainingPlays(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match " +
                "$x isa $type;$type plays geo-entity;"+
                "$y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;";
        String explicitQuery = "match $y has name 'Poland';$x isa $type;$x has resource $name;" +
                "{" +
                "{$name val 'Europe';};" +
                "{$type label 'continent' or $type label 'geoObject';};" +
                "} or {" +
                "{$name val 'Warsaw-Polytechnics' or $name val 'University-of-Warsaw';};" +
                "{$type label 'university';};" +
                "} or {" +
                "{$name val 'Warsaw' or $name val 'Wroclaw';};" +
                "{$type label 'city' or $type label 'geoObject';};" +
                "} or {" +
                "{$name val 'Masovia' or $name val 'Silesia';};" +
                "{$type label 'region' or $type label 'geoObject';};" +
                "}; select $x, $y, $type;";
        MatchQuery query = graph.graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
    }

    //TODO takes ages - take a look at selection strategy
    @Ignore
    @Test
    public void testReasoningWithQueryContainingPlays2(){
        String queryString = "match $x isa person;$y isa $type;$type plays recommended-product;($x, $y) isa recommendation;";
        String queryString2 = "match $x isa person;$y isa $type;{$type label 'product';} or {$type label 'tag';};($x, $y) isa recommendation;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeHas(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $x isa $type;$type has name;$y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    //TODO takes ages - look at atom selection strategy
    @Ignore
    @Test
    public void testReasoningWithQueryContainingTypeHas2(){
        String queryString = "match $x isa $type;$type has name;$y isa product;($x, $y) isa recommendation;";
        //String queryString2 = "match $x isa $type;$y isa product;($x, $y) isa recommendation;";
        String explicitQuery = "match $x isa person, has name $xName;$x isa $type;$y has name $yName;" +
                "{$type label 'person' or $type label 'entity2';};" +
                "{$xName val 'Alice';$yName val 'War of the Worlds';} or" +
                "{$xName val 'Bob';{$yName val 'Ducatti 1299';} or {$yName val 'The Good the Bad the Ugly';};} or" +
                "{$xName val 'Charlie';{$yName val 'Blizzard of Ozz';} or {$yName val 'Stratocaster';};} or " +
                "{$xName val 'Denis';{$yName val 'Colour of Magic';} or {$yName val 'Dorian Gray';};} or"+
                "{$xName val 'Frank';$yName val 'Nocturnes';} or" +
                "{$xName val 'Karl Fischer';{$yName val 'Faust';} or {$yName val 'Nocturnes';};} or " +
                "{$xName val 'Gary';$yName val 'The Wall';};select $x, $y, $type;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingRegex(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name val  /.*(.*)land(.*).*/;($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingContains(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name val contains 'land';($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingIndirectRelation(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match ($x, $y) isa $rel;$rel label is-located-in;select $x, $y;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithRuleContainingVarContraction(){
            Utility.createReflexiveRule(
                    snbGraph.graph().getRelationType("knows"),
                    snbGraph.graph().getRoleType("acquaintance1").getLabel(),
                    snbGraph.graph().getRoleType("acquaintance2").getLabel(),
                    snbGraph.graph());
        String queryString = "match ($x, $y) isa knows;select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Ignore
    @Test
    //propagated sub [x/Bob] prevents from capturing the right inference
    public void testReasoningWithRuleContainingVarContraction2(){
            Utility.createReflexiveRule(
                    snbGraph.graph().getRelationType("knows"),
                    snbGraph.graph().getRoleType("acquaintance1").getLabel(),
                    snbGraph.graph().getRoleType("acquaintance2").getLabel(),
                    snbGraph.graph());
        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Ignore
    @Test
    //Bug with unification, perhaps should unify select vars not atom vars
    public void testReasoningWithRuleContainingVarContraction3(){
        Pattern body = snbGraph.graph().graql().parsePattern("$x isa person");
        Pattern head = snbGraph.graph().graql().parsePattern("(acquaintance1: $x, acquaintance2: $x) isa knows");
        snbGraph.graph().admin().getMetaRuleInference().putRule(body, head);

        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVariable(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $x isa $type;$type label 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingTypeVariable2(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match $x isa $type;$type label 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;$y has name 'Poland';select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in;$y has name 'Poland'; $y isa country;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingRelationTypeVar(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match (geo-entity: $x) isa $type;$type label 'is-located-in';";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers.filterVars(Sets.newHashSet(VarName.of("x"))), answers2);
    }

    @Test
    public void testReasoningWithQueryContainingRelationTypeVar2(){
        String queryString = "match $y isa product;(recommended-customer: $x, recommended-product: $y) isa $rel;";
        String queryString2 = "match $y isa product;(recommended-customer: $x, recommended-product: $y) isa $rel;$rel label recommendation;";
        QueryBuilder qb = snbGraph.graph().graql();
        QueryAnswers answers = queryAnswers(qb.infer(true).materialise(false).parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.infer(true).materialise(true).parse(queryString));
        QueryAnswers answers3 = queryAnswers(qb.infer(false).parse(queryString2));
        assertEquals(answers.size(), answers2.size());
        assertEquals(answers, answers2);
        assertEquals(answers2, answers3);
    }

    @Test
    public void testReasoningWithQueryContainingUnspecifiedCastings(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match (geo-entity: $x) isa is-located-in;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingUnspecifiedCastings2(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match (geo-entity: $x);";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingLimit(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String limitQueryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in;limit 5;";
        String queryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery limitQuery = iqb.parse(limitQueryString);
        MatchQuery query = iqb.parse(queryString);

        QueryAnswers limitedAnswers = queryAnswers(limitQuery);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(limitedAnswers.size(), 5);
        assertTrue(answers.size() > limitedAnswers.size());
        assertTrue(answers.containsAll(limitedAnswers));
    }

    @Test
    public void testReasoningWithQueryContainingOrder(){
        String queryString = "match $p isa person, has age $a;$pr isa product;($p, $pr) isa recommendation;order by $a;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);

        List<Map<String, Concept>> answers = query.execute();
        assertEquals(answers.iterator().next().get("a").asResource().getValue().toString(), "19");
    }

    @Test
    public void testReasoningWithQueryContainingOrderAndOffset(){
        String queryString = "match $p isa person, has age $a, has name $n;$pr isa product;($p, $pr) isa recommendation;";
        MatchQuery query = nonMaterialisedsnbGraph.graph().graql().infer(true).materialise(false).parse(queryString);

        final int offset = 4;
        List<Map<String, Concept>> fullAnswers = query.execute();
        List<Map<String, Concept>> answers = query.orderBy(VarName.of("a")).execute();
        List<Map<String, Concept>> answers2 = query.orderBy(VarName.of("a")).offset(offset).execute();

        assertEquals(fullAnswers.size(), answers2.size() + offset);
        assertEquals(answers.size(), answers2.size() + offset);
        assertEquals(answers.iterator().next().get("a").asResource().getValue().toString(), "19");
        assertEquals(answers2.iterator().next().get("a").asResource().getValue().toString(), "23");
    }

    @Test
    public void testReasoningWithQueryContainingRelates() {
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match ($x, $y) isa $rel-type;$rel-type relates geo-entity;" +
                "$y isa country;$y has name 'Poland';select $x;";
        String queryString2 = "match $y isa country;" +
            "($x, $y) isa is-located-in;$y has name 'Poland'; select $x;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingRelates2() {
        String queryString = "match ($x, $y) isa $rel;$rel relates $role;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString2);
        MatchQuery query2 = qb.parse(queryString);
        MatchQuery query3 = iqb.parse(queryString);
        query.execute();
        assertQueriesEqual(query2, query3);
    }

    @Test
    public void testReasoningWithQueryContainingResourceComparison(){
        //recommendations of products for people older than Denis - Frank, Karl and Gary
        String queryString = "match " +
                "$b has name 'Denis', has age $x;" +
                "$p has name $name, has age $y; $y val > $x;"+
                "$pr isa product;($p, $pr) isa recommendation;" +
                "select $p, $y, $pr, $name;";
        String explicitQuery = "match $p isa person, has age $y, has name $name;$pr isa product, has name $yName;" +
                "{$name val 'Frank';$yName val 'Nocturnes';} or" +
                "{$name val 'Karl Fischer';{$yName val 'Faust';} or {$yName val 'Nocturnes';};} or " +
                "{$name val 'Gary';$yName val 'The Wall';};select $p, $pr, $y, $name;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingResourceComparison2(){
        String queryString = "match $p has name $name, has age $x;$p2 has name 'Denis', has age $y;$x val < $y;" +
                "$t isa tag;($p, $t) isa recommendation; select $p, $name, $x, $t;";
        String explicitQuery = "match " +
                "$p isa person, has age $x, has name $name;$t isa tag, has name $yName;" +
                "{$name val 'Charlie';" +
                "{$yName val 'Yngwie Malmsteen';} or {$yName val 'Cacophony';} or" +
                "{$yName val 'Steve Vai';} or {$yName val 'Black Sabbath';};};select $p, $name, $x, $t;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingAmbiguousRolePlayers(){
        GraknGraph graph = nonMaterialisedGeoGraph.graph();
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertTrue(answers2.containsAll(answers));
        assertEquals(2*answers.size(), answers2.size());
    }

    @Test
    public void testReasoningWithQueryContainingRelationVariable(){
        String queryString = "match $x isa is-located-in;";
        String queryString2 = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in; select $x;";
        String queryString3 = "match $x ($x1, $x2) isa is-located-in; select $x;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        MatchQuery query = iqb.materialise(false).parse(queryString);
        MatchQuery query2 = iqb.materialise(false).parse(queryString2);
        MatchQuery query3 = iqb.materialise(false).parse(queryString3);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        QueryAnswers answers3 = queryAnswers(query3);
        assertEquals(answers, answers2);
        assertEquals(answers2, answers3);
    }


    @Test
    public void testReasoningWithQueryContainingRelationVariableWithMaterialisation(){
        String queryString = "match $x isa is-located-in;";
        String queryString2 = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in; select $x;";
        String queryString3 = "match $x ($x1, $x2) isa is-located-in; select $x;";
        MatchQuery query = geoGraph.graph().graql().infer(true).materialise(true).parse(queryString);
        MatchQuery query2 = geoGraph2.graph().graql().infer(true).materialise(true).parse(queryString2);
        MatchQuery query3 = geoGraph3.graph().graql().infer(true).materialise(true).parse(queryString3);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers requeriedAnswers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        QueryAnswers requeriedAnswers2 = queryAnswers(query2);
        QueryAnswers answers3 = queryAnswers(query3);
        QueryAnswers requeriedAnswers3 = queryAnswers(query3);

        assertEquals(answers.size(), answers2.size());
        assertEquals(answers2.size(), answers3.size());
        assertEquals(requeriedAnswers.size(), answers.size());
        assertEquals(requeriedAnswers.size(), requeriedAnswers2.size());
        assertEquals(requeriedAnswers2.size(), requeriedAnswers3.size());
    }

    @Test
    public void testReasoningWithQueryContainingRelationVariable2(){
        String queryString = "match $x isa recommendation;";
        String queryString2 = "match $x($x1, $x2) isa recommendation;select $x;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);

        QueryAnswers requeriedAnswers = queryAnswers(query);
        assertEquals(requeriedAnswers.size(), answers.size());
    }

    @Test
    public void testReasoningWithQueryContainingRelationVariableWithMaterialisation2(){
        String queryString = "match $x isa recommendation;";
        String queryString2 = "match $x(recommended-product: $x1, recommended-customer: $x2) isa recommendation; select $x;";
        String queryString3 = "match $x($x1, $x2) isa recommendation; select $x;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(true).parse(queryString);
        MatchQuery query2 = snbGraph2.graph().graql().infer(true).materialise(true).parse(queryString2);
        MatchQuery query3 = snbGraph3.graph().graql().infer(true).materialise(true).parse(queryString3);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers requeriedAnswers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        QueryAnswers requeriedAnswers2 = queryAnswers(query2);
        QueryAnswers answers3 = queryAnswers(query3);
        QueryAnswers requeriedAnswers3 = queryAnswers(query3);

        assertEquals(answers.size(), answers2.size());
        assertEquals(answers2.size(), answers3.size());
        assertEquals(requeriedAnswers.size(), answers.size());
        assertEquals(requeriedAnswers.size(), requeriedAnswers2.size());
        assertEquals(requeriedAnswers2.size(), requeriedAnswers3.size());
    }

    @Test
    public void testReasoningWithMatchAllQuery(){
        String queryString = "match $y isa product;$r($x, $y);$x isa entity;";
        String queryString2 = "match $y isa product;$x isa entity2;{" +
                "$r($x, $y) isa recommendation or " +
                "$r($x, $y) isa typing or " +
                "$r($x, $y) isa made-in;};";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingHas(){
        String queryString = "match $x isa person has name $y;";
        String queryString2 = "match $x isa person has resource $y; $y isa name;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testReasoningWithQueryContainingMultiPredResource(){
        String queryString = "match $p isa person, has age $a;$a val >23; $a val <27;$pr isa product;" +
                "($p, $pr) isa recommendation; select $p, $pr;";
        String queryString2 = "match $p isa person, has age >23, has age <27;$pr isa product;" +
                "($p, $pr) isa recommendation;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
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











