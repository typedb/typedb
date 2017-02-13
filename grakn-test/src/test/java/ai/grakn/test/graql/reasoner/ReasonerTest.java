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
import ai.grakn.concept.TypeName;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graphs.SNBGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.internal.reasoner.Utility;
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
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext geoGraph2 = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext geoGraph3 = GraphContext.preLoad(GeoGraph.get());

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void testSubPropertyRule() {
        Map<TypeName, TypeName> roleMap = new HashMap<>();
        RelationType parent = snbGraph.graph().getRelationType("sublocate");
        RelationType child = snbGraph.graph().getRelationType("resides");

        roleMap.put(snbGraph.graph().getRoleType("member-location").getName(), snbGraph.graph().getRoleType("subject-location").getName());
        roleMap.put(snbGraph.graph().getRoleType("container-location").getName(), snbGraph.graph().getRoleType("located-subject").getName());

        Pattern body = and(snbGraph.graph().graql().parsePatterns("(subject-location: $x, located-subject: $x1) isa resides;"));
        Pattern head = and(snbGraph.graph().graql().parsePatterns("(member-location: $x, container-location: $x1) isa sublocate;"));

        InferenceRule R2 = new InferenceRule(snbGraph.graph().admin().getMetaRuleInference().addRule(body, head), snbGraph.graph());
        Rule rule = Utility.createSubPropertyRule(parent, child, roleMap, snbGraph.graph());
        InferenceRule R = new InferenceRule(rule, snbGraph.graph());

        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testTransitiveRule() {
        Rule rule = Utility.createTransitiveRule(snbGraph.graph().getRelationType("sublocate"),
                snbGraph.graph().getRoleType("member-location").getName(), snbGraph.graph().getRoleType("container-location").getName(), snbGraph.graph());

        InferenceRule R = new InferenceRule(rule, snbGraph.graph());

        Pattern body = and(snbGraph.graph().graql().parsePatterns("(member-location: $x, container-location: $z) isa sublocate;" +
                "(member-location: $z, container-location: $y) isa sublocate;"));
        Pattern head = and(snbGraph.graph().graql().parsePatterns("(member-location: $x, container-location: $y) isa sublocate;"));

        InferenceRule R2 = new InferenceRule(snbGraph.graph().admin().getMetaRuleInference().addRule(body, head), snbGraph.graph());
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testReflexiveRule() {
        Rule rule = Utility.createReflexiveRule(snbGraph.graph().getRelationType("knows"), snbGraph.graph());
        InferenceRule R = new InferenceRule(rule, snbGraph.graph());

        Pattern body = and(snbGraph.graph().graql().parsePatterns("($x, $y) isa knows;"));
        Pattern head = and(snbGraph.graph().graql().parsePatterns("($x, $x) isa knows;"));

        InferenceRule R2 = new InferenceRule(snbGraph.graph().admin().getMetaRuleInference().addRule(body, head), snbGraph.graph());
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testPropertyChainRule() {
        RelationType resides = snbGraph.graph().getRelationType("resides");
        RelationType sublocate = snbGraph.graph().getRelationType("sublocate");

        LinkedHashMap<RelationType, Pair<TypeName, TypeName>> chain = new LinkedHashMap<>();

        chain.put(resides, new Pair<>(snbGraph.graph().getRoleType("located-subject").getName(), snbGraph.graph().getRoleType("subject-location").getName()));
        chain.put(sublocate, new Pair<>(snbGraph.graph().getRoleType("member-location").getName(), snbGraph.graph().getRoleType("container-location").getName()));

        Rule rule = Utility.createPropertyChainRule(resides, snbGraph.graph().getRoleType("located-subject").getName(),
                snbGraph.graph().getRoleType("subject-location").getName(), chain, snbGraph.graph());
        InferenceRule R = new InferenceRule(rule, snbGraph.graph());

        Pattern body = and(snbGraph.graph().graql().parsePatterns("(located-subject: $x, subject-location: $y) isa resides;" +
                "(member-location: $z, container-location: $y) isa sublocate;"));
        Pattern head = and(snbGraph.graph().graql().parsePatterns("(located-subject: $x, subject-location: $z) isa resides;"));

        InferenceRule R2 = new InferenceRule(snbGraph.graph().admin().getMetaRuleInference().addRule(body, head), snbGraph.graph());
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testComma(){
        String queryString = "match $x isa person, has firstname 'Bob', has name 'Bob', value 'Bob', has age <21;";
        String queryString2 = "match $x isa person; $x has firstname 'Bob';$x has name 'Bob';$x value 'Bob';$x has age <21;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertEquals(query.execute(), query2.execute());
    }

    @Test
    public void testComma2(){
        String queryString = "match $x isa person, value <21, value >18;";
        String queryString2 = "match $x isa person;$x value <21;$x value >18;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertEquals(query.execute(), query2.execute());
    }

    @Test
    public void testResourceAsVar(){
        String patternString = "{$x isa person, has firstname $y;}";
        String patternString2 = "{$x isa person;$x has firstname $y;}";
        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, snbGraph.graph()), snbGraph.graph());
        assertTrue(query.isEquivalent(query2));
    }

    @Test
    public void testResourceAsVar2(){
        String queryString = "match $x has firstname $y;";
        QueryBuilder qb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = qb.parse(queryString);
        Pattern body = and(snbGraph.graph().graql().parsePatterns("$x isa person;$x has name 'Bob';"));
        Pattern head = and(snbGraph.graph().graql().parsePatterns("$x has firstname 'Bob';"));
        snbGraph.graph().admin().getMetaRuleInference().addRule(body, head);
        Reasoner.commitGraph(snbGraph.graph());

        QueryAnswers answers = new QueryAnswers(query.admin().results());
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testResourceAsVar3(){
        String patternString = "{$x isa person;$x has age <10;}";
        String patternString2 = "{$x isa person;$x has age $y;$y value <10;}";
        ReasonerQueryImpl query = new ReasonerAtomicQuery(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerAtomicQuery(conjunction(patternString2, snbGraph.graph()), snbGraph.graph());
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar4(){
        String patternString = "{$x has firstname 'Bob';}";
        String patternString2 = "{$x has firstname $y;$y value 'Bob';}";
        ReasonerQueryImpl query = new ReasonerAtomicQuery(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerAtomicQuery(conjunction(patternString2, snbGraph.graph()), snbGraph.graph());
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar5(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{$x has firstname 'Bob', has lastname 'Geldof';}";
        String patternString2 = "{$x has firstname 'Bob';$x has lastname 'Geldof';}";
        String patternString3 = "{$x has firstname $x1;$x has lastname $x2;$x1 value 'Bob';$x2 value 'Geldof';}";
        String patternString4 = "{$x has firstname $x2;$x has lastname $x1;$x2 value 'Bob';$x1 value 'Geldof';}";
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
    public void testNoRelationType(){
        String queryString = "match $x isa city;$y isa country;($x, $y);$y has name 'Poland';$x has name $name;";
        String queryString2 = "match $x isa city;$y isa country;$y has name 'Poland';$x has name $name;" +
                "($x, $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testNoRelationTypeWithRoles(){
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);$y has name 'Poland';";
        String queryString2 = "match $x isa city;$y isa country;" +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;$y has name 'Poland';";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testNoRelationTypeWithRoles2(){
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);";
        String queryString2 = "match $x isa city;$y isa country;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    //TODO need to unify types in rules potentially
    @Test
    public void testTypeVar(){
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
    public void testTypeVar2(){
        String queryString = "match $x isa $type;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String explicitQuery = "match $y has name 'Poland';$x isa $type;$x has $name;" +
                "{" +
                "{$name value 'Warsaw-Polytechnics' or $name value 'University-of-Warsaw';};" +
                "{$type type-name 'university' or $type type-name 'entity' or $type type-name 'concept';};" +
                "} or {" +
                "{$name value 'Warsaw' or $name value 'Wroclaw';};" +
                "{$type type-name 'city' or $type type-name 'geoObject' or $type type-name 'entity' or $type type-name 'concept';};" +
                "} or {" +
                "{$name value 'Masovia' or $name value 'Silesia';};" +
                "{$type type-name 'region' or $type type-name 'geoObject' or $type type-name 'entity' or $type type-name 'concept';};" +
                "}; select $x, $y, $type;";
        MatchQuery query = geoGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = geoGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testTypeVar3(){
        String queryString = "match $x isa $type;$type type-name 'university';" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String explicitQuery = "match $y has name 'Poland';" +
                "{$x isa $type;$type type-name 'university';$x has name 'Warsaw-Polytechnics';} or" +
                "{$x isa $type;$type type-name 'university';$x has name 'University-of-Warsaw';};";
        MatchQuery query = geoGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = geoGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testSub(){
        String queryString = "match $x isa $type;$type sub geoObject;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';$x has name $name;";
        String queryString2 = "match $x isa $type;{$type type-name 'region';} or {$type type-name 'city';} or {$type type-name 'geoObject';};" +
                "$y isa country;$y has name 'Poland';(geo-entity: $x, entity-location: $y) isa is-located-in;$x has name $name;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testSub2(){
        String queryString = "match $x isa person;$y isa $type;$type sub recommendable;($x, $y) isa recommendation;";
        String explicitQuery = "match $x isa person, has name $xName;$y isa $type;$y has name $yName;" +
                "{$type type-name 'recommendable' or $type type-name 'product' or $type type-name 'tag';};" +
                "{$xName value 'Alice';$yName value 'War of the Worlds';} or" +
                "{$xName value 'Bob';{$yName value 'Ducatti 1299';} or {$yName value 'The Good the Bad the Ugly';};} or" +
                "{$xName value 'Charlie';{$yName value 'Blizzard of Ozz';} or {$yName value 'Stratocaster';};} or " +
                "{$xName value 'Denis';{$yName value 'Colour of Magic';} or {$yName value 'Dorian Gray';};} or"+
                "{$xName value 'Frank';$yName value 'Nocturnes';} or" +
                "{$xName value 'Karl Fischer';{$yName value 'Faust';} or {$yName value 'Nocturnes';};} or " +
                "{$xName value 'Gary';$yName value 'The Wall';} or" +
                "{$xName value 'Charlie';" +
                "{$yName value 'Yngwie Malmsteen';} or {$yName value 'Cacophony';} or {$yName value 'Steve Vai';} or {$yName value 'Black Sabbath';};} or " +
                "{$xName value 'Gary';$yName value 'Pink Floyd';};select $x, $y, $type;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testPlays(){
        String queryString = "match $x plays geo-entity;$y isa country;$y has name 'Poland';($x, $y) isa is-located-in;";
        String explicitQuery = "match $y has name 'Poland';$x has $name;" +
                "{$name value 'Warsaw-Polytechnics' or $name value 'University-of-Warsaw' or " +
                "$name value 'Warsaw' or $name value 'Wroclaw' or " +
                "$name value 'Masovia' or $name value 'Silesia';}; select $x, $y;";
        MatchQuery query = geoGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = geoGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testPlays2(){
        String queryString = "match $x plays $role;$y isa country;$y has name 'Poland';($x, $y) isa is-located-in;";
        String explicitQuery = "match $y has name 'Poland';$x has $name;" +
                "{" +
                "{$role type-name geo-entity or $role type-name concept or $role type-name role;};" +
                "{$name value 'Warsaw-Polytechnics' or $name value 'University-of-Warsaw' or " +
                "$name value 'Warsaw' or $name value 'Wroclaw' or " +
                "$name value 'Masovia' or $name value 'Silesia';};" +
                "} or {" +
                "{$role type-name entity-location or $role type-name concept or $role type-name role;};" +
                "{$name value 'Europe' or $name value 'Warsaw' or $name value 'Masovia' or $name value 'Silesia';};" +
                "}; select $x, $y, $role;";
        MatchQuery query = geoGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = geoGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testTautology(){
        String queryString = "match ($x, $y) isa is-located-in;city sub geoObject;";
        String queryString2 = "match ($x, $y) isa is-located-in;geoObject sub city;";
        String queryString3 = "match ($x, $y) isa is-located-in;";
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString3);
        MatchQuery query2 = qb.parse(queryString);
        MatchQuery query3 = iqb.parse(queryString);
        MatchQuery query4 = iqb.parse(queryString2);

        query.execute();
        assertQueriesEqual(query2, query3);
        assertTrue(query4.execute().isEmpty());
    }

    //TODO BUG: getRulesOfConclusion on geo-entity returns a rule!
    @Test
    public void testPlaysRole(){
        String queryString = "match $x isa $type;$type plays-role geo-entity;$y isa country;$y has name 'Poland';" +
             "($x, $y) isa is-located-in;";
        String explicitQuery = "match $y has name 'Poland';$x isa $type;$x has $name;" +
                "{" +
                "{$name value 'Europe';};" +
                "{$type type-name 'continent' or $type type-name 'geoObject';};" +
                "} or {" +
                "{$name value 'Warsaw-Polytechnics' or $name value 'University-of-Warsaw';};" +
                "{$type type-name 'university';};" +
                "} or {" +
                "{$name value 'Warsaw' or $name value 'Wroclaw';};" +
                "{$type type-name 'city' or $type type-name 'geoObject';};" +
                "} or {" +
                "{$name value 'Masovia' or $name value 'Silesia';};" +
                "{$type type-name 'region' or $type type-name 'geoObject';};" +
                "}; select $x, $y, $type;";
        MatchQuery query = geoGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = geoGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    //TODO loses type variable as non-core types are not unified in rules
    @Test
    public void testPlaysRole2(){
        String queryString = "match $x isa person;$y isa $type;$type plays-role recommended-product;($x, $y) isa recommendation;";
        String queryString2 = "match $x isa person;$y isa $type;{$type type-name 'product';} or {$type type-name 'tag';};($x, $y) isa recommendation;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testHasResource(){
        String queryString = "match $x isa $type;$type has-resource name;$y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testHasResource2(){
        String queryString = "match $x isa $type;$type has-resource name;$y isa product;($x, $y) isa recommendation;";
        //String queryString2 = "match $x isa $type;$y isa product;($x, $y) isa recommendation;";
        String explicitQuery = "match $x isa person, has name $xName;$x isa $type;$y has name $yName;" +
                "{$type type-name 'person' or $type type-name 'entity2';};" +
                "{$xName value 'Alice';$yName value 'War of the Worlds';} or" +
                "{$xName value 'Bob';{$yName value 'Ducatti 1299';} or {$yName value 'The Good the Bad the Ugly';};} or" +
                "{$xName value 'Charlie';{$yName value 'Blizzard of Ozz';} or {$yName value 'Stratocaster';};} or " +
                "{$xName value 'Denis';{$yName value 'Colour of Magic';} or {$yName value 'Dorian Gray';};} or"+
                "{$xName value 'Frank';$yName value 'Nocturnes';} or" +
                "{$xName value 'Karl Fischer';{$yName value 'Faust';} or {$yName value 'Nocturnes';};} or " +
                "{$xName value 'Gary';$yName value 'The Wall';};select $x, $y, $type;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testRegex(){
        String queryString = "match $y isa country;$y has name $name;"+
                "$name value  /.*(.*)land(.*).*/;($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testContains(){
        String queryString = "match $y isa country;$y has name $name;"+
                "$name value contains 'land';($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testIndirectRelation(){
        String queryString = "match ($x, $y) isa $rel;$rel type-name is-located-in;select $x, $y;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testVarContraction(){
        Utility.createReflexiveRule(snbGraph.graph().getRelationType("knows"), snbGraph.graph());
        String queryString = "match ($x, $y) isa knows;select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Ignore
    @Test
    //propagated sub [x/Bob] prevents from capturing the right inference
    public void testVarContraction2(){
        Utility.createReflexiveRule(snbGraph.graph().getRelationType("knows"), snbGraph.graph());
        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Ignore
    @Test
    //Bug with unification, perhaps should unify select vars not atom vars
    public void testVarContraction3(){
        Pattern body = snbGraph.graph().graql().parsePattern("$x isa person");
        Pattern head = snbGraph.graph().graql().parsePattern("($x, $x) isa knows");
        snbGraph.graph().admin().getMetaRuleInference().addRule(body, head);

        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testTypeVariable(){
        String queryString = "match $x isa $type;$type type-name 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testTypeVariable2(){
        String queryString = "match $x isa $type;$type type-name 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;$y has name 'Poland';select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in;$y has name 'Poland'; $y isa country;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testRelationVariable(){
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        String queryString2 = "match $r(geo-entity: $x, entity-location: $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = new QueryAnswers(query.admin().results());
        QueryAnswers answers2 = new QueryAnswers(query2.admin().results());
        answers2.forEach(answer -> assertEquals(answer.size(), 3));
        assertEquals(answers.size(), answers2.size());
    }

    @Test
    public void testRelationVariable2(){
        String queryString = "match ($x, $y) isa is-located-in;";
        String queryString2 = "match $r($x, $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = new QueryAnswers(query.admin().results());
        QueryAnswers answers2 = new QueryAnswers(query2.admin().results());
        answers2.forEach(answer -> assertEquals(answer.size(), 3));
        assertEquals(answers.size(), answers2.size());
    }

    @Test
    public void testUnspecifiedCastings(){
        String queryString = "match (geo-entity: $x) isa is-located-in;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testUnspecifiedCastings2(){
        String queryString = "match (geo-entity: $x);";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testRelationTypeVar(){
        String queryString = "match (geo-entity: $x) isa $type;$type type-name 'is-located-in'; select $x;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testRelationTypeVar2(){
        String queryString = "match (geo-entity: $x) isa $type;$type type-name 'is-located-in';";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers.filterVars(Sets.newHashSet(VarName.of("x"))), answers2);
    }

    @Test
    public void testLimit(){
        String limitQueryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in;limit 5;";
        String queryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in;";
        MatchQuery limitQuery = geoGraph.graph().graql().parse(limitQueryString);
        MatchQuery query = geoGraph.graph().graql().parse(queryString);

        QueryAnswers limitedAnswers = queryAnswers(limitQuery);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(answers.size() > limitedAnswers.size());
        assertTrue(answers.containsAll(limitedAnswers));
    }

    @Test
    public void testOrder(){
        String queryString = "match $p isa person, has age $a;$pr isa product;($p, $pr) isa recommendation;order by $a;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);

        List<Map<String, Concept>> answers = query.execute();
        assertEquals(answers.iterator().next().get("a").asResource().getValue().toString(), "19");
    }

    @Test
    public void testOrderAndOffset(){
        String queryString = "match $p isa person, has age $a, has name $n;$pr isa product;($p, $pr) isa recommendation;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);

        final int offset = 3;
        List<Map<String, Concept>> fullAnswers = query.execute();
        List<Map<String, Concept>> answers = query.orderBy(VarName.of("a")).offset(offset).execute();
        List<Map<String, Concept>> answers2 = query.orderBy(VarName.of("a")).execute();

        assertEquals(fullAnswers.size(), answers.size() + offset);
        assertEquals(answers2.size(), answers.size() + offset);
        assertEquals(answers.iterator().next().get("a").asResource().getValue().toString(), "23");
    }

    @Test
    public void testIsAbstract(){
        String queryString = "match $x is-abstract;";
        QueryAnswers answers = queryAnswers(snbGraph.graph().graql().infer(true).materialise(false).parse(queryString));
        QueryAnswers expAnswers = queryAnswers(snbGraph.graph().graql().infer(false).parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testTypeRegex(){
        String queryString = " match $x sub resource, regex /name/;";
        QueryAnswers answers = queryAnswers(snbGraph.graph().graql().infer(true).materialise(false).parse(queryString));
        QueryAnswers expAnswers = queryAnswers(snbGraph.graph().graql().infer(false).parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testDataType(){
        String queryString = " match $x sub resource, datatype string;";
        QueryAnswers answers = queryAnswers(snbGraph.graph().graql().infer(true).materialise(false).parse(queryString));
        QueryAnswers expAnswers = queryAnswers(snbGraph.graph().graql().infer(false).parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testHasRole() {
        String queryString = "match ($x, $y) isa $rel-type;$rel-type has-role geo-entity;" +
            "$y isa country;$y has name 'Poland';select $x;";
        String queryString2 = "match $y isa country;" +
            "($x, $y) isa is-located-in;$y has name 'Poland'; select $x;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testHasRole2() {
        String queryString = "match ($x, $y) isa $rel;$rel has-role $role;";
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
    public void testScope(){
        String queryString = "match $r ($p, $pr) isa recommendation;$r has-scope $s;";
        QueryAnswers answers = queryAnswers(snbGraph.graph().graql().infer(true).materialise(false).parse(queryString));
    }

    @Test
    public void testResourceComparison(){
        //recommendations of products for people older than Denis - Frank, Karl and Gary
        String queryString = "match $b has name 'Denis', has age $x; $p has name $name, has age $y; $y value > $x;"+
                "$pr isa product;($p, $pr) isa recommendation;select $p, $y, $pr, $name;";
        String explicitQuery = "match $p isa person, has age $y, has name $name;$pr isa product, has name $yName;" +
                "{$name value 'Frank';$yName value 'Nocturnes';} or" +
                "{$name value 'Karl Fischer';{$yName value 'Faust';} or {$yName value 'Nocturnes';};} or " +
                "{$name value 'Gary';$yName value 'The Wall';};select $p, $pr, $y, $name;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testResourceComparison2(){
        String queryString = "match $p has name $name, has age $x;$p2 has name 'Denis', has age $y;$x value < $y;" +
                "$t isa tag;($p, $t) isa recommendation; select $p, $name, $x, $t;";
        String explicitQuery = "match " +
                "$p isa person, has age $x, has name $name;$t isa tag, has name $yName;" +
                "{$name value 'Charlie';" +
                "{$yName value 'Yngwie Malmsteen';} or {$yName value 'Cacophony';} or" +
                "{$yName value 'Steve Vai';} or {$yName value 'Black Sabbath';};};select $p, $name, $x, $t;";
        MatchQuery query = snbGraph.graph().graql().infer(true).materialise(false).parse(queryString);
        MatchQuery query2 = snbGraph.graph().graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testTypeRelation(){
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
    public void testTypeRelationWithMaterialisation(){
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
    public void testTypeRelation2(){
        String queryString = "match $x isa recommendation;";
        String queryString2 = "match $x($x1, $x2) isa recommendation;select $x;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);

        QueryAnswers requeriedAnswers = queryAnswers(query);
        assertEquals(requeriedAnswers.size(), answers.size());
    }

    @Test
    public void testTypeRelationWithMaterialisation2(){
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
    public void testRelationTypeVariable(){
        String queryString = "match $y isa product;(recommended-customer: $x, recommended-product: $y) isa $rel;";
        String queryString2 = "match $y isa product;(recommended-customer: $x, recommended-product: $y) isa $rel;$rel type-name recommendation;";
        QueryBuilder qb = snbGraph.graph().graql();
        QueryAnswers answers = queryAnswers(qb.infer(true).materialise(false).parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.infer(true).materialise(true).parse(queryString));
        QueryAnswers answers3 = queryAnswers(qb.infer(false).parse(queryString2));
        assertEquals(answers.size(), answers2.size());
        assertEquals(answers, answers2);
        assertEquals(answers2, answers3);
    }

    @Test
    public void testMatchAll(){
        String queryString = "match $y isa product;$r($x, $y);$x isa entity;";
        String queryString2 = "match $y isa product;{$r(recommended-customer: $x, recommended-product: $y) or " +
                "$r($x, $y) isa typing or $r($x, $y) isa made-in;};";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(false);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testHas(){
        String queryString = "match $x isa person has name $y;";
        String queryString2 = "match $x isa person has $y; $y isa name;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testMultiPredResource(){
        String queryString = "match $p isa person, has age $a;$a value >23; $a value <27;$pr isa product;" +
                "($p, $pr) isa recommendation; select $p, $pr;";
        String queryString2 = "match $p isa person, has age >23, has age <27;$pr isa product;" +
                "($p, $pr) isa recommendation;";
        QueryBuilder iqb = snbGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        assertQueriesEqual(query, query2);
    }

    @Test
    public void testAmbiguousRolePlayers(){
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(true);
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertTrue(answers2.containsAll(answers));
        assertEquals(2*answers.size(), answers2.size());
    }

    @Test
    public void testAmbiguousRolePlayersWithSub(){
        String queryString = "match ($x, $y) isa is-located-in;$x id '174';";
        QueryBuilder iqb = geoGraph.graph().graql().infer(true).materialise(true);
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertEquals(answers, answers2);
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().results());
    }
    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        QueryAnswers answers = queryAnswers(q1);
        QueryAnswers answers2 = queryAnswers(q2);
        assertEquals(answers, answers2);
    }
}











