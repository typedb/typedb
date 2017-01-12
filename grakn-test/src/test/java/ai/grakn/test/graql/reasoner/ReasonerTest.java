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
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.test.AbstractGraknTest;
import ai.grakn.test.graql.reasoner.graphs.GeoGraph;
import ai.grakn.test.graql.reasoner.graphs.SNBGraph;
import com.google.common.collect.Sets;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static ai.grakn.graql.internal.pattern.Patterns.varName;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class ReasonerTest extends AbstractGraknTest {
    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void testSubPropertyRule() {
        GraknGraph graph = SNBGraph.getGraph();
        Map<String, String> roleMap = new HashMap<>();
        RelationType parent = graph.getRelationType("sublocate");
        RelationType child = graph.getRelationType("resides");

        roleMap.put(graph.getRoleType("member-location").getName(), graph.getRoleType("subject-location").getName());
        roleMap.put(graph.getRoleType("container-location").getName(), graph.getRoleType("located-subject").getName());

        Pattern body = and(graph.graql().parsePatterns("(subject-location: $x, located-subject: $x1) isa resides;"));
        Pattern head = and(graph.graql().parsePatterns("(member-location: $x, container-location: $x1) isa sublocate;"));

        InferenceRule R2 = new InferenceRule(graph.admin().getMetaRuleInference().addRule(body, head), graph);
        Rule rule = Utility.createSubPropertyRule(parent, child, roleMap, graph);
        InferenceRule R = new InferenceRule(rule, graph);

        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testTransitiveRule() {
        GraknGraph graph = SNBGraph.getGraph();

        Rule rule = Utility.createTransitiveRule(graph.getRelationType("sublocate"),
                graph.getRoleType("member-location").getName(), graph.getRoleType("container-location").getName(), graph);

        InferenceRule R = new InferenceRule(rule, graph);

        Pattern body = and(graph.graql().parsePatterns("(member-location: $x, container-location: $z) isa sublocate;" +
                      "(member-location: $z, container-location: $y) isa sublocate;"));
        Pattern head = and(graph.graql().parsePatterns("(member-location: $x, container-location: $y) isa sublocate;"));

        InferenceRule R2 = new InferenceRule(graph.admin().getMetaRuleInference().addRule(body, head), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testReflexiveRule() {
        GraknGraph graph = SNBGraph.getGraph();
        Rule rule = Utility.createReflexiveRule(graph.getRelationType("knows"), graph);
        InferenceRule R = new InferenceRule(rule, graph);

        Pattern body = and(graph.graql().parsePatterns("($x, $y) isa knows;"));
        Pattern head = and(graph.graql().parsePatterns("($x, $x) isa knows;"));

        InferenceRule R2 = new InferenceRule(graph.admin().getMetaRuleInference().addRule(body, head), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testPropertyChainRule() {
        GraknGraph graph = SNBGraph.getGraph();
        RelationType resides = graph.getRelationType("resides");
        RelationType sublocate = graph.getRelationType("sublocate");

        LinkedHashMap<RelationType, Pair<String, String>> chain = new LinkedHashMap<>();

        chain.put(resides, new Pair<>(graph.getRoleType("located-subject").getName(), graph.getRoleType("subject-location").getName()));
        chain.put(sublocate, new Pair<>(graph.getRoleType("member-location").getName(), graph.getRoleType("container-location").getName()));

        Rule rule = Utility.createPropertyChainRule(resides, graph.getRoleType("located-subject").getName(),
                graph.getRoleType("subject-location").getName(), chain, graph);
        InferenceRule R = new InferenceRule(rule, graph);

        Pattern body = and(graph.graql().parsePatterns("(located-subject: $x, subject-location: $y) isa resides;" +
                "(member-location: $z, container-location: $y) isa sublocate;"));
        Pattern head = and(graph.graql().parsePatterns("(located-subject: $x, subject-location: $z) isa resides;"));

        InferenceRule R2 = new InferenceRule(graph.admin().getMetaRuleInference().addRule(body, head), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testIdComma(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has name 'Bob';";
        Query query = new Query(queryString, graph);
        assertEquals(query.getAtoms().size(), 2);
    }

    @Test
    public void testComma(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has firstname 'Bob', has name 'Bob', value 'Bob', has age <21;";
        String queryString2 = "match $x isa person; $x has firstname 'Bob';$x has name 'Bob';$x value 'Bob';$x has age <21;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().parse(queryString2);
        assertEquals(query.execute(), query2.execute());
    }

    @Test
    public void testComma2(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, value <21, value >18;";
        String queryString2 = "match $x isa person;$x value <21;$x value >18;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().parse(queryString2);
        assertEquals(query.execute(), query2.execute());
    }

    @Test
    public void testResourceAsVar(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has firstname $y;";
        String queryString2 = "match $x isa person;$x has firstname $y;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.isEquivalent(query2));
    }

    @Test
    public void testResourceAsVar2(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x has firstname $y;";
        QueryBuilder qb = graph.graql().infer(true);
        MatchQuery query = qb.parse(queryString);
        Pattern body = and(graph.graql().parsePatterns("$x isa person;$x has name 'Bob';"));
        Pattern head = and(graph.graql().parsePatterns("$x has firstname 'Bob';"));
        graph.admin().getMetaRuleInference().addRule(body, head);

        QueryAnswers answers = new QueryAnswers(query.admin().results());
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testResourceAsVar3(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person;$x has age <10;";
        String queryString2 = "match $x isa person;$x has age $y;$y value <10;select $x;";
        Query query = new AtomicQuery(queryString, graph);
        Query query2 = new AtomicQuery(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar4(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x has firstname 'Bob';";
        String queryString2 = "match $x has firstname $y;$y value 'Bob';select $x;";
        Query query = new AtomicQuery(queryString, graph);
        Query query2 = new AtomicQuery(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar5(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x has firstname 'Bob', has lastname 'Geldof';";
        String queryString2 = "match $x has firstname 'Bob';$x has lastname 'Geldof';";
        String queryString3 = "match $x has firstname $x1;$x has lastname $x2;$x1 value 'Bob';$x2 value 'Geldof';";
        String queryString4 = "match $x has firstname $x2;$x has lastname $x1;$x2 value 'Bob';$x1 value 'Geldof';";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        Query query3 = new Query(queryString3, graph);
        Query query4 = new Query(queryString4, graph);

        assertTrue(query.equals(query3));
        assertTrue(query.equals(query4));
        assertTrue(query2.equals(query3));
        assertTrue(query2.equals(query4));
    }

    @Test
    public void testNoRelationType(){
        GraknGraph graph = GeoGraph.getGraph();
        String queryString = "match $x isa city;$y isa country;($x, $y);$y has name 'Poland';$x has name $name;";
        String queryString2 = "match $x isa city;$y isa country;$y has name 'Poland';$x has name $name;" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testNoRelationTypeWithRoles(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);$y has name 'Poland';";
        String queryString2 = "match $x isa city;$y isa country;" +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;$y has name 'Poland';";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testNoRelationTypeWithRoles2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);";
        String queryString2 = "match $x isa city;$y isa country;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    //TODO need to unify types in rules potentially
    @Test
    public void testTypeVar(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $x isa person;$y isa $type;($x, $y) isa recommendation;";
        String explicitQuery = "match $y isa $type;" +
                "{$x has name 'Alice';$y has name 'War of the Worlds';} or" +
                "{$x has name 'Bob';{$y has name 'Ducatti 1299';} or " +
                    "{$y has name 'The Good the Bad the Ugly';};} or" +
                "{$x has name 'Charlie';{$y has name 'Blizzard of Ozz';} or " +
                    "{$y has name 'Stratocaster';};} or " +
                "{$x has name 'Denis';{$y has name 'Colour of Magic';} or " +
                    "{$y has name 'Dorian Gray';};} or"+
                "{$x has name 'Frank';$y has name 'Nocturnes';} or" +
                "{$x has name 'Karl Fischer';{$y has name 'Faust';} or " +
                        "{$y has name 'Nocturnes';};} or " +
                "{$x has name 'Gary';$y has name 'The Wall';} or" +
                "{$x has name 'Charlie';{$y has name 'Yngwie Malmsteen';} or " +
                    "{$y has name 'Cacophony';} or " +
                    "{$y has name 'Steve Vai';} or " +
                    "{$y has name 'Black Sabbath';};} or " +
                "{$x has name 'Gary';$y has name 'Pink Floyd';};";

        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Test
    public void testTypeVar2(){
        GraknGraph lgraph = GeoGraph.getGraph();
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
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);

                assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Test
    public void testTypeVar3(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type type-name 'university';" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String explicitQuery = "match $y has name 'Poland';" +
                "{$x isa $type;$type type-name 'university';$x has name 'Warsaw-Polytechnics';} or" +
                "{$x isa $type;$type type-name 'university';$x has name 'University-of-Warsaw';};";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Test
    public void testSub(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type sub geoObject;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';$x has name $name;";
        String queryString2 = "match $x isa $type;{$type type-name 'region';} or {$type type-name 'city';} or {$type type-name 'geoObject';};" +
                "$y isa country;$y has name 'Poland';(geo-entity: $x, entity-location: $y) isa is-located-in;$x has name $name;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testSub2(){
        GraknGraph lgraph = SNBGraph.getGraph();
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
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);

                assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    //TODO BUG: getRulesOfConclusion on geo-entity returns a rule!
    @Test
    public void testPlaysRole(){
        GraknGraph lgraph = GeoGraph.getGraph();
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
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);

                assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    //TODO loses type variable as non-core types are not unified in rules
    @Test
    @Ignore
    public void testPlaysRole2(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $x isa person;$y isa $type;$type plays-role recommended-product;($x, $y) isa recommendation;";
        String queryString2 = "match $x isa person;$y isa $type;{$type type-name 'product';} or {$type type-name 'tag';};($x, $y) isa recommendation;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testHasResource(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type has-resource name;$y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testHasResource2(){
        GraknGraph lgraph = SNBGraph.getGraph();
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
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);

        assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Test
    public void testRegex(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name value  /.*(.*)land(.*).*/;($x, $y) isa is-located-in;select $x, $y;";
        String explicitQuery = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testContains(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name value contains 'land';($x, $y) isa is-located-in;select $x, $y;";
        String explicitQuery = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testIndirectRelation(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match ($x, $y) isa $rel;$rel type-name is-located-in;select $x, $y;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testVarContraction(){
        GraknGraph graph = SNBGraph.getGraph();
        Utility.createReflexiveRule(graph.getRelationType("knows"), graph);
        String queryString = "match ($x, $y) isa knows;select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Ignore
    @Test
    //propagated sub [x/Bob] prevents from capturing the right inference
    public void testVarContraction2(){
        GraknGraph graph = SNBGraph.getGraph();
        Utility.createReflexiveRule(graph.getRelationType("knows"), graph);
        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Ignore
    @Test
    //Bug with unification, perhaps should unify select vars not atom vars
    public void testVarContraction3(){
        GraknGraph graph = SNBGraph.getGraph();
        Pattern body = graph.graql().parsePattern("$x isa person");
        Pattern head = graph.graql().parsePattern("($x, $x) isa knows");
        graph.admin().getMetaRuleInference().addRule(body, head);

        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Test
    public void testTypeVariable(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type type-name 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testTypeVariable2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type type-name 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;$y has name 'Poland';select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in;$y has name 'Poland'; $y isa country;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testRelationVariable(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        String queryString2 = "match $r(geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, false).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, false).collect(Collectors.toSet()));
        answers2.forEach(answer -> {
            assertEquals(answer.size(), 3);
        });
        assertEquals(answers.size(), answers2.size());
    }

    //TODO need answer extrapolation
    @Ignore
    @Test
    public void testRelationVariable2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match ($x, $y) isa is-located-in;";
        String queryString2 = "match $r($x, $y) isa is-located-in;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, false).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, false).collect(Collectors.toSet()));

        answers2.forEach(answer -> {
            assertEquals(answer.size(), 3);
        });
        assertEquals(answers.size(), answers2.size());
    }

    @Test
    public void testUnspecifiedCastings(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x) isa is-located-in;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testUnspecifiedCastings2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x);";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testRelationTypeVar(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x) isa $type;$type type-name 'is-located-in'; select $x;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testRelationTypeVar2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x) isa $type;$type type-name 'is-located-in';";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, false).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, false).collect(Collectors.toSet()));
        assertEquals(answers.filterVars(Sets.newHashSet(varName("x"))), answers2);
    }

    @Test
    public void testLimit(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String limitQueryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in;limit 5;";
        String queryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in;";
        MatchQuery limitQuery = lgraph.graql().parse(limitQueryString);
        MatchQuery query = lgraph.graql().parse(queryString);

        QueryAnswers limitedAnswers = queryAnswers(limitQuery);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(answers.size() > limitedAnswers.size());
        assertTrue(answers.containsAll(limitedAnswers));
    }

    @Test
    public void testOrder(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $p isa person, has age $a;$pr isa product;($p, $pr) isa recommendation;order by $a;";
        MatchQuery query = lgraph.graql().infer(true).parse(queryString);

        List<Map<String, Concept>> answers = query.execute();
        assertTrue(answers.iterator().next().get("a").asResource().getValue().toString().equals("19"));
    }

    @Test
    public void testOrderAndOffset(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String fullQueryString = "match $p isa person, has age $a;$pr isa product;($p, $pr) isa recommendation;";
        String queryString = "match $p isa person, has age $a;$pr isa product;($p, $pr) isa recommendation;order by $a; offset 3;";
        MatchQuery fullQuery = lgraph.graql().infer(true).parse(fullQueryString);
        MatchQuery query = lgraph.graql().infer(true).parse(queryString);

        List<Map<String, Concept>> fullAnswers = fullQuery.execute();
        List<Map<String, Concept>> answers = query.execute();
        assertEquals(fullAnswers.size(), answers.size() + 3);
        assertTrue(answers.iterator().next().get("a").asResource().getValue().toString().equals("23"));
    }

    @Test
    public void testIsAbstract(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $x is-abstract;";
        MatchQuery query = lgraph.graql().parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers expAnswers= queryAnswers(lgraph.graql().<MatchQuery>parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testTypeRegex(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = " match $x sub resource, regex /name/;";
        MatchQuery query = lgraph.graql().parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers expAnswers= queryAnswers(lgraph.graql().<MatchQuery>parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testDataType(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = " match $x sub resource, datatype string;";
        MatchQuery query = lgraph.graql().parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers expAnswers= queryAnswers(lgraph.graql().<MatchQuery>parse(queryString));
        assertEquals(answers, expAnswers);
    }

    @Test
    public void testHasRole() {
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match ($x, $y) isa $rel-type;$rel-type has-role geo-entity;" +
        "$y isa country;$y has name 'Poland';select $x;";
        String queryString2 = "match $y isa country;" +
        "($x, $y) isa is-located-in;$y has name 'Poland'; select $x;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().parse(queryString2);
        assertQueriesEqual(Reasoner.resolve(query, false), Reasoner.resolve(query2, false));
    }

    @Test
    public void testScope(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $r ($p, $pr) isa recommendation;$r has-scope $s;";
        MatchQuery query = lgraph.graql().parse(queryString);
    }

    @Test
    public void testResourceComparison(){
        GraknGraph lgraph = SNBGraph.getGraph();
        //recommendations of products for people older than Denis - Frank, Karl and Gary
        String queryString = "match $b has name 'Denis', has age $x; $p has name $name, has age $y; $y value > $x;"+
        "$pr isa product;($p, $pr) isa recommendation;select $p, $y, $pr, $name;";
        String explicitQuery = "match $p isa person, has age $y, has name $name;$pr isa product, has name $yName;" +
                "{$name value 'Frank';$yName value 'Nocturnes';} or" +
                "{$name value 'Karl Fischer';{$yName value 'Faust';} or {$yName value 'Nocturnes';};} or " +
                "{$name value 'Gary';$yName value 'The Wall';};select $p, $pr, $y, $name;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Test
    public void testResourceComparison2(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $p has name $name, has age $x;$p2 has name 'Denis', has age $y;$x value < $y;" +
                "$t isa tag;($p, $t) isa recommendation; select $p, $name, $x, $t;";
        String explicitQuery = "match " +
                "$p isa person, has age $x, has name $name;$t isa tag, has name $yName;" +
                "{$name value 'Charlie';" +
                "{$yName value 'Yngwie Malmsteen';} or {$yName value 'Cacophony';} or" +
                "{$yName value 'Steve Vai';} or {$yName value 'Black Sabbath';};};select $p, $name, $x, $t;";
        MatchQuery query = lgraph.graql().parse(queryString);
        MatchQuery query2 = lgraph.graql().infer(false).parse(explicitQuery);
        assertQueriesEqual(Reasoner.resolve(query, false), query2);
    }

    @Test
    public void testTypeRelation(){
        GraknGraph graph = GeoGraph.getGraph();
        GraknGraph graph2 = GeoGraph.getGraph();
        GraknGraph graph3 = GeoGraph.getGraph();
        String queryString = "match $x isa is-located-in;";
        String queryString2 = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in; select $x;";
        String queryString3 = "match $x ($x1, $x2) isa is-located-in; select $x;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph2.graql().parse(queryString2);
        MatchQuery query3 = graph3.graql().parse(queryString3);

        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, false).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, false).collect(Collectors.toSet()));
        QueryAnswers answers3 = new QueryAnswers(Reasoner.resolve(query3, false).collect(Collectors.toSet()));
        assertEquals(answers, answers2);
        assertEquals(answers2, answers3);
    }

    @Test
    public void testTypeRelationWithMaterialisation(){
        GraknGraph graph = GeoGraph.getGraph();
        GraknGraph graph2 = GeoGraph.getGraph();
        GraknGraph graph3 = GeoGraph.getGraph();
        String queryString = "match $x isa is-located-in;";
        String queryString2 = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in; select $x;";
        String queryString3 = "match $x ($x1, $x2) isa is-located-in; select $x;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph2.graql().parse(queryString2);
        MatchQuery query3 = graph3.graql().parse(queryString3);

        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, true).collect(Collectors.toSet()));
        QueryAnswers requeriedAnswers = new QueryAnswers(Reasoner.resolve(query, true).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, true).collect(Collectors.toSet()));
        QueryAnswers requeriedAnswers2 = new QueryAnswers(Reasoner.resolve(query2, true).collect(Collectors.toSet()));
        QueryAnswers answers3 = new QueryAnswers(Reasoner.resolve(query3, true).collect(Collectors.toSet()));
        QueryAnswers requeriedAnswers3 = new QueryAnswers(Reasoner.resolve(query3, true).collect(Collectors.toSet()));

        assertEquals(answers.size(), answers2.size());
        assertEquals(answers2.size(), answers3.size());
        assertEquals(requeriedAnswers.size(), answers.size());
        assertEquals(requeriedAnswers.size(), requeriedAnswers2.size());
        assertEquals(requeriedAnswers2.size(), requeriedAnswers3.size());
    }

    @Test
    public void testTypeRelation2(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa recommendation;";
        String queryString2 = "match $x($x1, $x2) isa recommendation;select $x;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().parse(queryString2);
        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, false).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, false).collect(Collectors.toSet()));
        assertEquals(answers, answers2);
        QueryAnswers requeriedAnswers = new QueryAnswers(Reasoner.resolve(query, false).collect(Collectors.toSet()));
        assertEquals(requeriedAnswers.size(), answers.size());
    }

    @Test
    public void testTypeRelationWithMaterialisation2(){
        GraknGraph graph = SNBGraph.getGraph();
        GraknGraph graph2 = SNBGraph.getGraph();
        GraknGraph graph3 = SNBGraph.getGraph();
        String queryString = "match $x isa recommendation;";
        String queryString2 = "match $x(recommended-product: $x1, recommended-customer: $x2) isa recommendation; select $x;";
        String queryString3 = "match $x($x1, $x2) isa recommendation; select $x;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph2.graql().parse(queryString2);
        MatchQuery query3 = graph3.graql().parse(queryString3);

        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, true).collect(Collectors.toSet()));
        QueryAnswers requeriedAnswers = new QueryAnswers(Reasoner.resolve(query, true).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, true).collect(Collectors.toSet()));
        QueryAnswers requeriedAnswers2 = new QueryAnswers(Reasoner.resolve(query2, true).collect(Collectors.toSet()));
        QueryAnswers answers3 = new QueryAnswers(Reasoner.resolve(query3, true).collect(Collectors.toSet()));
        QueryAnswers requeriedAnswers3 = new QueryAnswers(Reasoner.resolve(query3, true).collect(Collectors.toSet()));

        assertEquals(answers.size(), answers2.size());
        assertEquals(answers2.size(), answers3.size());
        assertEquals(requeriedAnswers.size(), answers.size());
        assertEquals(requeriedAnswers.size(), requeriedAnswers2.size());
        assertEquals(requeriedAnswers2.size(), requeriedAnswers3.size());
    }

    @Test
    public void testHas(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person has name $y;";
        String queryString2 = "match $x isa person has $y; $y isa name;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().parse(queryString2);
        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, true).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, true).collect(Collectors.toSet()));
        assertEquals(answers, answers2);
    }

    @Test
    public void testMultiPredResource(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $p isa person, has age $a;$a value >23; $a value <27;$pr isa product;" +
                "($p, $pr) isa recommendation; select $p, $pr;";
        String queryString2 = "match $p isa person, has age >23, has age <27;$pr isa product;" +
                "($p, $pr) isa recommendation;";
        MatchQuery query = graph.graql().parse(queryString);
        MatchQuery query2 = graph.graql().parse(queryString2);
        QueryAnswers answers = new QueryAnswers(Reasoner.resolve(query, true).collect(Collectors.toSet()));
        QueryAnswers answers2 = new QueryAnswers(Reasoner.resolve(query2, true).collect(Collectors.toSet()));
        assertEquals(answers, answers2);
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().results());
    }

    private void assertQueriesEqual(Stream<Map<VarName, Concept>> s1, Stream<Map<VarName, Concept>> s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.collect(Collectors.toSet()));
    }

    private void assertQueriesEqual(Stream<Map<VarName, Concept>> s1, MatchQuery s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.admin().streamWithVarNames().collect(Collectors.toSet()));
    }
}











