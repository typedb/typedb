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
import ai.grakn.graql.Pattern;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.test.AbstractEngineTest;
import com.google.common.collect.Sets;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Rule;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.test.graql.reasoner.graphs.GeoGraph;
import ai.grakn.test.graql.reasoner.graphs.SNBGraph;
import java.util.LinkedHashMap;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.internal.reasoner.Utility.printAnswers;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class ReasonerTest extends AbstractEngineTest{

    @BeforeClass
    public static void onStartup(){
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

        InferenceRule R2 = new InferenceRule(graph.getMetaRuleInference().addRule(body, head), graph);
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

        InferenceRule R2 = new InferenceRule(graph.getMetaRuleInference().addRule(body, head), graph);
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

        InferenceRule R2 = new InferenceRule(graph.getMetaRuleInference().addRule(body, head), graph);
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

        InferenceRule R2 = new InferenceRule(graph.getMetaRuleInference().addRule(body, head), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testIdComma(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has name 'Bob';";
        Query query = new Query(queryString, graph);
        assertTrue(query.getAtoms().size() == 4);
    }

    @Test
    public void testComma(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has firstname 'Bob', has name 'Bob', value 'Bob', has age <21;";
        String queryString2 = "match $x isa person; $x has firstname 'Bob';$x has name 'Bob';$x value 'Bob';$x has age <21;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testComma2(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, value <21, value >18;";
        String queryString2 = "match $x isa person;$x value <21;$x value >18;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.equals(query2));
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
        Query query = new Query(queryString, graph);
        Pattern body = and(graph.graql().parsePatterns("$x isa person;$x has name 'Bob';"));
        Pattern head = and(graph.graql().parsePatterns("$x has firstname 'Bob';"));
        graph.getMetaRuleInference().addRule(body, head);

        Reasoner reasoner = new Reasoner(graph);
        QueryBuilder qb = graph.graql();
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
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
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa city;$y isa country;($x, $y);$y has name 'Poland';$x has name $name;";
        String queryString2 = "match $x isa city;$y isa country;$y has name 'Poland';$x has name $name;" +
                    "($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testNoRelationTypeWithRoles(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);$y has name 'Poland';";
        String queryString2 = "match $x isa city;$y isa country;" +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;$y has name 'Poland';";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testNoRelationTypeWithRoles2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);";
        String queryString2 = "match $x isa city;$y isa country;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    //TODO need to unify types in rules potentially
    @Test
    public void testTypeVar(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $x isa person;$y isa $type;($x, $y) isa recommendation;";
        String queryString2 = "match $y isa $type;" +
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

        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testTypeVar2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String queryString2 = "match $y has name 'Poland';" +
                "{$x isa $type;$type type-name 'university';$x has name 'Warsaw-Polytechnics';} or" +
                "{$x isa $type;$type type-name 'university';$x has name 'University-of-Warsaw';} or" +
                "{$x isa $type;{$type type-name 'city';} or {$type type-name 'geoObject';};$x has name 'Warsaw';} or" +
                "{$x isa $type;{$type type-name 'city';} or {$type type-name 'geoObject';};$x has name 'Wroclaw';} or" +
                "{$x isa $type;{$type type-name 'region';} or {$type type-name 'geoObject';};$x has name 'Masovia';} or" +
                "{$x isa $type;{$type type-name 'region';} or {$type type-name 'geoObject';};$x has name 'Silesia';};";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), Sets.newHashSet(query2));
    }

    @Test
    public void testTypeVar3(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type type-name 'university';" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String queryString2 = "match $y has name 'Poland';" +
                "{$x isa $type;$type type-name 'university';$x has name 'Warsaw-Polytechnics';} or" +
                "{$x isa $type;$type type-name 'university';$x has name 'University-of-Warsaw';};";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), Sets.newHashSet(query2));
    }

    @Test
    public void testSub(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type sub geoObject;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';$x has name $name;";
        String queryString2 = "match $x isa $type;{$type type-name 'region';} or {$type type-name 'city';} or {$type type-name 'geoObject';};" +
                "$y isa country;$y has name 'Poland';(geo-entity: $x, entity-location: $y) isa is-located-in;$x has name $name;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        Reasoner reasoner = new Reasoner(lgraph);
        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
    }

    @Test
    public void testSub2(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $x isa person;$y isa $type;$type sub entity;($x, $y) isa recommendation;";
        String queryString2 = "match $x isa person;$y isa $type;($x, $y) isa recommendation;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    //TODO BUG: getRulesOfConclusion on geo-entity returns a rule!
    @Test
    public void testPlaysRole(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type plays-role geo-entity;$y isa country;$y has name 'Poland';" +
             "($x, $y) isa is-located-in;";
        String queryString2 = "match $x isa $type;$y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    //TODO loses type variable as non-core types are not unified in rules
    @Test
    @Ignore
    public void testPlaysRole2(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $x isa person;$y isa $type;$type plays-role recommended-product;($x, $y) isa recommendation;";
        String queryString2 = "match $x isa person;$y isa $type;{$type type-name 'product';} or {$type type-name 'tag';};($x, $y) isa recommendation;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = lgraph.graql().parse(queryString2);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testHasResource(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type has-resource name;$y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testHasResource2(){
        GraknGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $x isa $type;$type has-resource name;$y isa product;($x, $y) isa recommendation;";
        String queryString2 = "match $x isa $type;$y isa product;($x, $y) isa recommendation;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testRegex(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name value  /.*(.*)land(.*).*/;($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(lgraph.graql().parse(queryString2)));
    }

    @Test
    public void testContains(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name value contains 'land';($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(lgraph.graql().parse(queryString2)));
    }

    @Test
    @Ignore
    public void testAllVarsRelation(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match ($x, $y) isa $rel;$rel isa is-located-in;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testVarContraction(){
        GraknGraph graph = SNBGraph.getGraph();
        Utility.createReflexiveRule(graph.getRelationType("knows"), graph);
        String queryString = "match ($x, $y) isa knows;select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        Query query = new Query(queryString, graph);
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    @Ignore
    //propagated sub [x/Bob] prevents from capturing the right inference
    public void testVarContraction2(){
        GraknGraph graph = SNBGraph.getGraph();
        Utility.createReflexiveRule(graph.getRelationType("knows"), graph);
        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        assertEquals(reasoner.resolve(new Query(queryString, graph)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    @Ignore
    //Bug with unification, perhaps should unify select vars not atom vars
    public void testVarContraction3(){
        GraknGraph graph = SNBGraph.getGraph();
        Pattern body = graph.graql().parsePattern("$x isa person;");
        Pattern head = graph.graql().parsePattern("($x, $x) isa knows;");
        graph.getMetaRuleInference().addRule(body, head);

        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        assertEquals(reasoner.resolve(new Query(queryString, graph)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testTypeVariable(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type type-name 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testTypeVariable2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type type-name 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;$y has name 'Poland';select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in;$y has name 'Poland'; $y isa country;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testRelationVariable(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        String queryString2 = "match $r(geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        answers2.forEach(answer -> {
            assert(answer.size() == 3);
        });
        assertTrue(answers.size() == answers2.size());
    }

    @Test
    public void testRelationVariable2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match ($x, $y) isa is-located-in;";
        String queryString2 = "match $r($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        answers2.forEach(answer -> {
            assert(answer.size() == 3);
        });
        assertTrue(answers.size() == answers2.size());
    }

    @Test
    public void testUnspecifiedCastings(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x) isa is-located-in;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);
        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testUnspecifiedCastings2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x);";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);
        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testRelationTypeVar(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x) isa $type;$type type-name 'is-located-in'; select $x;";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);
        Reasoner reasoner = new Reasoner(lgraph);
        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        printAnswers(answers);
        printAnswers(answers2);
        assertEquals(answers, answers2);
    }

    @Test
    public void testRelationTypeVar2(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x) isa $type;$type type-name 'is-located-in';";
        String queryString2 = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);
        Reasoner reasoner = new Reasoner(lgraph);
        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        printAnswers(answers);
        printAnswers(answers2);
        assertEquals(answers.filterVars(Sets.newHashSet("x")), answers2);
    }

    @Test
    public void testLimit(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String queryString = "match (geo-entity: $x, entity-location: $y)isa is-located-in;select $x; limit 5;";
        MatchQuery query = lgraph.graql().<MatchQuery>parse(queryString);
        Reasoner reasoner = new Reasoner(lgraph);
        printAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
    }
}











