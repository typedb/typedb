/*
 * MindmapsDB  A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.graql.reasoner;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Rule;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.graql.internal.reasoner.rule.InferenceRule;
import io.mindmaps.test.graql.reasoner.graphs.GeoGraph;
import io.mindmaps.test.graql.reasoner.graphs.SNBGraph;
import io.mindmaps.util.ErrorMessage;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.mindmaps.graql.internal.reasoner.Utility.createReflexiveRule;
import static io.mindmaps.graql.internal.reasoner.Utility.createSubPropertyRule;
import static io.mindmaps.graql.internal.reasoner.Utility.createTransitiveRule;
import static io.mindmaps.graql.internal.reasoner.Utility.printAnswers;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReasonerTest {

    @Test
    public void testSubPropertyRule() {
        MindmapsGraph graph = SNBGraph.getGraph();
        Map<String, String> roleMap = new HashMap<>();
        RelationType parent = graph.getRelationType("sublocate");
        RelationType child = graph.getRelationType("resides");

        roleMap.put(graph.getRoleType("member-location").getId(), graph.getRoleType("subject-location").getId());
        roleMap.put(graph.getRoleType("container-location").getId(), graph.getRoleType("located-subject").getId());

        String body = "(subject-location: $x, located-subject: $x1) isa resides;";
        String head = "(member-location: $x, container-location: $x1) isa sublocate;";

        InferenceRule R2 = new InferenceRule(graph.addRule(body, head, graph.getMetaRuleInference()), graph);
        Rule rule = createSubPropertyRule(parent , child, roleMap, graph);
        InferenceRule R = new InferenceRule(rule, graph);

        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testTransitiveRule() {
        MindmapsGraph graph = SNBGraph.getGraph();

        Rule rule = createTransitiveRule(graph.getRelationType("sublocate"),
                graph.getRoleType("member-location").getId(), graph.getRoleType("container-location").getId(), graph);

        InferenceRule R = new InferenceRule(rule, graph);

        String body = "(member-location: $x, container-location: $z) isa sublocate;" +
                      "(member-location: $z, container-location: $y) isa sublocate;";
        String head = "(member-location: $x, container-location: $y) isa sublocate;";

        InferenceRule R2 = new InferenceRule(graph.addRule(body, head, graph.getMetaRuleInference()), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testReflexiveRule() {
        MindmapsGraph graph = SNBGraph.getGraph();
        Rule rule = createReflexiveRule(graph.getRelationType("knows"), graph);
        InferenceRule R = new InferenceRule(rule, graph);

        String body = "($x, $y) isa knows;";
        String head = "($x, $x) isa knows;";

        InferenceRule R2 = new InferenceRule(graph.addRule(body, head, graph.getMetaRuleInference()), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testIdComma(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa Person, has name 'Bob';";
        Query query = new Query(queryString, graph);
        assertTrue(query.getAtoms().size() == 3);
    }

    @Test
    public void testComma(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has firstname 'Bob', has name 'Bob', value 'Bob', has age <21;";
        String queryString2 = "match $x isa person; $x has firstname 'Bob';$x has name 'Bob';$x value 'Bob';$x has age <21;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testComma2(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, value <21 value >18;";
        String queryString2 = "match $x isa person;$x value <21;$x value >18;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has firstname $y;";
        String queryString2 = "match $x isa person;$x has firstname $y;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar2(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x has firstname $y;";
        Query query = new Query(queryString, graph);
        String body = "$x isa person;$x has name 'Bob';";
        String head = "$x has firstname 'Bob';";
        graph.addRule(body, head, graph.getMetaRuleInference());

        Reasoner reasoner = new Reasoner(graph);
        QueryBuilder qb = graph.graql();
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Test
    public void testResourceAsVar3(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person;$x has age <10;";
        String queryString2 = "match $x isa person;$x has age $y;$y value <10;select $x;";
        Query query = new AtomicQuery(queryString, graph);
        Query query2 = new AtomicQuery(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar4(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x has firstname 'Bob';";
        String queryString2 = "match $x has firstname $y;$y value 'Bob';select $x;";
        Query query = new AtomicQuery(queryString, graph);
        Query query2 = new AtomicQuery(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar5(){
        MindmapsGraph graph = SNBGraph.getGraph();
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
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa city;$y isa country;($x, $y);$y has name 'Poland';";
        String queryString2 = "match $x isa city;$y isa country;" +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;$y has name 'Poland';";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testNoRelationTypeWithRoles(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
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
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa city;$y isa country;(geo-entity: $x, $y);";
        String queryString2 = "match $x isa city;$y isa country;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    @Ignore
    public void testTypeVar(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type id 'university';" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String queryString2 = "match $x isa university;$y isa country;$y has name 'Poland';" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery orig = Graql.parse(queryString);
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testSub(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type sub geoObject;" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country;$y has name 'Poland';";
        String queryString2 = "match $y isa country;$y has name 'Poland';" +
                "(geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    //TODO bug with answer unification
    @Test
    @Ignore
    public void testPlaysRole(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type plays-role geo-entity;$y isa country;$y has name 'Poland';" +
             "($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;$y has name 'Poland';" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testHasResource(){
        MindmapsGraph lgraph = SNBGraph.getGraph();
        String queryString = "match $x isa $type;$type has-resource name;$y isa product;($x, $y) isa recommendation;";
        String queryString2 = "match $x isa $person;$y isa product;($x, $y) isa recommendation;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testRegex(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
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
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $y isa country;$y has name $name;"+
                "$name value contains 'land';($x, $y) isa is-located-in;select $x, $y;";
        String queryString2 = "match $y isa country;{$y has name 'Poland';} or {$y has name 'England';};" +
                "($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(lgraph.graql().parse(queryString2)));
    }

    @Test
    public void testAndOrValuePredicate(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $y isa person;$y has age >18 and <25;";
        String explicitQuery = "match $y isa person;$y has name 'Bob';";
        MatchQuery query = new Query(queryString, graph);
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    @Ignore
    public void testAllVarsRelation(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match ($x, $y) isa $rel;$rel isa is-located-in;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    public void testVarContraction(){
        MindmapsGraph graph = SNBGraph.getGraph();
        createReflexiveRule(graph.getRelationType("knows"), graph);
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
        MindmapsGraph graph = SNBGraph.getGraph();
        createReflexiveRule(graph.getRelationType("knows"), graph);
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
        MindmapsGraph graph = SNBGraph.getGraph();
        String body = "$x isa person;";
        String head = "($x, $x) isa knows;";
        graph.addRule(body, head, graph.getMetaRuleInference());

        String queryString = "match ($x, $y) isa knows;$x has name 'Bob';select $y;";
        String explicitQuery = "match $y isa person;$y has name 'Bob' or $y has name 'Charlie';";
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        assertEquals(reasoner.resolve(new Query(queryString, graph)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    @Ignore
    public void testTypeVariable(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type id 'city';"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;select $x, $y;";
        String queryString2 = "match $x isa city;"+
                "(geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;";
        MatchQuery query = new Query(queryString, lgraph);
        MatchQuery query2 = new Query(queryString2, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        printAnswers(reasoner.resolve(query));
        assertEquals(reasoner.resolve(query), reasoner.resolve(query2));
    }

    @Test
    @Ignore
    public void testTypeVariable2(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $x isa $type;$type isa city;"+
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
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String queryString = "match $r($x, $y);";
        MatchQuery query = new Query(queryString, lgraph);

        Reasoner reasoner = new Reasoner(lgraph);
        printAnswers(reasoner.resolve(query));
    }
}
