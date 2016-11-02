/*
 * MindmapsDB - A Distributed Semantic Database
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

package io.mindmaps.test.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.RuleType;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.test.graql.reasoner.graphs.CWGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class CWInferenceTest {

    @Test
    public void testWeapon() {
        MindmapsGraph graph = CWGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa weapon;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "{$x isa weapon;} or {" +
                "{{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};} or {$x isa rocket;$x has propulsion 'gsp';};" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testAlignment() {
        MindmapsGraph graph = CWGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $z isa country;$z has alignment 'hostile';";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $z isa country, has name 'Nono';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testTransactionQuery() {
        MindmapsGraph graph = CWGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person;$z isa country;($x, $y, $z) isa transaction;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa person;" +
                "$z isa country;" +
                "{($x, $y, $z) isa transaction;} or {" +
                "$x isa person;" +
                "$z isa country;" +
                "{{$y isa weapon;} or {" +
                "{{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};} or {$y isa rocket;$y has propulsion 'gsp';};" +
                "};} or {{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};};" +
                "($x, $z) isa is-paid-by;" +
                "($z, $y) isa owns;" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testTransactionQuery2() {
        MindmapsGraph graph = CWGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person;$z isa country;$y isa weapon;($x, $y, $z) isa transaction;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa person;" +
                "$z isa country;" +
                "{$y isa weapon;} or {" +
                "{{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};} or {$y isa rocket;$y has propulsion 'gsp';};" +
                "};" +
                "{($x, $y, $z) isa transaction;} or {" +
                "$x isa person;" +
                "$z isa country;" +
                "{{$y isa weapon;} or {" +
                "{{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};} or {$y isa rocket;$y has propulsion 'gsp';};" +
                "};} or {{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};};" +
                "($x, $z) isa is-paid-by;" +
                "($z, $y) isa owns;" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testQuery() {
        MindmapsGraph graph = CWGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa criminal;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "{$x isa criminal;} or {" +
                "$x has nationality 'American';" +
                "($x, $y, $z) isa transaction or {" +
                    "$x isa person;$z isa country;" +
                    "{ {$y isa weapon;} or { {$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';}; }; };" +
                    "($x, $z) isa is-paid-by;($z, $y) isa owns;" +
                    "};" +
                "{$y isa weapon;} or {$y isa missile;} or {$y has propulsion 'gsp';$y isa rocket;};" +
                "{$z has alignment 'hostile';} or {" +
                    "$y1 isa country;$y1 has name 'America';" +
                    "($z, $y1) isa is-enemy-of;" +
                    "$z isa country;" +
                    "};" +
                "$x isa person;" +
                "$z isa country;" +
                "}; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testQueryWithOr() {
        MindmapsGraph graph = CWGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match {$x isa criminal;} or {$x has nationality 'American';$x isa person;};";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
            "{{$x isa criminal;} or {$x has nationality 'American';" +
            "{$z has alignment 'hostile';} or {" +
                "$yy value 'America';" +
                "($z, $yy) isa is-enemy-of;" +
                "$z isa country;" +
                "$yy isa country;" +
            "};" +
            "($x, $y, $z) isa transaction or {" +
                "$x isa person;" +
                "$z isa country;" +
                "{ {$y isa weapon;} or { {$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';} ;} ;};" +
                "($x, $z) isa is-paid-by;" +
                "($z, $y) isa owns;" +
            "};" +
            "{$y isa weapon;} or {{$y isa missile;} or {$y has propulsion 'gsp';$y isa rocket;};};" +
            "$x isa person;" +
            "$z isa country;};} or {$x has nationality 'American';$x isa person;}; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testVarSub() {
        MindmapsGraph graph = CWGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match" +
                "$y isa person;$yy isa country;$yyy isa weapon;" +
                "($y, $yy, $yyy) isa transaction;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$y isa person;" +
                "$yy isa country;" +
                "{$yyy isa weapon;} or {" +
                "{{$yyy isa missile;} or {$yyy isa rocket;$yyy has propulsion 'gsp';};} or {$yyy isa rocket;$yyy has propulsion 'gsp';};" +
                "};" +
                "{($y, $yy, $yyy) isa transaction;} or {" +
                "$y isa person;" +
                "$yy isa country;" +
                "{{$yyy isa weapon;} or {" +
                "{{$yyy isa missile;} or {$yyy isa rocket;$yyy has propulsion 'gsp';};} or {$yyy isa rocket;$yyy has propulsion 'gsp';};" +
                "};} or {{$yyy isa missile;} or {$yyy isa rocket;$yyy has propulsion 'gsp';};};" +
                "($y, $yy) isa is-paid-by;" +
                "($yy, $yyy) isa owns;" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testVarSub2() {
        MindmapsGraph graph = CWGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match" +
                "$y isa person;$z isa country;$x isa weapon;" +
                "($y, $z, $x) isa transaction;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$y isa person;" +
                "$z isa country;" +
                "{$x isa weapon;} or {" +
                "{{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};} or {$x isa rocket;$x has propulsion 'gsp';};" +
                "};" +
                "{($y, $z, $x) isa transaction;} or {" +
                "$y isa person;" +
                "$z isa country;" +
                "{{$x isa weapon;} or {" +
                "{{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};} or {$x isa rocket;$x has propulsion 'gsp';};" +
                "};} or {{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};};" +
                "($y, $z) isa is-paid-by;" +
                "($z, $x) isa owns;" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testGraphCase() {
        MindmapsGraph localGraph = CWGraph.getGraph();
        Reasoner localReasoner = new Reasoner(localGraph);
        QueryBuilder lqb = Graql.withGraph(localGraph);
        RuleType inferenceRule = localGraph.getRuleType("inference-rule");

        localGraph.putEntityType("region");

        String R6_LHS = "$x isa region;";
        String R6_RHS = "$x isa country;";
        localGraph.addRule(R6_LHS, R6_RHS, inferenceRule);

        localReasoner.linkConceptTypes();
        String queryString = "match $x isa criminal;";
        MatchQuery query = lqb.parse(queryString);

        String explicitQuery = "match " +
                "{$x isa criminal;} or {" +
                "$x has nationality 'American';" +
                "($x, $y, $z) isa transaction or {" +
                "$x isa person ;" +
                "{$z isa country;} or {$z isa region;};" +
                "{ {$y isa weapon;} or { {$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';}; }; };" +
                "($x, $z) isa is-paid-by;" +
                "($z, $y) isa owns;" +
                "};" +
                "{$y isa weapon;} or {{$y isa missile;} or {$y has propulsion 'gsp';$y isa rocket;};};" +
                "{$z has alignment 'hostile';} or {" +
                "$yy has name 'America';" +
                "($z, $yy) isa is-enemy-of;" +
                "$z isa country;" +
                "$yy isa country;" +
                "};" +
                "}; select $x;";

        assertEquals(localReasoner.resolve(query), Sets.newHashSet(lqb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), lqb.parseMatch(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
