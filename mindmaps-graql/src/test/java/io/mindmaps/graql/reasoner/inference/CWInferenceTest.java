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

package io.mindmaps.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.RuleType;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.reasoner.graphs.CWGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class CWInferenceTest {

    private static MindmapsGraph graph;
    private static Reasoner reasoner;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        graph = CWGraph.getGraph();
        reasoner = new Reasoner(graph);
        qb = Graql.withGraph(graph);
    }

    private static void printMatchQuery(MatchQuery query) {
        System.out.println(query.toString().replace(" or ", "\nor\n").replace("};", "};\n").replace("; {", ";\n{"));
    }

    @Test
    public void testWeapon() {
        String queryString = "match $x isa weapon";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match \n" +
                "{$x isa weapon} or {\n" +
                "{{$x isa missile} or {$x isa rocket;$x has propulsion 'gsp';}} or {$x isa rocket;$x has propulsion 'gsp';}\n" +
                "}";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testTransactionQuery() {
        String queryString = "match $x isa person;$z isa country;($x, $y, $z) isa transaction";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match \n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{($x, $y, $z) isa transaction} or {\n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{{$y isa weapon} or {\n" +
                "{{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';}} or {$y isa rocket;$y has propulsion 'gsp';};\n" +
                "}} or {{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';};};\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
                "}";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testTransactionQuery2() {
        String queryString = "match $x isa person;$z isa country;$y isa weapon;($x, $y, $z) isa transaction";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match \n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{$y isa weapon} or {\n" +
                "{{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';}} or {$y isa rocket;$y has propulsion 'gsp';};\n" +
                "};\n" +
                "{($x, $y, $z) isa transaction} or {\n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{{$y isa weapon} or {\n" +
                "{{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';}} or {$y isa rocket;$y has propulsion 'gsp';};\n" +
                "}} or {{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';};};\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
                "}";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testQuery() {
        String queryString = "match $x isa criminal;";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match " +
                "{$x isa criminal} or {" +
                "$x has nationality 'American';" +
                "($x, $y, $z) isa transaction or {" +
                    "$x isa person;$z isa country;" +
                    "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                    "($x, $z) isa is-paid-by;($z, $y) isa owns" +
                    "};" +
                "{$y isa weapon} or {$y isa missile} or {$y has propulsion 'gsp';$y isa rocket};\n" +
                "{$z has alignment 'hostile'} or {" +
                    "$y1 isa country;$y1 id 'America';" +
                    "($z, $y1) isa is-enemy-of;" +
                    "$z isa country;" +
                    "};" +
                "$x isa person;" +
                "$z isa country" +
                "}; select $x";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testQueryWithOr() {
        String queryString = "match {$x isa criminal} or {$x has nationality 'American';$x isa person}";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match " +
            "{{$x isa criminal} or {$x has nationality 'American';\n" +
            "{$z has alignment 'hostile'} or {" +
                "$yy value 'America';\n" +
                "($z, $yy) isa is-enemy-of;\n" +
                "$z isa country;\n" +
                "$yy isa country" +
            "};\n" +
            "($x, $y, $z) isa transaction or {" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
            "};\n" +
            "{$y isa weapon} or {{$y isa missile} or {$y has propulsion 'gsp';$y isa rocket}};\n" +
            "$x isa person;\n" +
            "$z isa country}} or {$x has nationality 'American';$x isa person} select $x";

        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testVarSub() {
        String queryString = "match" +
                "$y isa person;$yy isa country;$yyy isa weapon;\n" +
                "($y, $yy, $yyy) isa transaction";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match \n" +
                "$y isa person;\n" +
                "$yy isa country;\n" +
                "{$yyy isa weapon} or {\n" +
                "{{$yyy isa missile} or {$yyy isa rocket;$yyy has propulsion 'gsp';}} or {$yyy isa rocket;$yyy has propulsion 'gsp';};\n" +
                "};\n" +
                "{($y, $yy, $yyy) isa transaction} or {\n" +
                "$y isa person;\n" +
                "$yy isa country;\n" +
                "{{$yyy isa weapon} or {\n" +
                "{{$yyy isa missile} or {$yyy isa rocket;$yyy has propulsion 'gsp';}} or {$yyy isa rocket;$yyy has propulsion 'gsp';};\n" +
                "}} or {{$yyy isa missile} or {$yyy isa rocket;$yyy has propulsion 'gsp';};};\n" +
                "($y, $yy) isa is-paid-by;\n" +
                "($yy, $yyy) isa owns\n" +
                "}";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testVarSub2() {
        String queryString = "match" +
                "$y isa person;$z isa country;$x isa weapon;\n" +
                "($y, $z, $x) isa transaction";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match \n" +
                "$y isa person;\n" +
                "$z isa country;\n" +
                "{$x isa weapon} or {\n" +
                "{{$x isa missile} or {$x isa rocket;$x has propulsion 'gsp';}} or {$x isa rocket;$x has propulsion 'gsp';};\n" +
                "};\n" +
                "{($y, $z, $x) isa transaction} or {\n" +
                "$y isa person;\n" +
                "$z isa country;\n" +
                "{{$x isa weapon} or {\n" +
                "{{$x isa missile} or {$x isa rocket;$x has propulsion 'gsp';}} or {$x isa rocket;$x has propulsion 'gsp';};\n" +
                "}} or {{$x isa missile} or {$x isa rocket;$x has propulsion 'gsp';};};\n" +
                "($y, $z) isa is-paid-by;\n" +
                "($z, $x) isa owns\n" +
                "}";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testGraphCase() {
        MindmapsGraph localGraph = CWGraph.getGraph();
        Reasoner localReasoner = new Reasoner(localGraph);
        QueryBuilder lqb = Graql.withGraph(localGraph);
        RuleType inferenceRule = localGraph.getRuleType("inference-rule");

        localGraph.putEntityType("region");

        String R6_LHS = "match $x isa region";
        String R6_RHS = "match $x isa country";
        localGraph.putRule("R6", R6_LHS, R6_RHS, inferenceRule);

        localReasoner.linkConceptTypes();
        String queryString = "match $x isa criminal;";
        MatchQuery query = lqb.parseMatch(queryString);

        String explicitQuery = "match " +
                "{$x isa criminal} or {\n" +
                "$x has nationality 'American';\n" +
                "($x, $y, $z) isa transaction or {" +
                "$x isa person ;\n" +
                "{$z isa country} or {$z isa region};\n" +
                "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
                "};\n" +
                "{$y isa weapon} or {{$y isa missile} or {$y has propulsion 'gsp';$y isa rocket}};\n" +
                "{$z has alignment 'hostile'} or {" +
                "$yy id 'America';\n" +
                "($z, $yy) isa is-enemy-of;\n" +
                "$z isa country;\n" +
                "$yy isa country" +
                "}" +
                "} select $x";

        assertEquals(localReasoner.resolve(query), Sets.newHashSet(lqb.parseMatch(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), lqb.parseMatch(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
