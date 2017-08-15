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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.test.graphs.GeoGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.test.GraphContext;

import ai.grakn.test.GraknTestSetup;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class QueryTest {

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext genealogyOntology = GraphContext.preLoad("genealogy/ontology.gql").assumeTrue(GraknTestSetup.usingTinker());

    @BeforeClass
    public static void setUpClass() throws Exception {
        assumeTrue(GraknTestSetup.usingTinker());
    }

    @Test //simple equality tests between original and a copy of a query
    public void testAlphaEquivalence_QueryCopyIsAlphaEquivalent(){
        GraknTx graph = geoGraph.graph();
        String patternString = "{$x isa city;$y isa country;($x, $y) isa is-located-in;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        queryEquivalence(query, (ReasonerQueryImpl) query.copy(), true);
    }

    @Test //check two queries are alpha-equivalent - equal up to the choice of free variables
    public void testAlphaEquivalence() {
        GraknTx graph = geoGraph.graph();
        String patternString = "{" +
                "$x isa city, has name 'Warsaw';" +
                "$y isa region;" +
                "($x, $y) isa is-located-in;" +
                "($y, $z) isa is-located-in;" +
                "$z isa country, has name 'Poland';}";

        String patternString2 = "{"+
                "($r, $ctr) isa is-located-in;" +
                "($c, $r) isa is-located-in;" +
                "$c isa city, has name 'Warsaw';" +
                "$r isa region;" +
                "$ctr isa country, has name 'Poland';}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        queryEquivalence(query, query2, true);
    }

    //TODO
    @Ignore
    @Test
    public void testAlphaEquivalence_chainTreeAndLoopStructure() {
        GraknTx graph = geoGraph.graph();
        String chainString = "{" +
                "($x, $y) isa is-located-in;" +
                "($y, $z) isa is-located-in;" +
                "($z, $u) isa is-located-in;" +
                "}";

        String treeString2 = "{" +
                "($x, $y) isa is-located-in;" +
                "($y, $z) isa is-located-in;" +
                "($y, $u) isa is-located-in;" +
                "}";

        String loopString3 = "{" +
                "($x, $y) isa is-located-in;" +
                "($y, $z) isa is-located-in;" +
                "($z, $x) isa is-located-in;" +
                "}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(chainString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(treeString2, graph), graph);
        ReasonerQueryImpl query3 = ReasonerQueries.create(conjunction(loopString3, graph), graph);
        queryEquivalence(query, query2, false);
        queryEquivalence(query, query3, false);
        queryEquivalence(query2, query3, false);
    }

    @Test //tests various configurations of alpha-equivalence with extra type atoms present
    public void testAlphaEquivalence_nonMatchingTypes() {
        GraknTx graph = geoGraph.graph();
        String polandId = getConcept(graph, "name", "Poland").getId().getValue();
        String patternString = "{$y id '" + polandId + "'; $y isa country; (geo-entity: $y1, entity-location: $y), isa is-located-in;}";
        String patternString2 = "{$x1 id '" + polandId + "'; $y isa country; (geo-entity: $x1, entity-location: $x2), isa is-located-in;}";
        String patternString3 = "{$y id '" + polandId + "'; $x isa city; (geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;}";
        String patternString4 = "{$x isa city; (entity-location: $y1, geo-entity: $x), isa is-located-in;}";
        String patternString5 = "{(geo-entity: $y1, entity-location: $y2), isa is-located-in;}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        ReasonerQueryImpl query3 = ReasonerQueries.create(conjunction(patternString3, graph), graph);
        ReasonerQueryImpl query4 = ReasonerQueries.create(conjunction(patternString4, graph), graph);
        ReasonerQueryImpl query5 = ReasonerQueries.create(conjunction(patternString5, graph), graph);

        queryEquivalence(query, query2, false);
        queryEquivalence(query, query3, false);
        queryEquivalence(query2, query3, false);
        queryEquivalence(query, query4, false);
        queryEquivalence(query, query5, false);
        queryEquivalence(query3, query4, false);
        queryEquivalence(query3, query5, false);
        queryEquivalence(query4, query5, false);
    }

    @Test //tests alpha-equivalence of queries with indirect types
    public void testAlphaEquivalence_indirectTypes(){
        GraknTx graph = geoGraph.graph();
        String patternString = "{(entity-location: $x2, geo-entity: $x1) isa is-located-in;" +
                "$x1 isa $t1; $t1 sub geoObject;}";
        String patternString2 = "{(geo-entity: $y1, entity-location: $y2) isa is-located-in;" +
                "$y1 isa $t2; $t2 sub geoObject;}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        queryEquivalence(query, query2, true);
    }

    //Bug #11150 Relations with resources as single VarPatternAdmin
    @Test //tests whether directly and indirectly reified relations are equivalent
    public void testAlphaEquivalence_reifiedRelation(){
        GraknTx graph = genealogyOntology.graph();
        String patternString = "{$rel (happening: $b, protagonist: $p) isa event-protagonist has event-role 'parent';}";
        String patternString2 = "{$rel (happening: $c, protagonist: $r) isa event-protagonist; $rel has event-role 'parent';}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        queryEquivalence(query, query2, true);
    }

    private void queryEquivalence(ReasonerQueryImpl a, ReasonerQueryImpl b, boolean expectation){
        assertEquals(a.toString() + " =? " + b.toString(), a.equals(b), expectation);
        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), a.hashCode() == b.hashCode(), true);
        }
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private static Concept getConcept(GraknTx graph, String typeLabel, Object val){
        return graph.graql().match(Graql.var("x").has(typeLabel, val).admin()).execute().iterator().next().get("x");
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
