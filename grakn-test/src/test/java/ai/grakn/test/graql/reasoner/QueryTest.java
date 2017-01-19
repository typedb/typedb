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
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graphs.SNBGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.test.GraphContext;
import com.google.common.collect.Sets;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class QueryTest {

    @ClassRule
    public static final GraphContext snbGraph = GraphContext.preLoad(SNBGraph.get());

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext ancestorGraph = GraphContext.preLoad("ancestor-friend-test.gql");

    @ClassRule
    public static final GraphContext genealogyOntology = GraphContext.preLoad("genealogy/ontology.gql");

    @BeforeClass
    public static void setUpClass() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void testCopyConstructor(){
        String patternString = "{$x isa person;$y isa product;($x, $y) isa recommendation;}";
        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl copy = new ReasonerQueryImpl(query);
        assertQueriesEqual(query.getMatchQuery(), copy.getMatchQuery());
    }

    @Test
    public void testTwinPattern() {
        String patternString = "{$x isa person;$x has name 'Bob';}";
        String patternString2 = "{$x isa person, has name 'Bob';}";
        String patternString3 = "{$x isa person, value 'Bob';}";
        String patternString4 = "{$x isa person;$x value 'Bob';}";

        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query3 = new ReasonerQueryImpl(conjunction(patternString3, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query4 = new ReasonerQueryImpl(conjunction(patternString4, snbGraph.graph()), snbGraph.graph());
        assertTrue(query.isEquivalent(query2));
        assertTrue(query3.isEquivalent(query4));
    }

    @Test
    public void testAlphaEquivalence() {
        String patternString = "{$x isa person;$t isa tag;$t value 'Michelangelo';" +
                "($x, $t) isa tagging;" +
                "$y isa product;$y value 'Michelangelo  The Last Judgement';}";

        String patternString2 = "{$x isa person;$y isa tag;$y value 'Michelangelo';" +
                "($x, $y) isa tagging;" +
                "$pr isa product;$pr value 'Michelangelo  The Last Judgement';}";

        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, snbGraph.graph()), snbGraph.graph());
        assertTrue(query.isEquivalent(query2));
    }

    @Test
    public void testAlphaEquivalence2() {
        String patternString = "{$X id 'a'; (ancestor-friend: $X, person: $Y), isa Ancestor-friend;}";
        String patternString2 = "{$X id 'a'; (person: $X, ancestor-friend: $Y), isa Ancestor-friend;}";
        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, ancestorGraph.graph()), ancestorGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, ancestorGraph.graph()), ancestorGraph.graph());
        assertTrue(!query.isEquivalent(query2));
    }

    @Test
    public void testAlphaEquivalence3() {
        GraknGraph graph = geoGraph.graph();
        String patternString = "{$y id 'Poland'; $y isa country; (geo-entity: $y1, entity-location: $y), isa is-located-in;}";
        String patternString2 = "{$y id 'Poland'; $x isa city; (geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;}";
        String patternString3 = "{$x isa city; (entity-location: $y1, geo-entity: $x), isa is-located-in;}";
        String patternString4 = "{(geo-entity: $y1, entity-location: $y2), isa is-located-in;}";

        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, graph), graph);
        ReasonerQueryImpl query3 = new ReasonerQueryImpl(conjunction(patternString3, graph), graph);
        ReasonerQueryImpl query4 = new ReasonerQueryImpl(conjunction(patternString4, graph), graph);

        assertTrue(!query.isEquivalent(query2));
        assertTrue(!query.isEquivalent(query3));
        assertTrue(!query.isEquivalent(query4));

        assertTrue(!query2.isEquivalent(query3));
        assertTrue(!query2.isEquivalent(query4));
        assertTrue(!query3.isEquivalent(query4));

        String patternString5 = "{(entity-location: $y, geo-entity: $y1), isa is-located-in;}";
        String patternString6 = "{(geo-entity: $y1, entity-location: $y2), isa is-located-in;}";
        String patternString7 = "{(entity-location: $y, geo-entity: $x), isa is-located-in; $x isa city;}";
        String patternString8 = "{$x isa city; (entity-location: $y1, geo-entity: $x), isa is-located-in;}";

        ReasonerQueryImpl query5 = new ReasonerQueryImpl(conjunction(patternString5, graph), graph);
        ReasonerQueryImpl query6 = new ReasonerQueryImpl(conjunction(patternString6, graph), graph);
        ReasonerQueryImpl query7 = new ReasonerQueryImpl(conjunction(patternString7, graph), graph);
        ReasonerQueryImpl query8 = new ReasonerQueryImpl(conjunction(patternString8, graph), graph);
        assertTrue(query5.isEquivalent(query6));
        assertTrue(query7.isEquivalent(query8));
    }

    @Test
    public void testQueryEquivalence(){
        String patternString = "{(entity-location: $x2, geo-entity: $x1) isa is-located-in;" +
                "$x1 isa $t1; $t1 sub geoObject;}";
        String patternString2 = "{(geo-entity: $y1, entity-location: $y2) isa is-located-in;" +
                "$y1 isa $t2; $t2 sub geoObject;}";
        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, geoGraph.graph()), geoGraph.graph());
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, geoGraph.graph()), geoGraph.graph());
        assertTrue(query.isEquivalent(query2));
    }

    @Test
    public void testUnification(){
        String parentString = "{(entity-location: $y, geo-entity: $y1), isa is-located-in;}";
        String childString = "{(geo-entity: $y1, entity-location: $y2), isa is-located-in;}";

        ReasonerAtomicQuery parentQuery = new ReasonerAtomicQuery(conjunction(parentString, geoGraph.graph()), geoGraph.graph());
        ReasonerAtomicQuery childQuery = new ReasonerAtomicQuery(conjunction(childString, geoGraph.graph()), geoGraph.graph());

        Atomic childAtom = childQuery.getAtom();
        Atomic parentAtom = parentQuery.getAtom();

        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);

        ReasonerAtomicQuery childCopy = new ReasonerAtomicQuery(childQuery);
        childCopy.unify(unifiers);
        Atomic childAtomCopy = childCopy.getAtom();
        assertTrue(!childAtomCopy.equals(childAtom));
    }

    //Bug #11150 Relations with resources as single VarAdmin
    @Test
    public void testRelationResources(){
        GraknGraph graph = genealogyOntology.graph();
        String patternString = "{$rel (happening: $b, protagonist: $p) isa event-protagonist has event-role 'parent';}";
        String patternString2 = "{$rel (happening: $b, protagonist: $p) isa event-protagonist; $rel has event-role 'parent';}";
        ReasonerQueryImpl query = new ReasonerQueryImpl(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(conjunction(patternString2, graph), graph);
        assertTrue(query.equals(query2));
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }

}
