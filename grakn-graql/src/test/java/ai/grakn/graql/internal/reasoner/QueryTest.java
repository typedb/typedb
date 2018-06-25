/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.concept.Concept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.kbs.GeoKB;
import ai.grakn.test.kbs.SNBKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class QueryTest {

    @ClassRule
    public static final SampleKBContext geoKB = GeoKB.context();

    @ClassRule
    public static final SampleKBContext genealogySchema = SampleKBContext.load("genealogy/schema.gql");

    @ClassRule
    public static final SampleKBContext snbGraph = SNBKB.context();

    @BeforeClass
    public static void setUpClass() throws Exception {
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Test
    public void testQueryReiterationCondition_CyclicalRuleGraph(){
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String patternString = "{($x, $y) isa is-located-in;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        assertTrue(query.requiresReiteration());
    }

    @Test
    public void testQueryReiterationCondition_AcyclicalRuleGraph(){
        EmbeddedGraknTx<?> graph = snbGraph.tx();
        String patternString = "{($x, $y) isa recommendation;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        assertFalse(query.requiresReiteration());
    }

    @Test
    public void testQueryReiterationCondition_AnotherCyclicalRuleGraph(){
        EmbeddedGraknTx<?> graph = snbGraph.tx();
        String patternString = "{($x, $y);}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        assertTrue(query.requiresReiteration());
    }

    @Test //simple equality tests between original and a copy of a query
    public void testAlphaEquivalence_QueryCopyIsAlphaEquivalent(){
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String patternString = "{$x isa city;$y isa country;($x, $y) isa is-located-in;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        queryEquivalence(query, (ReasonerQueryImpl) query.copy(), true);
    }

    @Test //check two queries are alpha-equivalent - equal up to the choice of free variables
    public void testAlphaEquivalence() {
        EmbeddedGraknTx<?> graph = geoKB.tx();
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
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String chainString = "{" +
                "($x, $y) isa is-located-in;" +
                "($y, $z) isa is-located-in;" +
                "($z, $u) isa is-located-in;" +
                "}";

        String treeString = "{" +
                "($x, $y) isa is-located-in;" +
                "($y, $z) isa is-located-in;" +
                "($y, $u) isa is-located-in;" +
                "}";

        String loopString = "{" +
                "($x, $y) isa is-located-in;" +
                "($y, $z) isa is-located-in;" +
                "($z, $x) isa is-located-in;" +
                "}";

        ReasonerQueryImpl chainQuery = ReasonerQueries.create(conjunction(chainString, graph), graph);
        ReasonerQueryImpl treeQuery = ReasonerQueries.create(conjunction(treeString, graph), graph);
        ReasonerQueryImpl loopQuery = ReasonerQueries.create(conjunction(loopString, graph), graph);
        queryEquivalence(chainQuery, treeQuery, false);
        queryEquivalence(chainQuery, loopQuery, false);
        queryEquivalence(treeQuery, loopQuery, false);
    }

    @Test //tests various configurations of alpha-equivalence with extra type atoms present
    public void testAlphaEquivalence_nonMatchingTypes() {
        EmbeddedGraknTx<?> graph = geoKB.tx();
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
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String patternString = "{(entity-location: $x2, geo-entity: $x1) isa is-located-in;" +
                "$x1 isa $t1; $t1 sub geoObject;}";
        String patternString2 = "{(geo-entity: $y1, entity-location: $y2) isa is-located-in;" +
                "$y1 isa $t2; $t2 sub geoObject;}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        queryEquivalence(query, query2, true);
    }

    @Test
    public void testAlphaEquivalence_RelationsWithSubstitution(){
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String patternString = "{(role: $x, role: $y);$x id 'V666';}";
        String patternString2 = "{(role: $x, role: $y);$y id 'V666';}";
        String patternString3 = "{(role: $x, role: $y);$x id 'V666';$y id 'V667';}";
        String patternString4 = "{(role: $x, role: $y);$y id 'V666';$x id 'V667';}";
        String patternString5 = "{(entity-location: $x, geo-entity: $y);$x id 'V666';$y id 'V667';}";
        String patternString6 = "{(entity-location: $x, geo-entity: $y);$y id 'V666';$x id 'V667';}";
        String patternString7 = "{(role: $x, role: $y);$x id 'V666';$y id 'V666';}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);
        Conjunction<VarPatternAdmin> pattern5 = conjunction(patternString5, graph);
        Conjunction<VarPatternAdmin> pattern6 = conjunction(patternString6, graph);
        Conjunction<VarPatternAdmin> pattern7 = conjunction(patternString7, graph);

        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 = ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 = ReasonerQueries.atomic(pattern4, graph);
        ReasonerAtomicQuery query5 = ReasonerQueries.atomic(pattern5, graph);
        ReasonerAtomicQuery query6 = ReasonerQueries.atomic(pattern6, graph);
        ReasonerAtomicQuery query7 = ReasonerQueries.atomic(pattern7, graph);

        queryEquivalence(query, query2, true);
        queryEquivalence(query, query3, false);
        queryEquivalence(query, query4, false);
        queryEquivalence(query, query5, false);
        queryEquivalence(query, query6, false);
        queryEquivalence(query, query7, false);

        queryEquivalence(query2, query3, false);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query2, query5, false);
        queryEquivalence(query2, query6, false);
        queryEquivalence(query2, query7, false);

        queryEquivalence(query3, query4, true);
        queryEquivalence(query3, query5, false);
        queryEquivalence(query3, query6, false);
        queryEquivalence(query3, query7, false);

        queryEquivalence(query4, query5, false);
        queryEquivalence(query4, query6, false);
        queryEquivalence(query4, query7, false);

        queryEquivalence(query5, query6, false);
        queryEquivalence(query5, query7, false);

        queryEquivalence(query6, query7, false);
    }

    //Bug #11150 Relations with resources as single VarPatternAdmin
    @Test //tests whether directly and indirectly reified relations are equivalent
    public void testAlphaEquivalence_reifiedRelation(){
        EmbeddedGraknTx<?> graph = genealogySchema.tx();
        String patternString = "{$rel (happening: $b, protagonist: $p) isa event-protagonist has event-role 'parent';}";
        String patternString2 = "{$rel (happening: $c, protagonist: $r) isa event-protagonist; $rel has event-role 'parent';}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        queryEquivalence(query, query2, true);
    }

    @Test
    public void testWhenReifyingRelation_ExtraAtomIsCreatedWithUserDefinedName(){
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{($x, $y) relates geo-entity;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerQueryImpl query = ReasonerQueries.create(pattern, graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(pattern2, graph);
        assertEquals(query.getAtoms(RelationshipAtom.class).findFirst().orElse(null).isUserDefined(), false);
        assertEquals(query2.getAtoms(RelationshipAtom.class).findFirst().orElse(null).isUserDefined(), true);
        assertEquals(query.getAtoms().size(), 1);
        assertEquals(query2.getAtoms().size(), 2);
    }

    private void queryEquivalence(ReasonerQueryImpl a, ReasonerQueryImpl b, boolean expectation){
        assertEquals(a.toString() + " =? " + b.toString(), a.equals(b), expectation);
        assertEquals(b.toString() + " =? " + a.toString(), b.equals(a), expectation);
        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), a.hashCode() == b.hashCode(), true);
        }
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, EmbeddedGraknTx<?> graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private static Concept getConcept(EmbeddedGraknTx<?> graph, String typeLabel, Object val){
        return graph.graql().match(Graql.var("x").has(typeLabel, val).admin()).get("x")
                .stream().map(ans -> ans.get("x")).findAny().get();
    }
}
