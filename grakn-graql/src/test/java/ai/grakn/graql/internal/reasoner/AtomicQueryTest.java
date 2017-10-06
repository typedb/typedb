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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.UnifierComparison;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.QueryEquivalence;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.GeoKB;
import ai.grakn.util.Schema;
import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Sets;
import java.util.HashSet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.GraqlTestUtil.assertNotExists;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicQueryTest {

    @ClassRule
    public static final SampleKBContext geoKB = SampleKBContext.preLoad(GeoKB.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext unificationTestSet = SampleKBContext.preLoad("unificationTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext unificationWithTypesSet = SampleKBContext.preLoad("unificationWithTypesTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() {
        assumeTrue(GraknTestSetup.usingTinker());
    }

    @Test
    public void testWhenConstructingNonAtomicQuery_ExceptionIsThrown() {
        GraknTx graph = geoKB.tx();
        String patternString = "{$x isa university;$y isa country;($x, $y) isa is-located-in;($y, $z) isa is-located-in;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        exception.expect(GraqlQueryException.class);
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);
    }

    @Test
    public void testWhenCreatingQueryWithNonexistentType_ExceptionIsThrown(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$x isa someType;}";
        exception.expect(GraqlQueryException.class);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
    }

    @Test
    public void testWhenMaterialising_MaterialisedInformationIsPresentInGraph(){
        GraknTx graph = geoKB.tx();
        QueryBuilder qb = graph.graql().infer(false);
        String explicitQuery = "match (geo-entity: $x, entity-location: $y) isa is-located-in;$x has name 'Warsaw';$y has name 'Poland'; get;";
        assertTrue(!qb.<GetQuery>parse(explicitQuery).iterator().hasNext());

        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        QueryAnswers answers = new QueryAnswers();

        answers.add(new QueryAnswer(
                ImmutableMap.of(
                        var("x"), getConceptByResourceValue(graph, "Warsaw"),
                        var("y"), getConceptByResourceValue(graph, "Poland")))
        );
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);

        assertNotExists(qb.parse(explicitQuery));
        answers.stream().forEach(atomicQuery::materialise);
        assertExists(qb.parse(explicitQuery));
    }

    private Concept getConceptByResourceValue(GraknTx graph, String id){
        Set<Concept> instances = graph.getAttributesByValue(id)
                .stream().flatMap(Attribute::ownerInstances).collect(Collectors.toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        return instances.iterator().next();
    }

    @Test
    public void testWhenCopying_TheCopyIsAlphaEquivalent(){
        GraknTx graph = geoKB.tx();
        String patternString = "{($x, $y) isa is-located-in;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery copy = ReasonerQueries.atomic(atomicQuery);
        assertEquals(atomicQuery, copy);
        assertEquals(atomicQuery.hashCode(), copy.hashCode());
    }

    @Test
    public void testWhenRoleTypesAreAmbiguous_answersArePermutedCorrectly(){
        GraknTx graph = geoKB.tx();
        String childString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";
        String parentString = "match ($x, $y) isa is-located-in; get;";

        QueryBuilder qb = graph.graql().infer(false);
        GetQuery childQuery = qb.parse(childString);
        GetQuery parentQuery = qb.parse(parentString);
        Set<Answer> answers = childQuery.stream().collect(toSet());
        Set<Answer> fullAnswers = parentQuery.stream().collect(toSet());
        Atom childAtom = ReasonerQueries.atomic(conjunction(childQuery.match().admin().getPattern()), graph).getAtom();
        Atom parentAtom = ReasonerQueries.atomic(conjunction(parentQuery.match().admin().getPattern()), graph).getAtom();

        MultiUnifier multiUnifier = childAtom.getMultiUnifier(childAtom, UnifierType.RULE);
        Set<Answer> permutedAnswers = answers.stream()
                .flatMap(a -> multiUnifier.stream().map(a::unify))
                .collect(Collectors.toSet());

        MultiUnifier multiUnifier2 = childAtom.getMultiUnifier(parentAtom, UnifierType.RULE);
        Set<Answer> permutedAnswers2 = answers.stream()
                .flatMap(a -> multiUnifier2.stream().map(a::unify))
                .collect(Collectors.toSet());

        assertEquals(fullAnswers, permutedAnswers2);
        assertEquals(answers, permutedAnswers);
    }

    @Test
    public void testWhenReifyingRelation_ExtraAtomIsCreatedWithUserDefinedName(){
        GraknTx graph = geoKB.tx();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{($x, $y) relates geo-entity;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        assertEquals(query.getAtom().isUserDefined(), false);
        assertEquals(query2.getAtom().isUserDefined(), true);
        assertEquals(query.getAtoms().size(), 1);
        assertEquals(query2.getAtoms().size(), 2);
    }

    @Test
    public void testWhenUnifiyingAtomWithItself_UnifierIsIdentity(){
        GraknTx graph = unificationWithTypesSet.tx();
        String patternString = "{$x1 isa entity1;$x2 isa entity2;($x1, $x2) isa binary;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern, graph);
        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        assertTrue(Sets.intersection(unifier.keySet(), Sets.newHashSet(var("x"), var("y"))).isEmpty());
    }

    @Test //only a single unifier exists
    public void testExactUnification_BinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa entity1;($x1, $x2) isa binary;}";
        String patternString2 = "{$y1 isa entity1;($y1, $y2) isa binary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);

        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier = new UnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
    }

    @Test //only a single unifier exists
    public void testExactUnification_BinaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa entity1;$x2 isa entity2;($x1, $x2) isa binary;}";
        String patternString2 = "{$y1 isa entity1;$y2 isa entity2;($y1, $y2) isa binary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);

        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier = new UnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
    }

    @Test
    public void testExactUnification_TernaryRelation_ParentRepeatsRoles(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String parentString = "{(role1: $x, role1: $y, role2: $z) isa ternary;}";
        String childString = "{(role1: $u, role2: $v, role3: $q) isa ternary;}";
        String childString2 = "{(role1: $u, role2: $v, role2: $q) isa ternary;}";
        String childString3 = "{(role1: $u, role1: $v, role2: $q) isa ternary;}";
        String childString4 = "{(role1: $u, role1: $u, role2: $q) isa ternary;}";
        Conjunction<VarPatternAdmin> parentPattern = conjunction(parentString, graph);
        Conjunction<VarPatternAdmin> childPattern = conjunction(childString, graph);
        Conjunction<VarPatternAdmin> childPattern2 = conjunction(childString2, graph);
        Conjunction<VarPatternAdmin> childPattern3 = conjunction(childString3, graph);
        Conjunction<VarPatternAdmin> childPattern4 = conjunction(childString4, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(parentPattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(childPattern, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(childPattern2, graph);
        ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(childPattern3, graph);
        ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(childPattern4, graph);

        MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery);
        MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery);

        assertTrue(emptyUnifier.isEmpty());
        assertTrue(emptyUnifier2.isEmpty());

        MultiUnifier unifier = childQuery3.getMultiUnifier(parentQuery);
        MultiUnifier correctUnifier = new MultiUnifierImpl(
                ImmutableMultimap.of(
                        var("u"), var("x"), 
                        var("v"), var("y"), 
                        var("q"), var("z")),
                ImmutableMultimap.of(
                        var("u"), var("y"),
                        var("v"), var("x"),
                        var("q"), var("z"))
        );
        assertTrue(unifier.containsAll(correctUnifier));
        assertEquals(unifier.size(), 2);

        Unifier unifier2 = childQuery4.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier2 = new UnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("u"), var("y"),
                var("q"), var("z")
        ));
        assertTrue(unifier2.containsAll(correctUnifier2));
    }

    @Test
    public void testExactUnification_TernaryRelation_ParentRepeatsMetaRoles(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String parentString = "{(role: $x, role: $y, role2: $z) isa ternary;}";
        String childString = "{(role1: $u, role2: $v, role3: $q) isa ternary;}";
        String childString2 = "{(role1: $u, role2: $v, role2: $q) isa ternary;}";
        String childString3 = "{(role1: $u, role1: $v, role2: $q) isa ternary;}";
        String childString4 = "{(role1: $u, role1: $u, role2: $q) isa ternary;}";
        Conjunction<VarPatternAdmin> parentPattern = conjunction(parentString, graph);
        Conjunction<VarPatternAdmin> childPattern = conjunction(childString, graph);
        Conjunction<VarPatternAdmin> childPattern2 = conjunction(childString2, graph);
        Conjunction<VarPatternAdmin> childPattern3 = conjunction(childString3, graph);
        Conjunction<VarPatternAdmin> childPattern4 = conjunction(childString4, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(parentPattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(childPattern, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(childPattern2, graph);
        ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(childPattern3, graph);
        ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(childPattern4, graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
        MultiUnifier correctUnifier = new MultiUnifierImpl(
                ImmutableMultimap.of(
                        var("u"), var("x"),
                        var("v"), var("z"),
                        var("q"), var("y")),
                ImmutableMultimap.of(
                        var("u"), var("y"),
                        var("v"), var("z"),
                        var("q"), var("x"))
        );
        assertTrue(unifier.containsAll(correctUnifier));
        assertEquals(unifier.size(), 2);

        MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery);
        MultiUnifier correctUnifier2 = new MultiUnifierImpl(
                ImmutableMultimap.of(
                        var("u"), var("x"),
                        var("v"), var("y"),
                        var("q"), var("z")),
                ImmutableMultimap.of(
                        var("u"), var("x"),
                        var("v"), var("z"),
                        var("q"), var("y")),
                ImmutableMultimap.of(
                        var("u"), var("y"),
                        var("v"), var("z"),
                        var("q"), var("x")),
                ImmutableMultimap.of(
                        var("u"), var("y"),
                        var("v"), var("x"),
                        var("q"), var("z"))
        );
        assertTrue(unifier2.containsAll(correctUnifier2));
        assertEquals(unifier2.size(), 4);

        MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery);
        MultiUnifier correctUnifier3 = new MultiUnifierImpl(
                ImmutableMultimap.of(
                        var("u"), var("x"),
                        var("v"), var("y"),
                        var("q"), var("z")),
                ImmutableMultimap.of(
                        var("u"), var("y"),
                        var("v"), var("x"),
                        var("q"), var("z"))
        );
        assertTrue(unifier3.containsAll(correctUnifier3));
        assertEquals(unifier3.size(), 2);

        Unifier unifier4 = childQuery4.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier4 = new UnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("u"), var("y"),
                var("q"), var("z")
        ));
        assertTrue(unifier4.containsAll(correctUnifier4));
    }

    @Test
    public void testExactUnification_TernaryRelation_ParentRepeatsRoles_ParentRepeatsRPs(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String parentString = "{(role1: $x, role1: $x, role2: $y) isa ternary;}";
        String childString = "{(role1: $u, role2: $v, role3: $q) isa ternary;}";
        String childString2 = "{(role1: $u, role2: $v, role2: $q) isa ternary;}";
        String childString3 = "{(role1: $u, role1: $v, role2: $q) isa ternary;}";
        String childString4 = "{(role1: $u, role1: $u, role2: $q) isa ternary;}";
        Conjunction<VarPatternAdmin> parentPattern = conjunction(parentString, graph);
        Conjunction<VarPatternAdmin> childPattern = conjunction(childString, graph);
        Conjunction<VarPatternAdmin> childPattern2 = conjunction(childString2, graph);
        Conjunction<VarPatternAdmin> childPattern3 = conjunction(childString3, graph);
        Conjunction<VarPatternAdmin> childPattern4 = conjunction(childString4, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(parentPattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(childPattern, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(childPattern2, graph);
        ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(childPattern3, graph);
        ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(childPattern4, graph);

        MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery);
        MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery);

        assertTrue(emptyUnifier.isEmpty());
        assertTrue(emptyUnifier2.isEmpty());

        Unifier unifier = childQuery3.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier = new UnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("v"), var("x"),
                var("q"), var("y")
        ));
        assertTrue(unifier.containsAll(correctUnifier));

        Unifier unifier2 = childQuery4.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier2 = new UnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("q"), var("y")
        ));
        assertTrue(unifier2.containsAll(correctUnifier2));
    }

    @Test
    public void testExactUnification_TernaryRelation_ParentRepeatsMetaRoles_ParentRepeatsRPs(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String parentString = "{(role: $x, role: $x, role2: $y) isa ternary;}";
        String childString = "{(role1: $u, role2: $v, role3: $q) isa ternary;}";
        String childString2 = "{(role1: $u, role2: $v, role2: $q) isa ternary;}";
        String childString3 = "{(role1: $u, role1: $v, role2: $q) isa ternary;}";
        String childString4 = "{(role1: $u, role1: $u, role2: $q) isa ternary;}";
        Conjunction<VarPatternAdmin> parentPattern = conjunction(parentString, graph);
        Conjunction<VarPatternAdmin> childPattern = conjunction(childString, graph);
        Conjunction<VarPatternAdmin> childPattern2 = conjunction(childString2, graph);
        Conjunction<VarPatternAdmin> childPattern3 = conjunction(childString3, graph);
        Conjunction<VarPatternAdmin> childPattern4 = conjunction(childString4, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(parentPattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(childPattern, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(childPattern2, graph);
        ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(childPattern3, graph);
        ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(childPattern4, graph);

        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier = new UnifierImpl(
                ImmutableMultimap.of(
                        var("q"), var("x"),
                        var("u"), var("x"),
                        var("v"), var("y"))
        );
        assertTrue(unifier.containsAll(correctUnifier));

        MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery);
        MultiUnifier correctUnifier2 = new MultiUnifierImpl(
                ImmutableMultimap.of(
                        var("u"), var("x"),
                        var("q"), var("x"),
                        var("v"), var("y")),
                ImmutableMultimap.of(
                        var("u"), var("x"),
                        var("v"), var("x"),
                        var("q"), var("y"))
        );
        assertTrue(unifier2.containsAll(correctUnifier2));
        assertEquals(unifier2.size(), 2);

        Unifier unifier3 = childQuery3.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier3 = new UnifierImpl(
                ImmutableMultimap.of(
                        var("u"), var("x"),
                        var("v"), var("x"),
                        var("q"), var("y"))
        );
        assertTrue(unifier3.containsAll(correctUnifier3));

        Unifier unifier4 = childQuery4.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier4 = new UnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("q"), var("y")
        ));
        assertTrue(unifier4.containsAll(correctUnifier4));
    }

    @Test
    public void testExactUnification_TernaryRelation_RepeatingRelationPlayers_withMetaRoles(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa entity3;$x3 isa entity5;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa entity5;$y1 isa entity3;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa entity5;$y2 isa entity4;$y1 isa entity3;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);

        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier unifier2 = childQuery2.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier = new UnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2"),
                var("y3"), var("x3")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
        assertTrue(unifier2.containsAll(correctUnifier));
    }

    @Test
    public void testExactUnification_TernaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa entity3;$x3 isa entity5;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa entity5;$y1 isa entity3;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa entity5;$y2 isa entity4;$y1 isa entity3;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);

        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier unifier2 = childQuery2.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier = new UnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2"),
                var("y3"), var("x3")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
        assertTrue(unifier2.containsAll(correctUnifier));
    }

    @Test
    public void testExactUnification_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa entity3;$x2 isa entity4; $x3 isa entity5;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa entity5;$y2 isa entity4;$y1 isa entity3;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa entity5;$y2 isa entity4;$y1 isa entity3;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);

        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier unifier2 = childQuery2.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier = new UnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2"),
                var("y3"), var("x3")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
        assertTrue(unifier2.containsAll(correctUnifier));
    }

    @Test // entity7 sub entity6 sub entity5
    public void testExactUnification_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes_TypeHierarchyInvolved(){
        GraknTx graph =  unificationWithTypesSet.tx();
        String parentString = "{$x1 isa entity5;$x2 isa entity6; $x3 isa entity7;($x1, $x2, $x3) isa ternary;}";

        String childString = "{$y3 isa entity7;$y2 isa entity6;$y1 isa entity5;($y2, $y3, $y1) isa ternary;}";
        String childString2 = "{$y3 isa entity7;$y2 isa entity6;$y1 isa entity5;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(parentString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(childString, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(childString2, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
        MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery);
        MultiUnifier correctUnifier = new MultiUnifierImpl(
                ImmutableMultimap.of(
                        var("y1"), var("x1"),
                        var("y2"), var("x2"),
                        var("y3"), var("x3")),
                ImmutableMultimap.of(
                        var("y1"), var("x1"),
                        var("y2"), var("x3"),
                        var("y3"), var("x2")),
                ImmutableMultimap.of(
                        var("y1"), var("x2"),
                        var("y2"), var("x1"),
                        var("y3"), var("x3")),
                ImmutableMultimap.of(
                        var("y1"), var("x2"),
                        var("y2"), var("x3"),
                        var("y3"), var("x1")),
                ImmutableMultimap.of(
                        var("y1"), var("x3"),
                        var("y2"), var("x1"),
                        var("y3"), var("x2")),
                ImmutableMultimap.of(
                        var("y1"), var("x3"),
                        var("y2"), var("x2"),
                        var("y3"), var("x1"))
        );
        assertTrue(unifier.containsAll(correctUnifier));
        assertTrue(unifier2.containsAll(correctUnifier));
    }

    @Test
    public void testEquivalence_DifferentIsaVariants(){
        testEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "isa", "entity1", "superEntity1");
    }

    @Test
    public void testEquivalence_DifferentSubVariants(){
        testEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "sub", "entity1", "role1");
    }

    @Test
    public void testEquivalence_DifferentPlaysVariants(){
        testEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "plays", "role1", "role2");
    }

    @Test
    public void testEquivalence_DifferentRelatesVariants(){
        testEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "relates", "role1", "role2");
    }

    @Test
    public void testEquivalence_DifferentHasVariants(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$x has res1;}";
        String patternString2 = "{$y has res1;}";
        String patternString3 = "{$x has " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + ";}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);

        ReasonerAtomicQuery query =ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 =ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 =ReasonerQueries.atomic(pattern3, graph);

        queryEquivalence(query, query2, true);
        queryEquivalence(query, query3, false);
        queryEquivalence(query2, query3, false);
    }

    private void testEquivalence_DifferentTypeVariants(GraknTx graph, String keyword, String label, String label2){
        String patternString = "{$x " + keyword + " " + label + ";}";
        String patternString2 = "{$y " + keyword + " $type;$type label " + label +";}";
        String patternString3 = "{$z " + keyword + " $t;$t label " + label +";}";
        String patternString4 = "{$x " + keyword + " $y;}";
        String patternString5 = "{$x " + keyword + " " + label2 + ";}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);
        Conjunction<VarPatternAdmin> pattern5 = conjunction(patternString5, graph);

        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 = ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 = ReasonerQueries.atomic(pattern4, graph);
        ReasonerAtomicQuery query5 = ReasonerQueries.atomic(pattern5, graph);

        queryEquivalence(query, query2, true);
        queryEquivalence(query, query3, true);
        queryEquivalence(query, query4, false);
        queryEquivalence(query, query5, false);

        queryEquivalence(query2, query3, true);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query2, query5, false);

        queryEquivalence(query3, query4, false);
        queryEquivalence(query3, query5, false);

        queryEquivalence(query4, query5, false);
    }

    @Test
    public void testEquivalence_TypesWithSameLabel(){
        GraknTx graph = unificationTestSet.tx();
        String isaPatternString = "{$x isa entity1;}";
        String subPatternString = "{$x sub entity1;}";

        String playsPatternString = "{$x plays role1;}";
        String relatesPatternString = "{$x relates role1;}";
        String hasPatternString = "{$x has role1;}";
        String subPatternString2 = "{$x sub role1;}";

        Conjunction<VarPatternAdmin> isaPattern = conjunction(isaPatternString, graph);
        Conjunction<VarPatternAdmin> subPattern = conjunction(subPatternString, graph);
        Conjunction<VarPatternAdmin> subPattern2 = conjunction(subPatternString2, graph);
        Conjunction<VarPatternAdmin> playsPattern = conjunction(playsPatternString, graph);
        Conjunction<VarPatternAdmin> hasPattern = conjunction(hasPatternString, graph);
        Conjunction<VarPatternAdmin> relatesPattern = conjunction(relatesPatternString, graph);

        ReasonerAtomicQuery isaQuery = ReasonerQueries.atomic(isaPattern, graph);
        ReasonerAtomicQuery subQuery = ReasonerQueries.atomic(subPattern, graph);
        ReasonerAtomicQuery playsQuery = ReasonerQueries.atomic(playsPattern, graph);
        ReasonerAtomicQuery relatesQuery = ReasonerQueries.atomic(relatesPattern, graph);
        ReasonerAtomicQuery hasQuery = ReasonerQueries.atomic(hasPattern, graph);
        ReasonerAtomicQuery subQuery2 = ReasonerQueries.atomic(subPattern2, graph);

        queryEquivalence(isaQuery, subQuery, false);
        queryEquivalence(isaQuery, playsQuery, false);
        queryEquivalence(isaQuery, relatesQuery, false);
        queryEquivalence(isaQuery, hasQuery, false);
        queryEquivalence(isaQuery, subQuery2, false);

        queryEquivalence(subQuery, playsQuery, false);
        queryEquivalence(subQuery, relatesQuery, false);
        queryEquivalence(subQuery, hasQuery, false);
        queryEquivalence(subQuery, subQuery2, false);

        queryEquivalence(playsQuery, relatesQuery, false);
        queryEquivalence(playsQuery, hasQuery, false);
        queryEquivalence(playsQuery, subQuery2, false);

        queryEquivalence(relatesQuery, hasQuery, false);
        queryEquivalence(relatesQuery, subQuery2, false);

        queryEquivalence(hasQuery, subQuery2, false);
    }

    @Test
    public void testEquivalence_TypesWithSubstitution(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$y isa entity1;}";
        String patternString2 = "{$x isa entity1; $x id 'X';}";
        String patternString3 = "{$a isa entity1; $b id 'X';}";
        String patternString4 = "{$z isa entity1; $z id 'Y';}";
        String patternString5 = "{$r isa entity1; $r id 'X';}";
        String patternString6 = "{$e isa $t;$t label entity1;$e id 'X';}";
        String patternString7 = "{$e isa entity ; $e id 'X';}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);
        Conjunction<VarPatternAdmin> pattern5 = conjunction(patternString5, graph);
        Conjunction<VarPatternAdmin> pattern6 = conjunction(patternString6, graph);
        Conjunction<VarPatternAdmin> pattern7 = conjunction(patternString7, graph);

        ReasonerAtomicQuery query =ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 =ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 =ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 =ReasonerQueries.atomic(pattern4, graph);
        ReasonerAtomicQuery query5 =ReasonerQueries.atomic(pattern5, graph);
        ReasonerAtomicQuery query6 =ReasonerQueries.atomic(pattern6, graph);
        ReasonerAtomicQuery query7 =ReasonerQueries.atomic(pattern7, graph);

        queryEquivalence(query, query2, false);
        queryEquivalence(query, query3, false, true, false);
        queryEquivalence(query, query4, false);
        queryEquivalence(query, query5, false);
        queryEquivalence(query, query6, false);
        queryEquivalence(query, query7, false);

        queryEquivalence(query2, query3, false);
        queryEquivalence(query2, query4, false, true);
        queryEquivalence(query2, query5, true);
        queryEquivalence(query2, query6, true);
        queryEquivalence(query2, query7, false);

        queryEquivalence(query3, query4, false);
        queryEquivalence(query3, query5, false);
        queryEquivalence(query3, query6, false);
        queryEquivalence(query3, query7, false);

        queryEquivalence(query4, query5, false, true);
        queryEquivalence(query4, query6, false, true);
        queryEquivalence(query4, query7, false);

        queryEquivalence(query5, query6, true);
        queryEquivalence(query5, query7, false);

        queryEquivalence(query6, query7, false);
    }

    @Test
    public void testEquivalence_DifferentResourceVariants(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$x has res1 'value';}";
        String patternString2 = "{$y has res1 $r;$r val 'value';}";
        String patternString3 = "{$y has res1 $r;}";
        String patternString4 = "{$y has res1 'value2';}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);

        ReasonerAtomicQuery query =ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 =ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 =ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 =ReasonerQueries.atomic(pattern4, graph);

        queryEquivalence(query, query2, true);
        queryEquivalence(query, query3, false);
        queryEquivalence(query, query4, false);
        queryEquivalence(query2, query3, false);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query3, query4, false);
    }

    @Test
    public void testEquivalence_ResourcesWithSubstitution(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$x has res1 $y;}";
        String patternString2 = "{$y has res1 $z; $y id 'X';}";
        String patternString3 = "{$z has res1 $u; $z id 'Y';}";

        String patternString4 = "{$y has res1 $r;$r id 'X';}";
        String patternString5 = "{$r has res1 $x;$x id 'X';}";
        String patternString6 = "{$y has res1 $x;$x id 'Y';}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);
        Conjunction<VarPatternAdmin> pattern5 = conjunction(patternString5, graph);
        Conjunction<VarPatternAdmin> pattern6 = conjunction(patternString6, graph);

        ReasonerAtomicQuery query =ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 =ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 =ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 =ReasonerQueries.atomic(pattern4, graph);
        ReasonerAtomicQuery query5 =ReasonerQueries.atomic(pattern5, graph);
        ReasonerAtomicQuery query6 =ReasonerQueries.atomic(pattern6, graph);

        queryEquivalence(query, query2, false);
        queryEquivalence(query, query3, false);
        queryEquivalence(query, query4, false);
        queryEquivalence(query, query5, false);
        queryEquivalence(query, query6, false);

        queryEquivalence(query2, query3, false, false, true);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query2, query5, false);
        queryEquivalence(query2, query6, false);

        queryEquivalence(query3, query4, false);
        queryEquivalence(query3, query5, false);
        queryEquivalence(query3, query6, false);

        queryEquivalence(query4, query5, true);
        queryEquivalence(query4, query6, false, false, true);

        queryEquivalence(query5, query6, false, false, true);
    }

    @Test //tests alpha-equivalence of queries with resources with multi predicate
    public void testEquivalence_MultiPredicateResources(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$z has res1 $u;$a val >23; $a val <27;}";
        String patternString2 = "{$x isa entity1;$x has res1 $a;$a val >23; $a val <27;}";
        String patternString3 = "{$e isa entity1;$e has res1 > 23;}";
        String patternString4 = "{$p isa entity1;$p has res1 $a;$a val >23;}";
        String patternString5 = "{$x isa entity1;$x has res1 $y;$y val >27;$y val <23;}";
        String patternString6 = "{$a isa entity1;$a has res1 $p;$p val <27;$p val >23;}";
        String patternString7 = "{$x isa entity1, has res1 $a;$a val >23; $a val <27;}";
        String patternString8 = "{$x isa entity1, has res1 $z1;$z1 val >23; $z2 val <27;}";
        String patternString9 = "{$x isa $type;$type label entity1;$x has res1 $a;$a val >23; $a val <27;}";

        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        ReasonerAtomicQuery query3 = ReasonerQueries.atomic(conjunction(patternString3, graph), graph);
        ReasonerAtomicQuery query4 = ReasonerQueries.atomic(conjunction(patternString4, graph), graph);
        ReasonerAtomicQuery query5 = ReasonerQueries.atomic(conjunction(patternString5, graph), graph);
        ReasonerAtomicQuery query6 = ReasonerQueries.atomic(conjunction(patternString6, graph), graph);
        ReasonerAtomicQuery query7 = ReasonerQueries.atomic(conjunction(patternString7, graph), graph);
        ReasonerAtomicQuery query8 = ReasonerQueries.atomic(conjunction(patternString8, graph), graph);
        ReasonerAtomicQuery query9 = ReasonerQueries.atomic(conjunction(patternString9, graph), graph);

        queryEquivalence(query, query2, false);
        queryEquivalence(query, query3, false);
        queryEquivalence(query, query4, false);
        queryEquivalence(query, query5, false);
        queryEquivalence(query, query6, false);
        queryEquivalence(query, query7, false);
        queryEquivalence(query, query8, false);
        queryEquivalence(query, query9, false);

        queryEquivalence(query2, query3, false);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query2, query5, false);
        queryEquivalence(query2, query6, true);
        queryEquivalence(query2, query7, true);
        queryEquivalence(query2, query8, false);
        queryEquivalence(query2, query9, true);

        queryEquivalence(query3, query4, true);
        queryEquivalence(query3, query5, false);
        queryEquivalence(query3, query6, false);
        queryEquivalence(query3, query7, false);
        queryEquivalence(query3, query8, false, true, false);
        queryEquivalence(query3, query9, false);

        queryEquivalence(query4, query5, false);
        queryEquivalence(query4, query6, false);
        queryEquivalence(query4, query7, false);
        queryEquivalence(query4, query8, false, true, false);
        queryEquivalence(query4, query9, false);

        queryEquivalence(query5, query6, false);
        queryEquivalence(query5, query7, false);
        queryEquivalence(query5, query8, false);
        queryEquivalence(query5, query9, false);

        queryEquivalence(query6, query7, true);
        queryEquivalence(query6, query8, false);
        queryEquivalence(query6, query9, true);

        queryEquivalence(query7, query8, false);
        queryEquivalence(query7, query9, true);
    }

    @Test //tests alpha-equivalence of resource atoms with different predicates
    public void testEquivalence_resourcesWithDifferentPredicates() {
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$x has res1 $r;$r val > 1099;}";
        String patternString2 = "{$x has res1 $r;$r val < 1099;}";
        String patternString3 = "{$x has res1 $r;$r val = 1099;}";
        String patternString4 = "{$x has res1 $r;$r val '1099';}";
        String patternString5 = "{$x has res1 $r;$r val > $var;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);
        Conjunction<VarPatternAdmin> pattern5 = conjunction(patternString5, graph);

        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 =ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 =ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 =ReasonerQueries.atomic(pattern4, graph);
        ReasonerAtomicQuery query5 =ReasonerQueries.atomic(pattern5, graph);

        queryEquivalence(query, query2, false);
        queryEquivalence(query, query3, false);
        queryEquivalence(query, query4, false);
        queryEquivalence(query, query5, false);

        queryEquivalence(query2, query3, false);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query2, query5, false);

        queryEquivalence(query3, query4, true);
        queryEquivalence(query3, query5, false);

        queryEquivalence(query4, query5, false);
    }

    @Test
    public void testEquivalence_DifferentRelationInequivalentVariants(){
        GraknTx graph = unificationTestSet.tx();

        HashSet<String> patternStrings = Sets.newHashSet(
                "{$x isa relation1;}",
                "{($y) isa relation1;}",

                "{($x, $y);}",
                "{($x, $y) isa relation1;}",
                "{(role1: $x, role2: $y) isa relation1;}",
                "{(role: $y, role2: $z) isa relation1;}",
                "{(role: $x, role: $x, role2: $z) isa relation1;}",

                "{$x ($y, $z) isa relation1;}",
                "{$x (role1: $y, role2: $z) isa relation1;}"
        );

        Set<ReasonerAtomicQuery> atoms = patternStrings.stream()
                .map(s -> conjunction(s, graph))
                .map(p -> ReasonerQueries.atomic(p, graph))
                .collect(toSet());

        atoms.forEach(at -> {
            atoms.stream()
                    .filter(a -> a != at)
                    .forEach(a -> queryEquivalence(a, at, false, false));
        });
    }

    @Test
    public void testEquivalence_RelationWithRepeatingVariables(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{(role1: $x, role2: $y);}";
        String patternString2 = "{(role1: $x, role2: $x);}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);

        ReasonerAtomicQuery query =ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 =ReasonerQueries.atomic(pattern2, graph);
        queryEquivalence(query, query2, false, false);
    }

    @Test
    public void testEquivalence_RelationsWithTypedRolePlayers(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{(role: $x, role: $y);$x isa entity1;}";
        String patternString2 = "{(role: $x, role: $y);$y isa entity1;}";
        String patternString3 = "{(role: $x, role: $y);$x isa superEntity1;}";
        String patternString4 = "{(role: $x, role: $y);$y isa entity1;$x isa entity1;}";
        String patternString5 = "{(role1: $x, role2: $y);$x isa entity1;}";
        String patternString6 = "{(role1: $x, role2: $y);$y isa entity1;}";
        String patternString7 = "{(role1: $x, role2: $y);$x isa entity1;$y isa superEntity1;}";
        String patternString8 = "{(role1: $x, role2: $y);$x isa entity1;$y isa entity1;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);
        Conjunction<VarPatternAdmin> pattern5 = conjunction(patternString5, graph);
        Conjunction<VarPatternAdmin> pattern6 = conjunction(patternString6, graph);
        Conjunction<VarPatternAdmin> pattern7 = conjunction(patternString7, graph);
        Conjunction<VarPatternAdmin> pattern8 = conjunction(patternString8, graph);

        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 = ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 = ReasonerQueries.atomic(pattern4, graph);
        ReasonerAtomicQuery query5 = ReasonerQueries.atomic(pattern5, graph);
        ReasonerAtomicQuery query6 = ReasonerQueries.atomic(pattern6, graph);
        ReasonerAtomicQuery query7 = ReasonerQueries.atomic(pattern7, graph);
        ReasonerAtomicQuery query8 = ReasonerQueries.atomic(pattern8, graph);

        queryEquivalence(query, query2, true);
        queryEquivalence(query, query3, false);
        queryEquivalence(query, query4, false);
        queryEquivalence(query, query5, false);
        queryEquivalence(query, query6, false);
        queryEquivalence(query, query7, false);
        queryEquivalence(query, query8, false);

        queryEquivalence(query2, query3, false);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query2, query5, false);
        queryEquivalence(query2, query6, false);
        queryEquivalence(query2, query7, false);
        queryEquivalence(query2, query8, false);

        queryEquivalence(query3, query4, false);
        queryEquivalence(query3, query5, false);
        queryEquivalence(query3, query6, false);
        queryEquivalence(query3, query7, false);
        queryEquivalence(query3, query8, false);

        queryEquivalence(query4, query5, false);
        queryEquivalence(query4, query6, false);
        queryEquivalence(query4, query7, false);
        queryEquivalence(query4, query8, false);

        queryEquivalence(query5, query6, false);
        queryEquivalence(query5, query7, false);
        queryEquivalence(query5, query8, false);

        queryEquivalence(query6, query7, false);
        queryEquivalence(query6, query8, false);

        queryEquivalence(query7, query8, false);
    }

    @Test
    public void testEquivalence_RelationsWithSubstitution(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{(role: $x, role: $y);$x id 'V666';}";
        String patternString2 = "{(role: $x, role: $y);$y id 'V666';}";
        String patternString3 = "{(role: $x, role: $y);$x id 'V666';$y id 'V667';}";
        String patternString4 = "{(role: $x, role: $y);$y id 'V666';$x id 'V667';}";
        String patternString5 = "{(role1: $x, role2: $y);$x id 'V666';$y id 'V667';}";
        String patternString6 = "{(role1: $x, role2: $y);$y id 'V666';$x id 'V667';}";
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

        queryEquivalence(query, query2, true, true);
        queryEquivalence(query, query3, false, false);
        queryEquivalence(query, query4, false, false);
        queryEquivalence(query, query5, false, false);
        queryEquivalence(query, query6, false, false);
        queryEquivalence(query, query7, false, false);

        queryEquivalence(query2, query3, false, false);
        queryEquivalence(query2, query4, false, false);
        queryEquivalence(query2, query5, false, false);
        queryEquivalence(query2, query6, false, false);
        queryEquivalence(query2, query7, false, false);

        queryEquivalence(query3, query4, true, true);
        queryEquivalence(query3, query5, false, false);
        queryEquivalence(query3, query6, false, false);
        queryEquivalence(query3, query7, false, true);

        queryEquivalence(query4, query5, false, false);
        queryEquivalence(query4, query6, false, false);
        queryEquivalence(query4, query7, false, true);

        queryEquivalence(query5, query6, false, true);
        queryEquivalence(query5, query7, false, false);

        queryEquivalence(query6, query7, false, false);
    }

    @Test
    public void testEquivalence_RelationsWithSubstitution_differentRolesMapped(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{(role1: $x, role2: $y);$x id 'V666';}";
        String patternString2 = "{(role1: $x, role2: $y);$x id 'V667';}";
        String patternString3 = "{(role1: $x, role2: $y);$y id 'V666';}";
        String patternString4 = "{(role1: $x, role2: $y);$y id 'V667';}";
        String patternString5 = "{(role1: $x, role2: $y);$x id 'V666';$y id 'V667';}";
        String patternString6 = "{(role1: $x, role2: $y);$y id 'V666';$x id 'V667';}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);
        Conjunction<VarPatternAdmin> pattern5 = conjunction(patternString5, graph);
        Conjunction<VarPatternAdmin> pattern6 = conjunction(patternString6, graph);

        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 = ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 = ReasonerQueries.atomic(pattern4, graph);
        ReasonerAtomicQuery query5 = ReasonerQueries.atomic(pattern5, graph);
        ReasonerAtomicQuery query6 = ReasonerQueries.atomic(pattern6, graph);

        queryEquivalence(query, query2, false, true);
        queryEquivalence(query, query3, false, false);
        queryEquivalence(query, query4, false, false);
        queryEquivalence(query, query5, false, false);
        queryEquivalence(query, query6, false, false);

        queryEquivalence(query2, query3, false, false);
        queryEquivalence(query2, query4, false, false);
        queryEquivalence(query2, query5, false, false);
        queryEquivalence(query2, query6, false, false);

        queryEquivalence(query3, query4, false, true);
        queryEquivalence(query3, query5, false, false);
        queryEquivalence(query3, query6, false, false);

        queryEquivalence(query4, query5, false, false);
        queryEquivalence(query4, query6, false, false);

        queryEquivalence(query5, query6, false, true);
    }

    @Test
    public void traversalTest(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{(role1: $x, role2: $y);$x id 'a';$y id 'b';}";
        String patternString2 = "{(role1: $x, role2: $y);$y id 'c';$x id 'd';}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);

        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);

        GreedyTraversalPlan.createTraversal(query.getPattern(), graph);
        GreedyTraversalPlan.createTraversal(query2.getPattern(), graph);
        System.out.println();
    }

    private void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean expectation){
        queryEquivalence(a, b, expectation, expectation, expectation);
    }

    private void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean expectation, boolean structuralExpectation){
        queryEquivalence(a, b, expectation, expectation, structuralExpectation);
    }

    private void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, Equivalence<ReasonerQueryImpl> equiv){
        assertEquals("Query: " + a.toString() + " =? " + b.toString(), equiv.equivalent(a, b), queryExpectation);
        assertEquals("Query: " + b.toString() + " =? " + a.toString(), equiv.equivalent(b, a), queryExpectation);

        //check hash additionally if need to be equal
        if (queryExpectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), equiv.hash(a) == equiv.hash(b), true);
        }
    }

    private void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, boolean atomExpectation, boolean structuralExpectation){
        queryEquivalence(a, b, queryExpectation, QueryEquivalence.AlphaEquivalence);
        queryEquivalence(a, b, structuralExpectation, QueryEquivalence.StructuralEquivalence);
        atomicEquivalence(a.getAtom(), b.getAtom(), atomExpectation);
    }

    private void atomicEquivalence(Atomic a, Atomic b, boolean expectation){
        assertEquals("Atom: " + a.toString() + " =? " + b.toString(), a.isAlphaEquivalent(b), expectation);
        assertEquals("Atom: " + b.toString() + " =? " + a.toString(), b.isAlphaEquivalent(a), expectation);

        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), a.alphaEquivalenceHashCode() == b.alphaEquivalenceHashCode(), true);
        }
    }

    private Conjunction<VarPatternAdmin> conjunction(PatternAdmin pattern){
        Set<VarPatternAdmin> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

}
