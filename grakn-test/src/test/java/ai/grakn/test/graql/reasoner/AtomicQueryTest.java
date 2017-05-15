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
import ai.grakn.graphs.AdmissionsGraph;
import ai.grakn.graphs.CWGraph;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graphs.SNBGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.test.GraphContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.entityTypeFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.permuteFunction;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.subFilter;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicQueryTest {

    @ClassRule
    public static final GraphContext snbGraph = GraphContext.preLoad(SNBGraph.get()).assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get()).assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext admissionsGraph = GraphContext.preLoad(AdmissionsGraph.get()).assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext cwGraph = GraphContext.preLoad(CWGraph.get()).assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext ancestorGraph = GraphContext.preLoad("ancestor-friend-test.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext unificationWithTypesSet = GraphContext.preLoad("unificationWithTypesTest.gql").assumeTrue(usingTinker());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() {
        assumeTrue(usingTinker());
    }

    @Test
    public void testWhenConstructingNonAtomicQuery_ExceptionIsThrown() {
        String patternString = "{$x isa person;$y isa product;($x, $y) isa recommendation;($y, $t) isa typing;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, snbGraph.graph());
        exception.expect(IllegalStateException.class);
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, snbGraph.graph());
    }

    @Test
    public void testWhenCopying_TheCopyIsAlphaEquivalent(){
        String patternString = "{($x, $y) isa recommendation;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, snbGraph.graph());
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, snbGraph.graph());
        ReasonerAtomicQuery copy = ReasonerQueries.atomic(atomicQuery);
        assertEquals(atomicQuery, copy);
        assertEquals(atomicQuery.hashCode(), copy.hashCode());
    }

    @Test
    public void testWhenModifyingAQuery_TheCopyDoesNotChange(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{(recommended-product: $x, recommended-customer: $y) isa recommendation;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery copy = ReasonerQueries.atomic(atomicQuery);

        atomicQuery.unify(VarName.of("y"), VarName.of("z"));
        MatchQuery q1 = atomicQuery.getMatchQuery();
        MatchQuery q2 = copy.getMatchQuery();
        assertNotEquals(q1, q2);
    }

    @Test
    public void testWhenCopyingAQuery_TheyHaveTheSameRoleVarTypeMaps(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{(recommended-product: $x, recommended-customer: $y) isa recommendation;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery copy = ReasonerQueries.atomic(atomicQuery);

        atomicQuery.unify(VarName.of("y"), VarName.of("z"));
        assertEquals(ReasonerQueries.atomic(conjunction(patternString, graph), snbGraph.graph()).getAtom().getRoleVarTypeMap(), copy.getAtom().getRoleVarTypeMap());
    }

    @Test
    public void testWhenMaterialising_MaterialisedInformationIsPresentInGraph(){
        GraknGraph graph = snbGraph.graph();
        QueryBuilder qb = graph.graql().infer(false);
        String explicitQuery = "match (recommended-customer: $x, recommended-product: $y) isa recommendation;$x has name 'Bob';$y has name 'Colour of Magic';";
        assertTrue(!qb.<MatchQuery>parse(explicitQuery).ask().execute());

        String patternString = "{(recommended-customer: $x, recommended-product: $y) isa recommendation;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        QueryAnswers answers = new QueryAnswers();

        answers.add(new QueryAnswer(
                ImmutableMap.of(
                        VarName.of("x"), getConcept("Bob"),
                        VarName.of("y"), getConcept("Colour of Magic")))
        );
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);

        assertFalse(qb.<MatchQuery>parse(explicitQuery).ask().execute());
        answers.stream().flatMap(atomicQuery::materialise).collect(Collectors.toList());
        assertTrue(qb.<MatchQuery>parse(explicitQuery).ask().execute());
    }

    @Test
    public void testWhenRoleTypesAreAmbiguous_answersArePermutedCorrectly(){
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        String queryString2 = "match ($x, $y) isa is-located-in;";
        GraknGraph graph = geoGraph.graph();
        QueryBuilder qb = graph.graql().infer(false);
        MatchQuery query = qb.parse(queryString);
        MatchQuery query2 = qb.parse(queryString2);
        Set<Answer> answers = query.admin().stream().collect(toSet());
        Set<Answer> fullAnswers = query2.admin().stream().collect(toSet());
        Atom mappedAtom = ReasonerQueries.atomic(conjunction(query.admin().getPattern()), graph).getAtom();
        Atom unmappedAtom = ReasonerQueries.atomic(conjunction(query2.admin().getPattern()), graph).getAtom();

        Set<Unifier> permutationUnifiers = mappedAtom.getPermutationUnifiers(mappedAtom);
        Set<IdPredicate> unmappedIdPredicates = mappedAtom.getUnmappedIdPredicates();
        Set<TypeAtom> unmappedTypeConstraints = mappedAtom.getUnmappedTypeConstraints();
        Set<Answer> permutedAnswers = answers.stream()
                .flatMap(a -> permuteFunction.apply(a, permutationUnifiers))
                .filter(a -> subFilter(a, unmappedIdPredicates))
                .filter(a -> entityTypeFilter(a, unmappedTypeConstraints))
                .collect(Collectors.toSet());

        Set<Unifier> permutationUnifiers2 = unmappedAtom.getPermutationUnifiers(mappedAtom);
        Set<IdPredicate> unmappedIdPredicates2 = unmappedAtom.getUnmappedIdPredicates();
        Set<TypeAtom> unmappedTypeConstraints2 = unmappedAtom.getUnmappedTypeConstraints();
        Set<Answer> permutedAnswers2 = answers.stream()
                .flatMap(a -> permuteFunction.apply(a, permutationUnifiers2))
                .filter(a -> subFilter(a, unmappedIdPredicates2))
                .filter(a -> entityTypeFilter(a, unmappedTypeConstraints2))
                .collect(Collectors.toSet());

        assertEquals(fullAnswers, permutedAnswers2);
        assertEquals(answers, permutedAnswers);
    }

    @Test
    public void testWhenReifyingRelation_ExtraAtomIsCreatedWithUserDefinedName(){
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{($x, $y) relates geo-entity;}";
        GraknGraph graph = geoGraph.graph();
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        assertEquals(query.getAtom().isUserDefinedName(), false);
        assertEquals(query2.getAtom().isUserDefinedName(), true);
        assertEquals(query.getAtoms().size(), 1);
        assertEquals(query2.getAtoms().size(), 2);
    }

    @Test //basic unification test based on mapping variables to corresponding roles together with checking copied atom is not affected
    public void testWhenUnifiying_CopyIsNotAffected(){
        GraknGraph graph = geoGraph.graph();
        String parentString = "{(entity-location: $y, geo-entity: $y1), isa is-located-in;}";
        String childString = "{(geo-entity: $y1, entity-location: $y2), isa is-located-in;}";

        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction(parentString, graph), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(childString, graph), graph);

        Atomic childAtom = childQuery.getAtom();
        Atomic parentAtom = parentQuery.getAtom();

        Unifier unifiers = childAtom.getUnifier(parentAtom);

        ReasonerAtomicQuery childCopy = ReasonerQueries.atomic(childQuery);
        childCopy.unify(unifiers);
        Atomic childAtomCopy = childCopy.getAtom();
        assertNotEquals(childAtomCopy, childAtom);
    }

    @Test
    public void testWhenUnifiyingAtomWithItself_UnifierIsTrivial(){
        String patternString = "{$x isa country;($x, $y) isa is-enemy-of;$y isa country;}";
        GraknGraph graph = cwGraph.graph();
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        assertTrue(Sets.intersection(unifier.keySet(), Sets.newHashSet(VarName.of("x"), VarName.of("y"))).isEmpty());
    }

    @Test
    public void testWhenUnifiyingBinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        GraknGraph graph =  unificationWithTypesSet.graph();
        String patternString = "{$x1 isa entity1;($x1, $x2) isa binary;}";
        String patternString2 = "{$y1 isa entity1;($y1, $y2) isa binary;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                VarName.of("y1"), VarName.of("x1"),
                VarName.of("y2"), VarName.of("x2")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
    }

    @Test
    public void testWhenUnifiyingBinaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        GraknGraph graph =  unificationWithTypesSet.graph();
        String patternString = "{$x1 isa entity1;$x2 isa entity2;($x1, $x2) isa binary;}";
        String patternString2 = "{$y1 isa entity1;$y2 isa entity2;($y1, $y2) isa binary;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                VarName.of("y1"), VarName.of("x1"),
                VarName.of("y2"), VarName.of("x2")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
    }

    @Test
    public void testWhenUnifiyingTernaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        GraknGraph graph =  unificationWithTypesSet.graph();
        String patternString = "{$x1 isa entity3;$x3 isa entity5;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa entity5;$y1 isa entity3;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa entity5;$y2 isa entity4;$y1 isa entity3;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier unifier2 = childQuery2.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                VarName.of("y1"), VarName.of("x1"),
                VarName.of("y2"), VarName.of("x2"),
                VarName.of("y3"), VarName.of("x3")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
        assertTrue(unifier2.containsAll(correctUnifier));
    }

    @Test
    public void testWhenUnifiyingTernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        GraknGraph graph =  unificationWithTypesSet.graph();
        String patternString = "{$x1 isa entity3;$x2 isa entity4; $x3 isa entity5;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa entity5;$y2 isa entity4;$y1 isa entity3;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa entity5;$y2 isa entity4;$y1 isa entity3;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier unifier2 = childQuery2.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                VarName.of("y1"), VarName.of("x1"),
                VarName.of("y2"), VarName.of("x2"),
                VarName.of("y3"), VarName.of("x3")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
        assertTrue(unifier2.containsAll(correctUnifier));
    }

    @Test
    public void testWhenUnifiyingTernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes_TypeHierarchyInvolved(){
        GraknGraph graph =  unificationWithTypesSet.graph();
        String patternString = "{$x1 isa entity5;$x2 isa entity6; $x3 isa entity7;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa entity7;$y2 isa entity6;$y1 isa entity5;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa entity7;$y2 isa entity6;$y1 isa entity5;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier unifier2 = childQuery2.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                VarName.of("y1"), VarName.of("x1"),
                VarName.of("y2"), VarName.of("x2"),
                VarName.of("y3"), VarName.of("x3")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
        assertTrue(unifier2.containsAll(correctUnifier));
    }

    private Conjunction<VarAdmin> conjunction(PatternAdmin pattern){
        Set<VarAdmin> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private Concept getConcept(String id){
        Set<Concept> instances = snbGraph.graph().getResourcesByValue(id)
                .stream().flatMap(res -> res.ownerInstances().stream()).collect(Collectors.toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        return instances.iterator().next();
    }

}
