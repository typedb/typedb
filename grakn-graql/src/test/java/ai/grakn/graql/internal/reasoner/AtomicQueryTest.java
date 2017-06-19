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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.AdmissionsGraph;
import ai.grakn.test.graphs.CWGraph;
import ai.grakn.test.graphs.GeoGraph;
import ai.grakn.test.graphs.SNBGraph;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.entityTypeFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.subFilter;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicQueryTest {

    @ClassRule
    public static final GraphContext snbGraph = GraphContext.preLoad(SNBGraph.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext admissionsGraph = GraphContext.preLoad(AdmissionsGraph.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext cwGraph = GraphContext.preLoad(CWGraph.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext ancestorGraph = GraphContext.preLoad("ancestor-friend-test.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext unificationWithTypesSet = GraphContext.preLoad("unificationWithTypesTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() {
        assumeTrue(GraknTestSetup.usingTinker());
    }

    @Test
    public void testWhenConstructingNonAtomicQuery_ExceptionIsThrown() {
        String patternString = "{$x isa person;$y isa product;($x, $y) isa recommendation;($y, $t) isa typing;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, snbGraph.graph());
        exception.expect(GraqlQueryException.class);
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, snbGraph.graph());
    }

    @Test
    public void testWhenCopying_TheCopyIsAlphaEquivalent(){
        String patternString = "{($x, $y) isa recommendation;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, snbGraph.graph());
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, snbGraph.graph());
        ReasonerAtomicQuery copy = ReasonerQueries.atomic(atomicQuery);
        assertEquals(atomicQuery, copy);
        assertEquals(atomicQuery.hashCode(), copy.hashCode());
    }

    @Test
    public void testAlphaEquivalence_RepeatingVariables(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{(recommended-customer: $x, recommended-product: $y);}";
        String patternString2 = "{(recommended-customer: $x, recommended-product: $x);}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);

        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        assertNotEquals(query, query2);
    }

    @Test
    public void testWhenMaterialising_MaterialisedInformationIsPresentInGraph(){
        GraknGraph graph = snbGraph.graph();
        QueryBuilder qb = graph.graql().infer(false);
        String explicitQuery = "match (recommended-customer: $x, recommended-product: $y) isa recommendation;$x has name 'Bob';$y has name 'Colour of Magic';";
        assertTrue(!qb.<MatchQuery>parse(explicitQuery).ask().execute());

        String patternString = "{(recommended-customer: $x, recommended-product: $y) isa recommendation;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        QueryAnswers answers = new QueryAnswers();

        answers.add(new QueryAnswer(
                ImmutableMap.of(
                        Graql.var("x"), getConcept("Bob"),
                        Graql.var("y"), getConcept("Colour of Magic")))
        );
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);

        assertFalse(qb.<MatchQuery>parse(explicitQuery).ask().execute());
        answers.stream().forEach(atomicQuery::materialise);
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
                .flatMap(a -> a.permute(permutationUnifiers))
                .filter(a -> subFilter(a, unmappedIdPredicates))
                .filter(a -> entityTypeFilter(a, unmappedTypeConstraints))
                .collect(Collectors.toSet());

        Set<Unifier> permutationUnifiers2 = unmappedAtom.getPermutationUnifiers(mappedAtom);
        Set<IdPredicate> unmappedIdPredicates2 = unmappedAtom.getUnmappedIdPredicates();
        Set<TypeAtom> unmappedTypeConstraints2 = unmappedAtom.getUnmappedTypeConstraints();
        Set<Answer> permutedAnswers2 = answers.stream()
                .flatMap(a -> a.permute(permutationUnifiers2))
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
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        assertEquals(query.getAtom().isUserDefinedName(), false);
        assertEquals(query2.getAtom().isUserDefinedName(), true);
        assertEquals(query.getAtoms().size(), 1);
        assertEquals(query2.getAtoms().size(), 2);
    }

    @Test
    public void testWhenUnifiyingAtomWithItself_UnifierIsTrivial(){
        String patternString = "{$x isa country;($x, $y) isa is-enemy-of;$y isa country;}";
        GraknGraph graph = cwGraph.graph();
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        assertTrue(Sets.intersection(unifier.keySet(), Sets.newHashSet(Graql.var("x"), Graql.var("y"))).isEmpty());
    }

    @Test
    public void testWhenUnifiyingBinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        GraknGraph graph =  unificationWithTypesSet.graph();
        String patternString = "{$x1 isa entity1;($x1, $x2) isa binary;}";
        String patternString2 = "{$y1 isa entity1;($y1, $y2) isa binary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                Graql.var("y1"), Graql.var("x1"),
                Graql.var("y2"), Graql.var("x2")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
    }

    @Test
    public void testWhenUnifiyingBinaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        GraknGraph graph =  unificationWithTypesSet.graph();
        String patternString = "{$x1 isa entity1;$x2 isa entity2;($x1, $x2) isa binary;}";
        String patternString2 = "{$y1 isa entity1;$y2 isa entity2;($y1, $y2) isa binary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                Graql.var("y1"), Graql.var("x1"),
                Graql.var("y2"), Graql.var("x2")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
    }

    @Test
    public void testWhenUnifiyingTernaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        GraknGraph graph =  unificationWithTypesSet.graph();
        String patternString = "{$x1 isa entity3;$x3 isa entity5;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa entity5;$y1 isa entity3;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa entity5;$y2 isa entity4;$y1 isa entity3;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier unifier2 = childQuery2.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                Graql.var("y1"), Graql.var("x1"),
                Graql.var("y2"), Graql.var("x2"),
                Graql.var("y3"), Graql.var("x3")
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
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier unifier2 = childQuery2.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                Graql.var("y1"), Graql.var("x1"),
                Graql.var("y2"), Graql.var("x2"),
                Graql.var("y3"), Graql.var("x3")
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
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(pattern3, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        Unifier unifier2 = childQuery2.getUnifier(parentQuery);
        Unifier correctUnifier = new UnifierImpl(ImmutableMap.of(
                Graql.var("y1"), Graql.var("x1"),
                Graql.var("y2"), Graql.var("x2"),
                Graql.var("y3"), Graql.var("x3")
        ));
        assertTrue(unifier.containsAll(correctUnifier));
        assertTrue(unifier2.containsAll(correctUnifier));
    }

    private Conjunction<VarPatternAdmin> conjunction(PatternAdmin pattern){
        Set<VarPatternAdmin> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarPatternAdmin> vars = graph.graql().parsePattern(patternString).admin()
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
