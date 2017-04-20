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
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.test.GraphContext;
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
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.permuteFunction;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.subFilter;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicQueryTest {

    @ClassRule
    public static final GraphContext snbGraph = GraphContext.preLoad(SNBGraph.get());

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext admissionsGraph = GraphContext.preLoad(AdmissionsGraph.get());

    @ClassRule
    public static final GraphContext cwGraph = GraphContext.preLoad(CWGraph.get());

    @ClassRule
    public static final GraphContext ancestorGraph = GraphContext.preLoad("ancestor-friend-test.gql");

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
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(pattern, snbGraph.graph());
    }

    @Test
    public void testWhenCopying_TheCopyIsAlphaEquivalent(){
        String patternString = "{($x, $y) isa recommendation;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, snbGraph.graph());
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(pattern, snbGraph.graph());
        ReasonerAtomicQuery copy = new ReasonerAtomicQuery(atomicQuery);
        assertEquals(atomicQuery, copy);
        assertEquals(atomicQuery.hashCode(), copy.hashCode());
    }

    @Test
    public void testWhenModifyingAQuery_TheCopyDoesNotChange(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{(recommended-item: $x, recommended-customer: $y) isa recommendation;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery copy = new ReasonerAtomicQuery(atomicQuery);

        atomicQuery.unify(VarName.of("y"), VarName.of("z"));
        MatchQuery q1 = atomicQuery.getMatchQuery();
        MatchQuery q2 = copy.getMatchQuery();
        assertNotEquals(q1, q2);
    }

    @Test
    public void testWhenCopyingAQuery_TheyHaveTheSameRoleVarTypeMaps(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{(recommended-item: $x, recommended-customer: $y) isa recommendation;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery copy = new ReasonerAtomicQuery(atomicQuery);

        atomicQuery.unify(VarName.of("y"), VarName.of("z"));
        assertEquals(new ReasonerAtomicQuery(conjunction(patternString, graph), snbGraph.graph()).getAtom().getRoleVarTypeMap(), copy.getAtom().getRoleVarTypeMap());
    }

    @Test
    public void testWhenMaterialising_MaterialisedInformationIsPresentInGraph(){
        QueryBuilder qb = snbGraph.graph().graql().infer(false);
        String explicitQuery = "match ($x, $y) isa recommendation;$x has name 'Bob';$y has name 'Colour of Magic';";
        assertTrue(!qb.<MatchQuery>parse(explicitQuery).ask().execute());

        String patternString = "{($x, $y) isa recommendation;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, snbGraph.graph());
        QueryAnswers answers = new QueryAnswers();

        answers.add(new QueryAnswer(
                ImmutableMap.of(
                        VarName.of("x"), getConcept("Bob"),
                        VarName.of("y"), getConcept("Colour of Magic")))
        );
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(pattern, snbGraph.graph());

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
        Set<Answer> answers = query.admin().streamWithVarNames().map(QueryAnswer::new).collect(toSet());
        Set<Answer> fullAnswers = query2.admin().streamWithVarNames().map(QueryAnswer::new).collect(toSet());
        Atom mappedAtom = new ReasonerAtomicQuery(conjunction(query.admin().getPattern()), graph).getAtom();
        Atom unmappedAtom = new ReasonerAtomicQuery(conjunction(query2.admin().getPattern()), graph).getAtom();

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
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(pattern2, graph);
        assertEquals(query.getAtom().isUserDefinedName(), false);
        assertEquals(query2.getAtom().isUserDefinedName(), true);
        assertEquals(query.getAtoms().size(), 1);
        assertEquals(query2.getAtoms().size(), 2);
    }

    @Test
    public void testWhenUnifiyingAtomWithItself_UnifierIsTrivial(){
        String patternString = "{$x isa country;($x, $y) isa is-enemy-of;$y isa country;}";
        GraknGraph graph = cwGraph.graph();
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery parentQuery = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery childQuery = new ReasonerAtomicQuery(pattern, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        assertTrue(Sets.intersection(unifier.keySet(), Sets.newHashSet(VarName.of("x"), VarName.of("y"))).isEmpty());

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
