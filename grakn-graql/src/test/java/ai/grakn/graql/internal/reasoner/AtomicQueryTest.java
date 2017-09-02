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
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.GeoKB;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.HashSet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.stream.Collectors;

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
    public void testWhenMaterialising_MaterialisedInformationIsPresentInGraph(){
        GraknTx graph = geoKB.tx();
        QueryBuilder qb = graph.graql().infer(false);
        String explicitQuery = "match (geo-entity: $x, entity-location: $y) isa is-located-in;$x has name 'Warsaw';$y has name 'Poland';";
        assertTrue(!qb.<MatchQuery>parse(explicitQuery).iterator().hasNext());

        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        QueryAnswers answers = new QueryAnswers();

        answers.add(new QueryAnswer(
                ImmutableMap.of(
                        Graql.var("x"), getConceptByResourceValue(graph, "Warsaw"),
                        Graql.var("y"), getConceptByResourceValue(graph, "Poland")))
        );
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);

        assertNotExists(qb.parse(explicitQuery));
        answers.stream().forEach(atomicQuery::materialise);
        assertExists(qb.parse(explicitQuery));
    }

    private Concept getConceptByResourceValue(GraknTx graph, String id){
        Set<Concept> instances = graph.getAttributesByValue(id)
                .stream().flatMap(res -> res.ownerInstances()).collect(Collectors.toSet());
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
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        String queryString2 = "match ($x, $y) isa is-located-in;";

        QueryBuilder qb = graph.graql().infer(false);
        MatchQuery query = qb.parse(queryString);
        MatchQuery query2 = qb.parse(queryString2);
        Set<Answer> answers = query.admin().stream().collect(toSet());
        Set<Answer> fullAnswers = query2.admin().stream().collect(toSet());
        Atom mappedAtom = ReasonerQueries.atomic(conjunction(query.admin().getPattern()), graph).getAtom();
        Atom unmappedAtom = ReasonerQueries.atomic(conjunction(query2.admin().getPattern()), graph).getAtom();

        Set<Unifier> permutationUnifiers = mappedAtom.getPermutationUnifiers(mappedAtom);
        Set<Answer> permutedAnswers = answers.stream()
                .flatMap(a -> a.permute(permutationUnifiers))
                .collect(Collectors.toSet());

        Set<Unifier> permutationUnifiers2 = unmappedAtom.getPermutationUnifiers(mappedAtom);
        Set<Answer> permutedAnswers2 = answers.stream()
                .flatMap(a -> a.permute(permutationUnifiers2))
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
        assertEquals(query.getAtom().isUserDefinedName(), false);
        assertEquals(query2.getAtom().isUserDefinedName(), true);
        assertEquals(query.getAtoms().size(), 1);
        assertEquals(query2.getAtoms().size(), 2);
    }

    @Test
    public void testWhenUnifiyingAtomWithItself_UnifierIsTrivial(){
        GraknTx graph = geoKB.tx();
        String patternString = "{$x isa city;($x, $y) isa is-located-in;$y isa country;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern, graph);
        Unifier unifier = childQuery.getUnifier(parentQuery);
        assertTrue(Sets.intersection(unifier.keySet(), Sets.newHashSet(Graql.var("x"), Graql.var("y"))).isEmpty());
    }

    @Test
    public void testWhenUnifiyingBinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        GraknTx graph =  unificationWithTypesSet.tx();
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
        GraknTx graph =  unificationWithTypesSet.tx();
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
        GraknTx graph =  unificationWithTypesSet.tx();
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

    @Test
    public void testAlphaEquivalence_DifferentIsaVariants(){
        testAlphaEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "isa", "entity1", "superEntity1");
    }

    @Test
    public void testAlphaEquivalence_DifferentSubVariants(){
        testAlphaEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "sub", "entity1", "role1");
    }

    @Test
    public void testAlphaEquivalence_DifferentPlaysVariants(){
        testAlphaEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "plays", "role1", "role2");
    }

    @Test
    public void testAlphaEquivalence_DifferentRelatesVariants(){
        testAlphaEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "relates", "role1", "role2");
    }

    @Test
    public void testAlphaEquivalence_DifferentHasVariants(){
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

    private void testAlphaEquivalence_DifferentTypeVariants(GraknTx graph, String keyword, String label, String label2){
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

        ReasonerAtomicQuery query =ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 =ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 =ReasonerQueries.atomic(pattern3, graph);
        ReasonerAtomicQuery query4 =ReasonerQueries.atomic(pattern4, graph);
        ReasonerAtomicQuery query5 =ReasonerQueries.atomic(pattern5, graph);

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
    public void testAlphaEquivalence_TypesWithSameLabel(){
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
        queryEquivalence(relatesQuery, playsQuery, false);
        queryEquivalence(relatesQuery, hasQuery, false);
        queryEquivalence(relatesQuery, subQuery2, false);
        queryEquivalence(relatesQuery, hasQuery, false);
        queryEquivalence(relatesQuery, subQuery2, false);
        queryEquivalence(hasQuery, subQuery2, false);
    }

    @Test
    public void testAlphaEquivalence_DifferentResourceVariants(){
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
    public void testAlphaEquivalence_ResourcesWithSubstitution(){
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

        queryEquivalence(query2, query3, false);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query2, query5, false);
        queryEquivalence(query2, query6, false);

        queryEquivalence(query3, query4, false);
        queryEquivalence(query3, query5, false);
        queryEquivalence(query3, query6, false);

        queryEquivalence(query4, query5, true);
        queryEquivalence(query4, query6, false);

        queryEquivalence(query5, query6, false);
    }

    @Test //tests alpha-equivalence of queries with resources with multi predicate
    public void testAlphaEquivalence_MultiPredicateResources(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$x isa entity1;$x has res1 $a;$a val >23; $a val <27;}";
        String patternString2 = "{$p isa entity1;$p has res1 $a;$a val >23;}";
        String patternString3 = "{$a isa entity1;$a has res1 $p;$p val <27;$p val >23;}";
        String patternString4 = "{$x isa entity1, has res1 $a;$a val >23; $a val <27;}";
        String patternString5 = "{$x isa $type;$type label entity1;$x has res1 $a;$a val >23; $a val <27;}";

        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        ReasonerAtomicQuery query3 = ReasonerQueries.atomic(conjunction(patternString3, graph), graph);
        ReasonerAtomicQuery query4 = ReasonerQueries.atomic(conjunction(patternString4, graph), graph);
        ReasonerAtomicQuery query5 = ReasonerQueries.atomic(conjunction(patternString5, graph), graph);

        queryEquivalence(query, query2, false);
        queryEquivalence(query, query3, true);
        queryEquivalence(query, query4, true);
        queryEquivalence(query, query5, true);
        queryEquivalence(query2, query3, false);
        queryEquivalence(query2, query4, false);
        queryEquivalence(query3, query4, true);
        queryEquivalence(query4, query5, true);
    }

    @Test //tests alpha-equivalence of resource atoms with different predicates
    public void testAlphaEquivalence_resourcesWithDifferentPredicates() {
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
        queryEquivalence(query2, query3, false);
        queryEquivalence(query3, query4, true);
        queryEquivalence(query4, query5, false);
    }

    @Test
    public void testAlphaEquivalence_DifferentRelationInequivalentVariants(){
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
                    .forEach(a -> queryEquivalence(a, at, false));
        });
    }

    @Test
    public void testAlphaEquivalence_RelationWithRepeatingVariables(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{(role1: $x, role2: $y);}";
        String patternString2 = "{(role1: $x, role2: $x);}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);

        ReasonerAtomicQuery query =ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 =ReasonerQueries.atomic(pattern2, graph);
        queryEquivalence(query, query2, false);
    }

    @Test
    public void testAlphaEquivalence_RelationsWithSubstitution(){
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

    private void queryEquivalence(ReasonerQueryImpl a, ReasonerQueryImpl b, boolean expectation){
        assertEquals(a.toString() + " =? " + b.toString(), a.equals(b), expectation);
        assertEquals(b.toString() + " =? " + a.toString(), b.equals(a), expectation);
        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), a.hashCode() == b.hashCode(), true);
        }
    }

    private Conjunction<VarPatternAdmin> conjunction(PatternAdmin pattern){
        Set<VarPatternAdmin> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

}
