/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
<<<<<<< HEAD
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
=======
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
>>>>>>> central/stable
 */

package ai.grakn.graql.internal.reasoner;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicEquivalence;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.kbs.GeoKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.GraqlTestUtil.assertNotExists;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AtomicQueryTest {

    @ClassRule
    public static final SampleKBContext geoKB = GeoKB.context();

    @ClassRule
    public static final SampleKBContext materialisationTestSet = SampleKBContext.load("materialisationTest.gql");

    @ClassRule
    public static final SampleKBContext unificationTestSet = SampleKBContext.load("unificationTest.gql");

    @ClassRule
    public static final SampleKBContext unificationWithTypesSet = SampleKBContext.load("unificationWithTypesTest.gql");

    @Test (expected = IllegalArgumentException.class)
    public void testWhenConstructingNonAtomicQuery_ExceptionIsThrown() {
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String patternString = "{$x isa university;$y isa country;($x, $y) isa is-located-in;($y, $z) isa is-located-in;}";
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(conjunction(patternString), graph);
    }

    @Test (expected = GraqlQueryException.class)
    public void testWhenCreatingQueryWithNonexistentType_ExceptionIsThrown(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String patternString = "{$x isa someType;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString), graph);
    }

    @Test
    public void testWhenMaterialising_MaterialisedInformationIsPresentInGraph(){
        EmbeddedGraknTx<?> graph = geoKB.tx();
        QueryBuilder qb = graph.graql().infer(false);
        String explicitQuery = "match (geo-entity: $x, entity-location: $y) isa is-located-in;$x has name 'Warsaw';$y has name 'Poland'; get;";
        assertTrue(!qb.<GetQuery>parse(explicitQuery).iterator().hasNext());

        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString);
        List<ConceptMap> answers = new ArrayList<>();

        answers.add(new ConceptMapImpl(
                ImmutableMap.of(
                        var("x"), getConceptByResourceValue(graph, "Warsaw"),
                        var("y"), getConceptByResourceValue(graph, "Poland")))
        );
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);

        assertNotExists(qb.parse(explicitQuery));
        answers.forEach(atomicQuery::materialise);
        assertExists(qb.parse(explicitQuery));
    }

    @Test
    public void testWhenMaterialisingEntity_MaterialisedInformationIsCorrectlyFlaggedAsInferred(){
        EmbeddedGraknTx<?> graph = materialisationTestSet.tx();
        ReasonerAtomicQuery entityQuery = ReasonerQueries.atomic(conjunction("$x isa newEntity"), graph);
        assertEquals(entityQuery.materialise(new ConceptMapImpl()).findFirst().orElse(null).get("x").asEntity().isInferred(), true);
    }

    @Test
    public void testWhenMaterialisingResources_MaterialisedInformationIsCorrectlyFlaggedAsInferred(){
        EmbeddedGraknTx<?> graph = materialisationTestSet.tx();
        QueryBuilder qb = graph.graql().infer(false);
        Concept firstEntity = Iterables.getOnlyElement(qb.<GetQuery>parse("match $x isa entity1; get;").execute()).get("x");
        Concept secondEntity = Iterables.getOnlyElement(qb.<GetQuery>parse("match $x isa entity2; get;").execute()).get("x");
        Concept resource = Iterables.getOnlyElement(qb.<GetQuery>parse("match $x isa resource; get;").execute()).get("x");

        ReasonerAtomicQuery resourceQuery = ReasonerQueries.atomic(conjunction("{$x has resource $r;$r == 'inferred';$x id " + firstEntity.id().getValue() + ";}"), graph);
        String reuseResourcePatternString =
                "{" +
                        "$x has resource $r;" +
                        "$x id " + secondEntity.id().getValue() + ";" +
                        "$r id " + resource.id().getValue() + ";" +
                        "}";

        ReasonerAtomicQuery reuseResourceQuery = ReasonerQueries.atomic(conjunction(reuseResourcePatternString), graph);

        assertEquals(resourceQuery.materialise(new ConceptMapImpl()).findFirst().orElse(null).get("r").asAttribute().isInferred(), true);

        reuseResourceQuery.materialise(new ConceptMapImpl()).collect(Collectors.toList());
        assertEquals(Iterables.getOnlyElement(
                qb.<GetQuery>parse("match" +
                        "$x has resource $r via $rel;" +
                        "$x id " + secondEntity.id().getValue() + ";" +
                        "$r id " + resource.id().getValue() + ";" +
                        "get;").execute()).get("rel").asRelationship().isInferred(), true);
        assertEquals(Iterables.getOnlyElement(
                qb.<GetQuery>parse("match" +
                        "$x has resource $r via $rel;" +
                        "$x id " + firstEntity.id().getValue() + ";" +
                        "$r id " + resource.id().getValue() + ";" +
                        "get;").execute()).get("rel").asRelationship().isInferred(), false);
    }

    @Test
    public void testWhenMaterialisingRelations_MaterialisedInformationIsCorrectlyFlaggedAsInferred(){
        EmbeddedGraknTx<?> graph = materialisationTestSet.tx();
        QueryBuilder qb = graph.graql().infer(false);
        Concept firstEntity = Iterables.getOnlyElement(qb.<GetQuery>parse("match $x isa entity1; get;").execute()).get("x");
        Concept secondEntity = Iterables.getOnlyElement(qb.<GetQuery>parse("match $x isa entity2; get;").execute()).get("x");

        ReasonerAtomicQuery relationQuery = ReasonerQueries.atomic(conjunction(
                "{" +
                        "$r (role1: $x, role2: $y);" +
                        "$x id " + firstEntity.id().getValue() + ";" +
                        "$y id " + secondEntity.id().getValue() + ";" +
                        "}"
                ),
                graph
        );

        assertEquals(relationQuery.materialise(new ConceptMapImpl()).findFirst().orElse(null).get("r").asRelationship().isInferred(), true);
    }

    @Test
    public void testWhenCopying_TheCopyIsAlphaEquivalent(){
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String patternString = "{($x, $y) isa is-located-in;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString);
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery copy = ReasonerQueries.atomic(atomicQuery);
        assertEquals(atomicQuery, copy);
        assertEquals(atomicQuery.hashCode(), copy.hashCode());
    }

    @Test
    public void testWhenRoleTypesAreAmbiguous_answersArePermutedCorrectly(){
        EmbeddedGraknTx<?> graph = geoKB.tx();
        String childString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";
        String parentString = "match ($x, $y) isa is-located-in; get;";

        QueryBuilder qb = graph.graql().infer(false);
        GetQuery childQuery = qb.parse(childString);
        GetQuery parentQuery = qb.parse(parentString);
        Set<ConceptMap> answers = childQuery.stream().collect(toSet());
        Set<ConceptMap> fullAnswers = parentQuery.stream().collect(toSet());
        Atom childAtom = ReasonerQueries.atomic(conjunction(childQuery.match().admin().getPattern()), graph).getAtom();
        Atom parentAtom = ReasonerQueries.atomic(conjunction(parentQuery.match().admin().getPattern()), graph).getAtom();

        MultiUnifier multiUnifier = childAtom.getMultiUnifier(childAtom, UnifierType.RULE);
        Set<ConceptMap> permutedAnswers = answers.stream()
                .flatMap(a -> multiUnifier.stream().map(a::unify))
                .collect(Collectors.toSet());

        MultiUnifier multiUnifier2 = childAtom.getMultiUnifier(parentAtom, UnifierType.RULE);
        Set<ConceptMap> permutedAnswers2 = answers.stream()
                .flatMap(a -> multiUnifier2.stream().map(a::unify))
                .collect(Collectors.toSet());

        assertEquals(fullAnswers, permutedAnswers2);
        assertEquals(answers, permutedAnswers);
    }

    @Test
    public void testWhenUnifiyingAtomWithItself_UnifierIsIdentity(){
        EmbeddedGraknTx<?> graph = unificationWithTypesSet.tx();
        String patternString = "{$x1 isa twoRoleEntity;$x2 isa twoRoleEntity2;($x1, $x2) isa binary;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(pattern, graph);
        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier unifier2 = parentQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier unifier3 = childQuery.getMultiUnifier(childQuery).getUnifier();
        assertTrue(Sets.intersection(unifier.keySet(), Sets.newHashSet(var("x"), var("y"))).isEmpty());
        assertTrue(unifier2.isEmpty());
        assertTrue(unifier3.isEmpty());
    }

    /**
     * ##################################
     *
     *     Unification Tests
     *
     * ##################################
     */

    @Test
    public void testUnification_EXACT_BinaryRelationWithSubs(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();

        Concept x1 = getConceptByResourceValue(graph, "x1");
        Concept x2 = getConceptByResourceValue(graph, "x2");

        String basePatternString = "{($x1, $x2) isa binary;}";
        String basePatternString2 = "{($y1, $y2) isa binary;}";

        ReasonerAtomicQuery xbaseQuery = ReasonerQueries.atomic(conjunction(basePatternString), graph);
        ReasonerAtomicQuery ybaseQuery = ReasonerQueries.atomic(conjunction(basePatternString2), graph);

        ConceptMap xAnswer = new ConceptMapImpl(ImmutableMap.of(var("x1"), x1, var("x2"), x2));
        ConceptMap flippedXAnswer = new ConceptMapImpl(ImmutableMap.of(var("x1"), x2, var("x2"), x1));

        ConceptMap yAnswer = new ConceptMapImpl(ImmutableMap.of(var("y1"), x1, var("y2"), x2));
        ConceptMap flippedYAnswer = new ConceptMapImpl(ImmutableMap.of(var("y1"), x2, var("y2"), x1));

        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(xbaseQuery, xAnswer);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(xbaseQuery, flippedXAnswer);

        Unifier unifier = childQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier = new UnifierImpl(ImmutableMultimap.of(
                var("x1"), var("x2"),
                var("x2"), var("x1")
        ));
        assertTrue(unifier.containsAll(correctUnifier));

        ReasonerAtomicQuery yChildQuery = ReasonerQueries.atomic(ybaseQuery, yAnswer);
        ReasonerAtomicQuery yChildQuery2 = ReasonerQueries.atomic(ybaseQuery, flippedYAnswer);

        Unifier unifier2 = yChildQuery.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier2 = new UnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2")
        ));
        assertTrue(unifier2.containsAll(correctUnifier2));

        Unifier unifier3 = yChildQuery2.getMultiUnifier(parentQuery).getUnifier();
        Unifier correctUnifier3 = new UnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x2"),
                var("y2"), var("x1")
        ));
        assertTrue(unifier3.containsAll(correctUnifier3));
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa twoRoleEntity;($x1, $x2) isa binary;}";
        String patternString2 = "{$y1 isa twoRoleEntity;($y1, $y2) isa binary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2);
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
    public void testUnification_EXACT_BinaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa twoRoleEntity;$x2 isa twoRoleEntity2;($x1, $x2) isa binary;}";
        String patternString2 = "{$y1 isa twoRoleEntity;$y2 isa twoRoleEntity2;($y1, $y2) isa binary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2);
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
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsRoles(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String parentString = "{(role1: $x, role1: $y, role2: $z) isa ternary;}";
        String childString = "{(role1: $u, role2: $v, role3: $q) isa ternary;}";
        String childString2 = "{(role1: $u, role2: $v, role2: $q) isa ternary;}";
        String childString3 = "{(role1: $u, role1: $v, role2: $q) isa ternary;}";
        String childString4 = "{(role1: $u, role1: $u, role2: $q) isa ternary;}";
        Conjunction<VarPatternAdmin> parentPattern = conjunction(parentString);
        Conjunction<VarPatternAdmin> childPattern = conjunction(childString);
        Conjunction<VarPatternAdmin> childPattern2 = conjunction(childString2);
        Conjunction<VarPatternAdmin> childPattern3 = conjunction(childString3);
        Conjunction<VarPatternAdmin> childPattern4 = conjunction(childString4);
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
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsMetaRoles(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String parentString = "{(role: $x, role: $y, role2: $z) isa ternary;}";
        String childString = "{(role1: $u, role2: $v, role3: $q) isa ternary;}";
        String childString2 = "{(role1: $u, role2: $v, role2: $q) isa ternary;}";
        String childString3 = "{(role1: $u, role1: $v, role2: $q) isa ternary;}";
        String childString4 = "{(role1: $u, role1: $u, role2: $q) isa ternary;}";
        Conjunction<VarPatternAdmin> parentPattern = conjunction(parentString);
        Conjunction<VarPatternAdmin> childPattern = conjunction(childString);
        Conjunction<VarPatternAdmin> childPattern2 = conjunction(childString2);
        Conjunction<VarPatternAdmin> childPattern3 = conjunction(childString3);
        Conjunction<VarPatternAdmin> childPattern4 = conjunction(childString4);
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
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsRoles_ParentRepeatsRPs(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String parentString = "{(role1: $x, role1: $x, role2: $y) isa ternary;}";
        String childString = "{(role1: $u, role2: $v, role3: $q) isa ternary;}";
        String childString2 = "{(role1: $u, role2: $v, role2: $q) isa ternary;}";
        String childString3 = "{(role1: $u, role1: $v, role2: $q) isa ternary;}";
        String childString4 = "{(role1: $u, role1: $u, role2: $q) isa ternary;}";
        Conjunction<VarPatternAdmin> parentPattern = conjunction(parentString);
        Conjunction<VarPatternAdmin> childPattern = conjunction(childString);
        Conjunction<VarPatternAdmin> childPattern2 = conjunction(childString2);
        Conjunction<VarPatternAdmin> childPattern3 = conjunction(childString3);
        Conjunction<VarPatternAdmin> childPattern4 = conjunction(childString4);
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
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsMetaRoles_ParentRepeatsRPs(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String parentString = "{(role: $x, role: $x, role2: $y) isa ternary;}";
        String childString = "{(role1: $u, role2: $v, role3: $q) isa ternary;}";
        String childString2 = "{(role1: $u, role2: $v, role2: $q) isa ternary;}";
        String childString3 = "{(role1: $u, role1: $v, role2: $q) isa ternary;}";
        String childString4 = "{(role1: $u, role1: $u, role2: $q) isa ternary;}";
        Conjunction<VarPatternAdmin> parentPattern = conjunction(parentString);
        Conjunction<VarPatternAdmin> childPattern = conjunction(childString);
        Conjunction<VarPatternAdmin> childPattern2 = conjunction(childString2);
        Conjunction<VarPatternAdmin> childPattern3 = conjunction(childString3);
        Conjunction<VarPatternAdmin> childPattern4 = conjunction(childString4);
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
    public void testUnification_EXACT_TernaryRelationWithTypes_RepeatingRelationPlayers_withMetaRoles(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa threeRoleEntity;$x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa threeRoleEntity3;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3);
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
    public void testUnification_EXACT_TernaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa threeRoleEntity;$x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa threeRoleEntity3;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3);
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
    public void testUnification_EXACT_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String patternString = "{$x1 isa threeRoleEntity;$x2 isa threeRoleEntity2; $x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}";
        String patternString2 = "{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}";
        String patternString3 = "{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3);
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

    @Test // subSubThreeRoleEntity sub subThreeRoleEntity sub threeRoleEntity3
    public void testUnification_EXACT_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes_TypeHierarchyInvolved(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        String parentString = "{$x1 isa threeRoleEntity;$x2 isa subThreeRoleEntity; $x3 isa subSubThreeRoleEntity;($x1, $x2, $x3) isa ternary;}";

        String childString = "{$y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;($y2, $y3, $y1) isa ternary;}";
        String childString2 = "{$y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}";
        Conjunction<VarPatternAdmin> pattern = conjunction(parentString);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(childString);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(childString2);
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
    public void testUnification_EXACT_ResourcesWithTypes(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentQuery = "{$x has resource $r; $x isa baseRoleEntity;}";

        String childQuery = "{$r has resource $x; $r isa subRoleEntity;}";
        String childQuery2 = "{$x1 has resource $x; $x1 isa subSubRoleEntity;}";
        String baseQuery = "{$r has resource $x; $r isa entity;}";

        exactQueryUnification(parentQuery, childQuery, false, false, true, graph);
        exactQueryUnification(parentQuery, childQuery2, false, false, true, graph);
        exactQueryUnification(parentQuery, baseQuery, true, true, true, graph);
    }

    @Test
    public void testUnification_EXACT_BinaryRelationWithRoleAndTypeHierarchy_MetaTypeParent(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa entity; $y isa entity;}";

        String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa baseRoleEntity; $v isa baseRoleEntity;}";
        String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa baseRoleEntity; $x isa baseRoleEntity;}";
        String specialisedRelation3 = "{(subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subRoleEntity;}";
        String specialisedRelation4 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subRoleEntity;}";
        String specialisedRelation5 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation6 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

        exactQueryUnification(parentRelation, specialisedRelation, false, false, true, graph);
        exactQueryUnification(parentRelation, specialisedRelation2, false, false, true, graph);
        exactQueryUnification(parentRelation, specialisedRelation3, false, false, true, graph);
        exactQueryUnification(parentRelation, specialisedRelation4, false, false, true, graph);
        exactQueryUnification(parentRelation, specialisedRelation5, false, false, true, graph);
        exactQueryUnification(parentRelation, specialisedRelation6, false, false, true, graph);
    }

    @Test
    public void testUnification_EXACT_BinaryRelationWithRoleAndTypeHierarchy_BaseRoleParent(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String baseParentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa baseRoleEntity; $y isa baseRoleEntity;}";
        String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa subSubRoleEntity; $y isa subSubRoleEntity;}";

        String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";
        String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

        exactQueryUnification(baseParentRelation, specialisedRelation, false, false, true, graph);
        exactQueryUnification(baseParentRelation, specialisedRelation2, false, false, true, graph);
        exactQueryUnification(baseParentRelation, specialisedRelation3, false, false, true, graph);
        exactQueryUnification(baseParentRelation, specialisedRelation4, false, false, true, graph);

        exactQueryUnification(parentRelation, specialisedRelation, false, false, false, graph);
        exactQueryUnification(parentRelation, specialisedRelation2, false, false, false, graph);
        exactQueryUnification(parentRelation, specialisedRelation3, false, false, false, graph);
        exactQueryUnification(parentRelation, specialisedRelation4, false, false, false, graph);
    }

    @Test
    public void testUnification_EXACT_BinaryRelationWithRoleAndTypeHierarchy_BaseRoleParent_middleTypes(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa subRoleEntity; $y isa subRoleEntity;}";

        String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subSubRoleEntity;}";
        String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

        exactQueryUnification(parentRelation, specialisedRelation, false, false, true, graph);
        exactQueryUnification(parentRelation, specialisedRelation2, false, false, true, graph);
        exactQueryUnification(parentRelation, specialisedRelation3, false, false, true, graph);
        exactQueryUnification(parentRelation, specialisedRelation4, false, false, true, graph);
    }

    @Test
    public void testUnification_STRUCTURAL_differentRelationVariants(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();

        Entity baseEntity = graph.getEntityType("baseRoleEntity").instances().findFirst().orElse(null);
        Entity anotherBaseEntity = graph.getEntityType("anotherBaseRoleEntity").instances().findFirst().orElse(null);
        Entity subEntity = graph.getEntityType("subRoleEntity").instances().findFirst().orElse(null);
        
        ArrayList<String> qs = Lists.newArrayList(
                "{(baseRole1: $x, baseRole2: $y);}",

                //(x[], y), 1-3
                "{(baseRole1: $x1_1, baseRole2: $x2_1); $x1_1 isa baseRoleEntity;}",
                "{(baseRole1: $x1_1b, baseRole2: $x2_1b); $x1_1b id '" + baseEntity.id().getValue() + "';}",
                "{(baseRole1: $x1_1c, baseRole2: $x2_1c); $x1_1c id '" + subEntity.id().getValue() + "';}",

                //(x, y[]), 4-6
                "{(baseRole1: $x1_1, baseRole2: $x2_1); $x2_1 isa baseRoleEntity;}",
                "{(baseRole1: $x1_1b, baseRole2: $x2_1b); $x2_1b id '" + baseEntity.id().getValue() + "';}",
                "{(baseRole1: $x1_1c, baseRole2: $x2_1c); $x2_1c id '" + subEntity.id().getValue() + "';}",

                //(x[], y), 7-9
                "{(baseRole1: $x1_2, baseRole2: $x2_2); $x1_2 isa anotherBaseRoleEntity;}",
                "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x1_2b id '" + anotherBaseEntity.id().getValue() + "';}",
                "{(baseRole1: $x1_2c, baseRole2: $x2_2c); $x1_2c id '" + subEntity.id().getValue() + "';}",

                //(x, y[]), 10-12
                "{(baseRole1: $x1_2, baseRole2: $x2_2); $x2_2 isa anotherBaseRoleEntity;}",
                "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x2_2b id '" + anotherBaseEntity.id().getValue() + "';}",
                "{(baseRole1: $x1_2c, baseRole2: $x2_2c); $x2_2c id '" + subEntity.id().getValue() + "';}",

                //(x[], y[]), 13-15
                "{(baseRole1: $x1_3, baseRole2: $x2_3); $x1_3 isa baseRoleEntity; $x2_3 isa anotherBaseRoleEntity;}",
                "{(baseRole1: $x1_3b, baseRole2: $x2_3b); $x1_3b id '" + baseEntity.id().getValue() + "'; $x2_3b id '" + anotherBaseEntity.id().getValue() + "';}",
                "{(baseRole1: $x1_3c, baseRole2: $x2_3c); $x1_3c id '" + baseEntity.id().getValue() + "'; $x2_3c id '" + anotherBaseEntity.id().getValue() + "';}"
        );

        structuralUnification(qs.get(0), qs, new ArrayList<>(), graph);

        structuralUnification(qs.get(1), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(2), qs, subList(qs, Lists.newArrayList(3, 8, 9)), graph);
        structuralUnification(qs.get(3), qs, subList(qs, Lists.newArrayList(2, 8, 9)), graph);

        structuralUnification(qs.get(4), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(5), qs, subList(qs, Lists.newArrayList(6, 11, 12)), graph);
        structuralUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(5, 11, 12)), graph);

        structuralUnification(qs.get(7), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(9, 2, 3)), graph);
        structuralUnification(qs.get(9), qs, subList(qs, Lists.newArrayList(8, 2, 3)), graph);

        structuralUnification(qs.get(10), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(11), qs, subList(qs, Lists.newArrayList(12, 5, 6)), graph);
        structuralUnification(qs.get(12), qs, subList(qs, Lists.newArrayList(11, 5, 6)), graph);

        structuralUnification(qs.get(13), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(14), qs, Collections.singletonList(qs.get(15)), graph);
        structuralUnification(qs.get(15), qs, Collections.singletonList(qs.get(14)), graph);
    }

    @Test
    public void testUnification_STRUCTURAL_differentTypeVariants(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();

        Iterator<Attribute<Object>> resources = graph.getAttributeType("resource").instances().collect(toSet()).iterator();
        Attribute<Object> resource = resources.next();
        Attribute<Object> anotherResource = resources.next();

        ArrayList<String> qs = Lists.newArrayList(
                "{$x isa resource;}",
                "{$xb isa resource-long;}",

                //2-3
                "{$x1a isa resource; $x1a id '" + resource.id().getValue() + "';}",
                "{$x1b isa resource; $x1b id '" + anotherResource.id().getValue() + "';}",

                //4-5
                "{$x2a isa resource; $x2a == 'someValue';}",
                "{$x2b isa resource; $x2b == 'someOtherValue';}",

                //6-7
                "{$x3a isa resource-long; $x3a == '0';}",
                "{$x3b isa resource-long; $x3b == '1';}",

                //7-8
                "{$x4a isa resource-long; $x4a > '0';}",
                "{$x4b isa resource-long; $x4b < '1';}",
                "{$x4c isa resource-long; $x4c >= '0';}",
                "{$x4d isa resource-long; $x4d <= '1';}"
        );

        structuralUnification(qs.get(0), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(1), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(2), qs, Collections.singletonList(qs.get(3)), graph);
        structuralUnification(qs.get(3), qs, Collections.singletonList(qs.get(2)), graph);

        structuralUnification(qs.get(4), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(5), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(6), qs, new ArrayList<>(), graph);

        structuralUnification(qs.get(7), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(8), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(9), qs, new ArrayList<>(), graph);
    }

    @Test
    public void testUnification_STRUCTURAL_differentResourceVariants(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();

        Iterator<Attribute<Object>> resources = graph.getAttributeType("resource").instances().collect(toSet()).iterator();
        Iterator<Entity> entities = graph.getEntityType("baseRoleEntity").instances().collect(toSet()).iterator();
        Attribute<Object> resource = resources.next();
        Attribute<Object> anotherResource = resources.next();
        Entity entity = entities.next();
        Entity anotherEntity = entities.next();

        ArrayList<String> qs = Lists.newArrayList(
                "{$x has resource $r;}",
                "{$xb has resource-long $rb;}",

                //2-3
                "{$x1a has resource $r1a; $x1a id '" + entity.id().getValue() + "';}",
                "{$x1b has resource $r1b; $x1b id '" + anotherEntity.id().getValue() + "';}",

                //4-5
                "{$x2a has resource $r2a; $r2a id '" + resource.id().getValue() + "';}",
                "{$x2b has resource $r2b; $r2b id '" + anotherResource.id().getValue() + "';}",

                //6-9
                "{$x3a has resource 'someValue';}",
                "{$x3b has resource 'someOtherValue';}",
                "{$x3c has resource $x3c; $x3c == 'someValue';}",
                "{$x3d has resource $x3d; $x3d == 'someOtherValue';}",

                //10-13
                "{$x4a has resource-long '0';}",
                "{$x4b has resource-long '1';}",
                "{$x4c has resource-long $x4c; $x4c == '0';}",
                "{$x4d has resource-long $x4d; $x4d == '1';}",

                //14-17
                "{$x5a has resource-long $x5a; $x5a > '0';}",
                "{$x5b has resource-long $x5b; $x5b < '1';}",
                "{$x5c has resource-long $x5c; $x5c >= '0';}",
                "{$x5d has resource-long $x5d; $x5d <= '1';}"
        );

        structuralUnification(qs.get(0), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(1), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(2), qs, Collections.singletonList(qs.get(3)), graph);
        structuralUnification(qs.get(3), qs, Collections.singletonList(qs.get(2)), graph);

        structuralUnification(qs.get(4), qs, Collections.singletonList(qs.get(5)), graph);
        structuralUnification(qs.get(5), qs, Collections.singletonList(qs.get(4)), graph);

        structuralUnification(qs.get(6), qs, Collections.singletonList(qs.get(8)), graph);
        structuralUnification(qs.get(7), qs, Collections.singletonList(qs.get(9)), graph);
        structuralUnification(qs.get(8), qs, Collections.singletonList(qs.get(6)), graph);
        structuralUnification(qs.get(9), qs, Collections.singletonList(qs.get(7)), graph);

        structuralUnification(qs.get(10), qs, Collections.singletonList(qs.get(12)), graph);
        structuralUnification(qs.get(11), qs, Collections.singletonList(qs.get(13)), graph);
        structuralUnification(qs.get(12), qs, Collections.singletonList(qs.get(10)), graph);
        structuralUnification(qs.get(13), qs, Collections.singletonList(qs.get(11)), graph);

        structuralUnification(qs.get(14), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(15), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(16), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(17), qs, new ArrayList<>(), graph);

    }

    private <T> List<T> subList(List<T> list, Collection<Integer> elements){
        List<T> subList = new ArrayList<>();
        elements.forEach(el -> subList.add(list.get(el)));
        return subList;
    }

    /**
     * checks the correctness and uniqueness of an EXACT unifier required to unify child query with parent
     * @param parentQuery parent query
     * @param childQuery child query
     * @param checkInverse flag specifying whether the inverse equality u^{-1}=u(parent, child) of the unifier u(child, parent) should be checked
     * @param ignoreTypes flag specifying whether the types should be disregarded and only role players checked for containment
     * @param checkEquality if true the parent and child answers will be checked for equality, otherwise they are checked for containment of child answers in parent
     */
    private void exactQueryUnification(ReasonerAtomicQuery parentQuery, ReasonerAtomicQuery childQuery, boolean checkInverse, boolean checkEquality, boolean ignoreTypes){
        UnifierType type = UnifierType.EXACT;
        Unifier unifier = childQuery.getMultiUnifier(parentQuery, type).getUnifier();
        //TODO enable that
        //queryEquivalence(childQuery, parentQuery, true, ReasonerQueryEquivalence.AlphaEquivalence);

        List<ConceptMap> childAnswers = childQuery.getQuery().execute();
        List<ConceptMap> unifiedAnswers = childAnswers.stream()
                .map(a -> a.unify(unifier))
                .filter(a -> !a.isEmpty())
                .collect(Collectors.toList());
        List<ConceptMap> parentAnswers = parentQuery.getQuery().execute();

        if (checkInverse) {
            Unifier inverse = parentQuery.getMultiUnifier(childQuery, type).getUnifier();
            assertEquals(unifier.inverse(), inverse);
            assertEquals(unifier, inverse.inverse());
        }

        assertTrue(!childAnswers.isEmpty());
        assertTrue(!unifiedAnswers.isEmpty());
        assertTrue(!parentAnswers.isEmpty());

        Set<Var> parentNonTypeVariables = Sets.difference(parentQuery.getAtom().getVarNames(), Sets.newHashSet(parentQuery.getAtom().getPredicateVariable()));
        if (!checkEquality){
            if(!ignoreTypes){
                assertTrue(parentAnswers.containsAll(unifiedAnswers));
            } else {
                List<ConceptMap> projectedParentAnswers = parentAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedUnified = unifiedAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                assertTrue(projectedParentAnswers.containsAll(projectedUnified));
            }

        } else {
            Unifier inverse = unifier.inverse();
            if(!ignoreTypes) {
                assertCollectionsEqual(parentAnswers, unifiedAnswers);
                List<ConceptMap> parentToChild = parentAnswers.stream().map(a -> a.unify(inverse)).collect(Collectors.toList());
                assertCollectionsEqual(parentToChild, childAnswers);
            } else {
                Set<Var> childNonTypeVariables = Sets.difference(childQuery.getAtom().getVarNames(), Sets.newHashSet(childQuery.getAtom().getPredicateVariable()));
                List<ConceptMap> projectedParentAnswers = parentAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedUnified = unifiedAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedChild = childAnswers.stream().map(ans -> ans.project(childNonTypeVariables)).collect(Collectors.toList());

                assertCollectionsEqual(projectedParentAnswers, projectedUnified);
                List<ConceptMap> projectedParentToChild = projectedParentAnswers.stream()
                        .map(a -> a.unify(inverse))
                        .map(ans -> ans.project(childNonTypeVariables))
                        .collect(Collectors.toList());
                assertCollectionsEqual(projectedParentToChild, projectedChild);
            }
        }
    }

    private void exactQueryUnification(String parentPatternString, String childPatternString, boolean checkInverse, boolean checkEquality, boolean ignoreTypes, EmbeddedGraknTx<?> graph){
        exactQueryUnification(
                ReasonerQueries.atomic(conjunction(parentPatternString), graph),
                ReasonerQueries.atomic(conjunction(childPatternString), graph),
                checkInverse,
                checkEquality,
                ignoreTypes);
    }

    private void structuralUnification(String childString, String parentString, boolean unifierExists, EmbeddedGraknTx graph){
        ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childString), graph);
        ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentString), graph);

        queryEquivalence(child, parent, unifierExists, ReasonerQueryEquivalence.StructuralEquivalence);
        MultiUnifier multiUnifier = child.getMultiUnifier(parent, UnifierType.STRUCTURAL);
        assertEquals("Unexpected unifier: " + multiUnifier + " between the child - parent pair:\n" + child + " :\n" + parent, unifierExists, !multiUnifier.isEmpty());
        if (unifierExists){
            MultiUnifier multiUnifierInverse = parent.getMultiUnifier(child, UnifierType.STRUCTURAL);
            assertTrue("Unexpected unifier: " + multiUnifier + " between the child - parent pair:\n" + parent + " :\n" + child, !multiUnifierInverse.isEmpty());
            assertEquals(multiUnifierInverse, multiUnifier.inverse());
        }
    }

    private void structuralUnification(String child, List<String> queries, List<String> queriesWithUnifier, EmbeddedGraknTx graph){
        queries.forEach(parent -> structuralUnification(child, parent, queriesWithUnifier.contains(parent) || parent.equals(child), graph));
    }


    /**
     * ##################################
     *
     *     Equivalence Tests
     *
     * ##################################
     */

    @Test
    public void testEquivalence_DifferentIsaVariants(){
        testEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "isa", "baseRoleEntity", "subRoleEntity");
    }

    @Test
    public void testEquivalence_DifferentSubVariants(){
        testEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "sub", "baseRoleEntity", "baseRole1");
    }

    @Test
    public void testEquivalence_DifferentPlaysVariants(){
        testEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "plays", "baseRole1", "baseRole2");
    }

    @Test
    public void testEquivalence_DifferentRelatesVariants(){
        testEquivalence_DifferentTypeVariants(unificationTestSet.tx(), "relates", "baseRole1", "baseRole2");
    }

    @Test
    public void testEquivalence_DifferentHasVariants(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{$x has resource;}";
        String query2 = "{$y has resource;}";
        String query3 = "{$x has " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + ";}";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3);

        equivalence(query, queries, Lists.newArrayList(query2), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, Lists.newArrayList(query2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, Lists.newArrayList(query), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Lists.newArrayList(query), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    private void testEquivalence_DifferentTypeVariants(EmbeddedGraknTx<?> graph, String keyword, String label, String label2){
        String query = "{$x " + keyword + " " + label + ";}";
        String query2 = "{$y " + keyword + " $type;$type label " + label +";}";
        String query3 = "{$z " + keyword + " $t;$t label " + label +";}";
        String query4 = "{$x " + keyword + " $y;}";
        String query5 = "{$x " + keyword + " " + label2 + ";}";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query5);

        equivalence(query, queries, Lists.newArrayList(query2, query3), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, Lists.newArrayList(query2, query3), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, Lists.newArrayList(query, query3), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Lists.newArrayList(query, query3), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, Lists.newArrayList(query, query2), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, Lists.newArrayList(query, query2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_TypesWithSameLabel(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String isaQuery = "{$x isa baseRoleEntity;}";
        String subQuery = "{$x sub baseRoleEntity;}";

        String playsQuery = "{$x plays baseRole1;}";
        String relatesQuery = "{$x relates baseRole1;}";
        String hasQuery = "{$x has resource;}";
        String subQuery2 = "{$x sub baseRole1;}";

        ArrayList<String> queries = Lists.newArrayList(isaQuery, subQuery, playsQuery, relatesQuery, hasQuery, subQuery2);

        equivalence(isaQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(isaQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(subQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(subQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(playsQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(playsQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(relatesQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(relatesQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(hasQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(hasQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_TypesWithSubstitution(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{$y isa baseRoleEntity;}";
        String query2 = "{$x isa baseRoleEntity; $x id 'X';}";
        String query2b = "{$r isa baseRoleEntity; $r id 'X';}";
        String query2c = "{$e isa $t;$t label baseRoleEntity;$e id 'X';}";
        String query3s2 = "{$z isa baseRoleEntity; $z id 'Y';}";
        String query4s1 = "{$a isa baseRoleEntity; $b id 'X';}";
        String query5 = "{$e isa baseRoleEntity ; $e != $f;}";
        String query6 = "{$e isa baseRoleEntity ; $e != $f; $f id 'X';}";
        String query7 = "{$e isa entity ; $e id 'X';}";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query2b, query2c, query3s2, query4s1, query5, query6, query7);

        queryEquivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        atomicEquivalence(query, queries, Lists.newArrayList(query4s1), ReasonerQueryEquivalence.AlphaEquivalence.atomicEquivalence(), graph);
        equivalence(query, queries, Lists.newArrayList(query4s1), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, Lists.newArrayList(query2b, query2c), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Lists.newArrayList(query2b, query2c, query3s2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2b, queries, Lists.newArrayList(query2, query2c), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2b, queries, Lists.newArrayList(query2, query2c, query3s2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2c, queries, Lists.newArrayList(query2, query2b), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2c, queries, Lists.newArrayList(query2, query2b, query3s2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3s2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3s2, queries, Lists.newArrayList(query2, query2b, query2c), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        queryEquivalence(query4s1, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        atomicEquivalence(query4s1, queries, Lists.newArrayList(query), ReasonerQueryEquivalence.AlphaEquivalence.atomicEquivalence(), graph);
        equivalence(query4s1, queries, Lists.newArrayList(query), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_DifferentResourceVariants(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{$x has resource 'value';}";
        String query2 = "{$y has resource $r;$r 'value';}";
        String query3 = "{$y has resource $r;}";
        String query4 = "{$y has resource 'value2';}";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4);

        equivalence(query, queries, Lists.newArrayList(query2), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, Lists.newArrayList(query2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, Lists.newArrayList(query), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Lists.newArrayList(query), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_ResourcesWithSubstitution(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{$x has resource $y;}";
        String query2 = "{$y has resource $z; $y id 'X';}";
        String query3 = "{$z has resource $u; $z id 'Y';}";

        String query4 = "{$y has resource $r;$r id 'X';}";
        String query4b = "{$r has resource $x;$x id 'X';}";
        String query5 = "{$y has resource $x;$x id 'Y';}";

        String query6 = "{$y has resource $x;$x != $x2;}";
        String query7 = "{$y has resource $x;$x != $x2; $x2 id 'Y';}";
        String query8 = "{$y has resource $x;$y != $y2;}";
        String query9 = "{$y has resource $x;$y != $y2; $y2 id 'Y';}";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query4b, query5, query6, query7, query8, query9);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Collections.singletonList(query3), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4, queries, Lists.newArrayList(query4b), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4, queries, Lists.newArrayList(query4b, query5), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4b, queries, Lists.newArrayList(query4), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4b, queries, Lists.newArrayList(query4, query5), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query5, queries, Lists.newArrayList(query4, query4b), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query8, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query8, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test //tests alpha-equivalence of queries with resources with multi predicate
    public void testEquivalence_MultiPredicateResources(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{$z has resource $u;$a >23; $a <27;}";
        String query2 = "{$x isa baseRoleEntity;$x has resource $a;$a >23; $a <27;}";
        String query2b = "{$a isa baseRoleEntity;$a has resource $p;$p <27;$p >23;}";
        String query2c = "{$x isa baseRoleEntity, has resource $a;$a >23; $a <27;}";
        String query2d = "{$x isa $type;$type label baseRoleEntity;$x has resource $a;$a >23; $a <27;}";
        String query3 = "{$e isa baseRoleEntity;$e has resource > 23;}";
        String query3b = "{$p isa baseRoleEntity;$p has resource $a;$a >23;}";
        String query4 = "{$x isa baseRoleEntity;$x has resource $y;$y >27;$y <23;}";
        String query5 = "{$x isa baseRoleEntity, has resource $z1;$z1 >23; $z2 <27;}";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query2b, query2c, query2d, query3, query3b, query4, query5);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, Lists.newArrayList(query2b, query2c, query2d), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Lists.newArrayList(query2b, query2c, query2d), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2b, queries, Lists.newArrayList(query2, query2c, query2d), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2b, queries, Lists.newArrayList(query2, query2c, query2d), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2c, queries, Lists.newArrayList(query2, query2b, query2d), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2c, queries, Lists.newArrayList(query2, query2b, query2d), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2d, queries, Lists.newArrayList(query2, query2c, query2b), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2d, queries, Lists.newArrayList(query2, query2c, query2b), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        queryEquivalence(query3, queries, Lists.newArrayList(query3b), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        atomicEquivalence(query3, queries, Lists.newArrayList(query3b, query5), ReasonerQueryEquivalence.AlphaEquivalence.atomicEquivalence(), graph);
        queryEquivalence(query3, queries, Lists.newArrayList(query3b, query5), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        queryEquivalence(query3b, queries, Lists.newArrayList(query3), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        atomicEquivalence(query3b, queries, Lists.newArrayList(query3, query5), ReasonerQueryEquivalence.AlphaEquivalence.atomicEquivalence(), graph);
        queryEquivalence(query3b, queries, Lists.newArrayList(query3, query5), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        queryEquivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        queryEquivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test //tests alpha-equivalence of resource atoms with different predicates
    public void testEquivalence_resourcesWithDifferentPredicates() {
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{$x has resource $r;$r > 1099;}";
        String query2 = "{$x has resource $r;$r < 1099;}";
        String query3 = "{$x has resource $r;$r == 1099;}";
        String query3b = "{$x has resource $r;$r '1099';}";
        String query4 = "{$x has resource $r;$r > $var;}";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query3b, query4);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, Collections.singletonList(query3b), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, Collections.singletonList(query3b), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3b, queries, Collections.singletonList(query3), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3b, queries, Collections.singletonList(query3), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_DifferentRelationInequivalentVariants(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();

        HashSet<String> queries = Sets.newHashSet(
                "{$x isa binary;}",
                "{($y) isa binary;}",

                "{($x, $y);}",
                "{($x, $y) isa binary;}",
                "{(baseRole1: $x, baseRole2: $y) isa binary;}",
                "{(role: $y, baseRole2: $z) isa binary;}",
                "{(role: $y, baseRole2: $z) isa $type;}",
                "{(role: $y, baseRole2: $z) isa $type; $type label binary;}",
                "{(role: $x, role: $x, baseRole2: $z) isa binary;}",

                "{$x ($y, $z) isa binary;}",
                "{$x (baseRole1: $y, baseRole2: $z) isa binary;}"
        );

        queries.forEach(qA -> {
            queries.stream()
                    .filter(qB -> !qA.equals(qB))
                    .forEach(qB -> {
                        equivalence(qA, qB, false, ReasonerQueryEquivalence.AlphaEquivalence, graph);
                        equivalence(qA, qB, false, ReasonerQueryEquivalence.StructuralEquivalence, graph);
                    });
        });
    }

    @Test
    public void testEquivalence_RelationWithRepeatingVariables(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{(baseRole1: $x, baseRole2: $y);}";
        String query2 = "{(baseRole1: $x, baseRole2: $x);}";

        equivalence(query, query2, false, ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, query2, false, ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_RelationsWithTypedRolePlayers(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{(role: $x, role: $y);$x isa baseRoleEntity;}";
        String query2 = "{(role: $x, role: $y);$y isa baseRoleEntity;}";
        String query3 = "{(role: $x, role: $y);$x isa subRoleEntity;}";
        String query4 = "{(role: $x, role: $y);$y isa baseRoleEntity;$x isa baseRoleEntity;}";
        String query5 = "{(baseRole1: $x, baseRole2: $y);$x isa baseRoleEntity;}";
        String query6 = "{(baseRole1: $x, baseRole2: $y);$y isa baseRoleEntity;}";
        String query7 = "{(baseRole1: $x, baseRole2: $y);$x isa baseRoleEntity;$y isa subRoleEntity;}";
        String query8 = "{(baseRole1: $x, baseRole2: $y);$x isa baseRoleEntity;$y isa baseRoleEntity;}";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query5, query6, query7, query8);

        equivalence(query, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, Collections.singletonList(query), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Collections.singletonList(query), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_RelationsWithSubstitution(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{(role: $x, role: $y);$x id 'V666';}";
        String queryb = "{(role: $x, role: $y);$y id 'V666';}";

        String query2 = "{(role: $x, role: $y);$x != $y;}";
        String query3 = "{(role: $x, role: $y);$x != $y;$y id 'V667';}";

        String query4 = "{(role: $x, role: $y);$x id 'V666';$y id 'V667';}";
        String query4b = "{(role: $x, role: $y);$y id 'V666';$x id 'V667';}";

        String query7 = "{(role: $x, role: $y);$x id 'V666';$y id 'V666';}";

        String query5 = "{(baseRole1: $x, baseRole2: $y);$x id 'V666';$y id 'V667';}";
        String query6 = "{(baseRole1: $x, baseRole2: $y);$y id 'V666';$x id 'V667';}";

        ArrayList<String> queries = Lists.newArrayList(query, queryb, query2, query3, query4, query4b, query5, query6, query7);

        equivalence(query, queries, Collections.singletonList(queryb), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, Collections.singletonList(queryb), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(queryb, queries, Collections.singletonList(query), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(queryb, queries, Collections.singletonList(query), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4, queries, Collections.singletonList(query4b), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4, queries, Lists.newArrayList(query4b, query7), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4b, queries, Collections.singletonList(query4), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4b, queries, Lists.newArrayList(query4, query7), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query5, queries, Collections.singletonList(query6), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query6, queries, Collections.singletonList(query5), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query7, queries, Lists.newArrayList(query4, query4b), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_RelationsWithSubstitution_differentRolesMapped(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{(baseRole1: $x, baseRole2: $y);$x id 'V666';}";
        String query2 = "{(baseRole1: $x, baseRole2: $y);$x id 'V667';}";
        String query3 = "{(baseRole1: $x, baseRole2: $y);$y id 'V666';}";
        String query4 = "{(baseRole1: $x, baseRole2: $y);$y id 'V667';}";

        String query5 = "{(baseRole1: $x, baseRole2: $y);$x != $y;}";
        String query6 = "{(baseRole1: $x, baseRole2: $y);$x != $x2;}";
        String query7 = "{(baseRole1: $x, baseRole2: $y);$x != $x2;$x2 id 'V667';}";

        String query8 = "{(baseRole1: $x, baseRole2: $y);$x id 'V666';$y id 'V667';}";
        String query9 = "{(baseRole1: $x, baseRole2: $y);$y id 'V666';$x id 'V667';}";
        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query5, query6, query7, query9, query9);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Collections.singletonList(query), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, Collections.singletonList(query4), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4, queries, Collections.singletonList(query3), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query8, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query8, queries, Collections.singletonList(query9), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_ResourcesAsRoleplayers(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{(baseRole1: $x, baseRole2: $y);$x == 'V666';}";
        String query2 = "{(baseRole1: $x, baseRole2: $y);$x == 'V667';}";

        String query3 = "{(baseRole1: $x, baseRole2: $y);$y == 'V666';}";
        String query4 = "{(baseRole1: $x, baseRole2: $y);$y == 'V667';}";

        String query5 = "{(baseRole1: $x, baseRole2: $y);$x !== $y;}";
        String query6 = "{(baseRole1: $x, baseRole2: $y);$x !== $x2;}";

        //TODO
        //String query7 = "{(baseRole1: $x, baseRole2: $y);$x !== $x2;$x2 == 'V667';}";

        String query8 = "{(baseRole1: $x, baseRole2: $y);$x == 'V666';$y == 'V667';}";
        String query9 = "{(baseRole1: $x, baseRole2: $y);$y == 'V666';$x == 'V667';}";
        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query5, query6,  query9, query9);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        //equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        //equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query8, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query8, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_RelationsWithVariableAndSubstitution(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String query = "{$r (baseRole1: $x);$x id 'V666';}";
        String query2 = "{$a (baseRole1: $x);$x id 'V667';}";
        String query3 = "{$b (baseRole2: $y);$y id 'V666';}";
        String query4 = "{$c (baseRole1: $a);$a != $b;}";
        String query4b = "{$r (baseRole1: $x);$x != $y;}";
        String query5 = "{$e (baseRole1: $a);$a != $b;$b id 'V666';}";
        String query5b = "{$r (baseRole1: $x);$x != $y;$y id 'V666';}";
        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query4b, query5, query5b);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query2, queries, Collections.singletonList(query), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4, queries, Collections.singletonList(query4b), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4, queries, Collections.singletonList(query4b), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query4b, queries, Collections.singletonList(query4), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query4b, queries, Collections.singletonList(query4), ReasonerQueryEquivalence.StructuralEquivalence, graph);

        equivalence(query5, queries, Collections.singletonList(query5b), ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query5, queries, Collections.singletonList(query5b), ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    private Concept getConceptByResourceValue(EmbeddedGraknTx<?> graph, String id){
        Set<Concept> instances = graph.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(Collectors.toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        return instances.iterator().next();
    }

    private void equivalence(String target, List<String> queries, List<String> equivalentQueries, ReasonerQueryEquivalence equiv, EmbeddedGraknTx graph){
        queries.forEach(q -> equivalence(target, q, equivalentQueries.contains(q) || q.equals(target), equiv, graph));
    }

    private void equivalence(String patternA, String patternB, boolean expectation, ReasonerQueryEquivalence equiv, EmbeddedGraknTx graph){
        ReasonerAtomicQuery a = ReasonerQueries.atomic(conjunction(patternA), graph);
        ReasonerAtomicQuery b = ReasonerQueries.atomic(conjunction(patternB), graph);
        queryEquivalence(a, b, expectation, equiv);
        atomicEquivalence(a.getAtom(), b.getAtom(), expectation, equiv.atomicEquivalence());
    }

    private void queryEquivalence(String target, List<String> queries, List<String> equivalentQueries, ReasonerQueryEquivalence equiv, EmbeddedGraknTx graph){
        queries.forEach(q -> queryEquivalence(target, q, equivalentQueries.contains(q) || q.equals(target), equiv, graph));
    }

    private void queryEquivalence(String patternA, String patternB, boolean queryExpectation, ReasonerQueryEquivalence equiv, EmbeddedGraknTx graph){
        ReasonerAtomicQuery a = ReasonerQueries.atomic(conjunction(patternA), graph);
        ReasonerAtomicQuery b = ReasonerQueries.atomic(conjunction(patternB), graph);
        queryEquivalence(a, b, queryExpectation, equiv);
    }

    private void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        singleQueryEquivalence(a, a, true, equiv);
        singleQueryEquivalence(b, b, true, equiv);
        singleQueryEquivalence(a, b, queryExpectation, equiv);
        singleQueryEquivalence(b, a, queryExpectation, equiv);
    }

    private void atomicEquivalence(String target, List<String> queries, List<String> equivalentQueries, AtomicEquivalence equiv, EmbeddedGraknTx graph){
        queries.forEach(q -> atomicEquivalence(target, q, equivalentQueries.contains(q) || q.equals(target), equiv, graph));
    }

    private void atomicEquivalence(String patternA, String patternB, boolean expectation, AtomicEquivalence equiv, EmbeddedGraknTx graph){
        ReasonerAtomicQuery a = ReasonerQueries.atomic(conjunction(patternA), graph);
        ReasonerAtomicQuery b = ReasonerQueries.atomic(conjunction(patternB), graph);
        atomicEquivalence(a.getAtom(), b.getAtom(), expectation, equiv);
    }

    private void atomicEquivalence(Atomic a, Atomic b, boolean expectation, AtomicEquivalence equiv){
        singleAtomicEquivalence(a, a, true, equiv);
        singleAtomicEquivalence(b, b, true, equiv);
        singleAtomicEquivalence(a, b, expectation, equiv);
        singleAtomicEquivalence(b, a, expectation, equiv);
    }

    private void singleQueryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        assertEquals(equiv.name() + " - Query: " + a.toString() + " =? " + b.toString(), queryExpectation, equiv.equivalent(a, b));

        //check hash additionally if need to be equal
        if (queryExpectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), true, equiv.hash(a) == equiv.hash(b));
        }
    }

    private void singleAtomicEquivalence(Atomic a, Atomic b, boolean expectation, AtomicEquivalence equivalence){
        assertEquals(equivalence.name() + " - Atom: " + a.toString() + " =? " + b.toString(), expectation,  equivalence.equivalent(a, b));

        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), equivalence.hash(a) == equivalence.hash(b), true);
        }
    }

    private Conjunction<VarPatternAdmin> conjunction(PatternAdmin pattern){
        Set<VarPatternAdmin> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString){
        Set<VarPatternAdmin> vars = Graql.parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

}
