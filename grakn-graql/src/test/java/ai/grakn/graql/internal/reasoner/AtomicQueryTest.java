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
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.reasoner;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
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
import ai.grakn.graql.internal.reasoner.unifier.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.unifier.UnifierType;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.kbs.GeoKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Iterator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.reasoner.TestQueryPattern.subList;
import static ai.grakn.graql.internal.reasoner.TestQueryPattern.subListExcluding;
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
    public static final SampleKBContext genericSchema = SampleKBContext.load("genericSchema.gql");

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
        EmbeddedGraknTx<?> graph = genericSchema.tx();
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

    private static Entity entity;
    private static Entity anotherEntity;
    private static Entity anotherBaseEntity;
    private static Entity subEntity;
    private static Relationship relation;
    private static Relationship anotherRelation;
    private static Attribute<Object> resource;
    private static Attribute<Object> anotherResource;

    @BeforeClass
    public static void setupGenericSchema(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        Iterator<Entity> entities = graph.getEntityType("baseRoleEntity").instances().collect(toSet()).iterator();
        entity = entities.next();
        anotherEntity = entities.next();
        anotherBaseEntity = graph.getEntityType("anotherBaseRoleEntity").instances().findFirst().orElse(null);
        subEntity = graph.getEntityType("subRoleEntity").instances().findFirst().orElse(null);
        Iterator<Relationship> relations = graph.getRelationshipType("baseRelation").subs().flatMap(RelationshipType::instances).iterator();
        relation = relations.next();
        anotherRelation = relations.next();
        Iterator<Attribute<Object>> resources = graph.getAttributeType("resource").instances().collect(toSet()).iterator();
        resource = resources.next();
        anotherResource = resources.next();
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithSubs(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();

        Concept x1 = getConceptByResourceValue(graph, "x1");
        Concept x2 = getConceptByResourceValue(graph, "x2");

        ReasonerAtomicQuery xbaseQuery = ReasonerQueries.atomic(conjunction("{($x1, $x2) isa binary;}"), graph);
        ReasonerAtomicQuery ybaseQuery = ReasonerQueries.atomic(conjunction("{($y1, $y2) isa binary;}"), graph);

        ConceptMap xAnswer = new ConceptMapImpl(ImmutableMap.of(var("x1"), x1, var("x2"), x2));
        ConceptMap flippedXAnswer = new ConceptMapImpl(ImmutableMap.of(var("x1"), x2, var("x2"), x1));

        ConceptMap yAnswer = new ConceptMapImpl(ImmutableMap.of(var("y1"), x1, var("y2"), x2));
        ConceptMap flippedYAnswer = new ConceptMapImpl(ImmutableMap.of(var("y1"), x2, var("y2"), x1));

        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(xbaseQuery, xAnswer);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(xbaseQuery, flippedXAnswer);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                var("x1"), var("x2"),
                var("x2"), var("x1")
        ));
        assertTrue(unifier.equals(correctUnifier));

        ReasonerAtomicQuery yChildQuery = ReasonerQueries.atomic(ybaseQuery, yAnswer);
        ReasonerAtomicQuery yChildQuery2 = ReasonerQueries.atomic(ybaseQuery, flippedYAnswer);

        MultiUnifier unifier2 = yChildQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier2 = new MultiUnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2")
        ));
        assertTrue(unifier2.equals(correctUnifier2));

        MultiUnifier unifier3 = yChildQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier3 = new MultiUnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x2"),
                var("y2"), var("x1")
        ));
        assertTrue(unifier3.equals(correctUnifier3));
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa twoRoleEntity;($x1, $x2) isa binary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa twoRoleEntity;($y1, $y2) isa binary;}"), graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
        MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2")
        ));
        assertTrue(unifier.equals(correctUnifier));
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa twoRoleEntity;$x2 isa twoRoleEntity2;($x1, $x2) isa binary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa twoRoleEntity;$y2 isa twoRoleEntity2;($y1, $y2) isa binary;}"), graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
        MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2")
        ));
        assertTrue(unifier.equals(correctUnifier));
    }

    @Test
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsRoles(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role1: $x, role1: $y, role2: $z) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), graph);

        MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery);
        MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery);
        MultiUnifier emptyUnifier3 = childQuery4.getMultiUnifier(parentQuery);

        assertTrue(emptyUnifier.equals(MultiUnifierImpl.nonExistent()));
        assertTrue(emptyUnifier2.equals(MultiUnifierImpl.nonExistent()));
        assertTrue(emptyUnifier3.equals(MultiUnifierImpl.nonExistent()));

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
        assertTrue(unifier.equals(correctUnifier));
        assertEquals(2, unifier.size());
    }

    @Test
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsMetaRoles_ParentRepeatsRPs(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role: $x, role: $x, role2: $y) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier = new MultiUnifierImpl(
                ImmutableMultimap.of(
                        var("q"), var("x"),
                        var("u"), var("x"),
                        var("v"), var("y"))
        );
        assertTrue(unifier.equals(correctUnifier));

        MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
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
        assertTrue(unifier2.equals(correctUnifier2));
        assertEquals(unifier2.size(), 2);

        MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier3 = new MultiUnifierImpl(
                ImmutableMultimap.of(
                        var("u"), var("x"),
                        var("v"), var("x"),
                        var("q"), var("y"))
        );
        assertTrue(unifier3.equals(correctUnifier3));

        MultiUnifier unifier4 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier4 = new MultiUnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("q"), var("y")
        ));
        assertTrue(unifier4.equals(correctUnifier4));
    }

    @Test
    public void testUnification_EXACT_TernaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.EXACT);
        MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.EXACT);
        MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2"),
                var("y3"), var("x3")
        ));
        assertTrue(unifier.equals(correctUnifier));
        assertTrue(unifier2.equals(MultiUnifierImpl.nonExistent()));
    }

    @Test
    public void testUnification_RULE_TernaryRelation_ParentRepeatsMetaRoles(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role: $x, role: $y, role2: $z) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
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
        assertTrue(unifier.equals(correctUnifier));
        assertEquals(unifier.size(), 2);

        MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
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
        assertTrue(unifier2.equals(correctUnifier2));
        assertEquals(unifier2.size(), 4);

        MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
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
        assertTrue(unifier3.equals(correctUnifier3));
        assertEquals(unifier3.size(), 2);

        MultiUnifier unifier4 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier4 = new MultiUnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("u"), var("y"),
                var("q"), var("z")
        ));
        assertTrue(unifier4.equals(correctUnifier4));
    }

    @Test
    public void testUnification_RULE_TernaryRelation_ParentRepeatsRoles_ParentRepeatsRPs(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role1: $x, role1: $x, role2: $y) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), graph);

        MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);

        assertTrue(emptyUnifier.isEmpty());
        assertTrue(emptyUnifier2.isEmpty());

        MultiUnifier unifier = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("v"), var("x"),
                var("q"), var("y")
        ));
        assertTrue(unifier.equals(correctUnifier));

        MultiUnifier unifier2 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier2 = new MultiUnifierImpl(ImmutableMultimap.of(
                var("u"), var("x"),
                var("q"), var("y")
        ));
        assertTrue(unifier2.equals(correctUnifier2));
    }

    @Test
    public void testUnification_RULE_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x2 isa threeRoleEntity2; $x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                var("y1"), var("x1"),
                var("y2"), var("x2"),
                var("y3"), var("x3")
        ));
        assertTrue(unifier.equals(correctUnifier));
        assertTrue(unifier2.equals(correctUnifier));
    }

    @Test // subSubThreeRoleEntity sub subThreeRoleEntity sub threeRoleEntity3
    public void testUnification_RULE_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes_TypeHierarchyInvolved(){
        EmbeddedGraknTx<?> graph =  unificationWithTypesSet.tx();
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x2 isa subThreeRoleEntity; $x3 isa subSubThreeRoleEntity;($x1, $x2, $x3) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;($y2, $y3, $y1) isa ternary;}"), graph);
        ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), graph);

        MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
        MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
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
        assertTrue(unifier.equals(correctUnifier));
        assertTrue(unifier2.equals(correctUnifier));
    }


    @Test
    public void testUnification_RULE_ResourcesWithTypes(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        String parentQuery = "{$x has resource $r; $x isa baseRoleEntity;}";

        String childQuery = "{$r has resource $x; $r isa subRoleEntity;}";
        String childQuery2 = "{$x1 has resource $x; $x1 isa subSubRoleEntity;}";
        String baseQuery = "{$r has resource $x; $r isa entity;}";

        unificationWithResultChecks(parentQuery, childQuery, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentQuery, childQuery2, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentQuery, baseQuery, true, true, true, UnifierType.RULE, graph);
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_MetaTypeParent(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa entity; $y isa entity;}";

        String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa baseRoleEntity; $v isa baseRoleEntity;}";
        String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa baseRoleEntity; $x isa baseRoleEntity;}";
        String specialisedRelation3 = "{(subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subRoleEntity;}";
        String specialisedRelation4 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subRoleEntity;}";
        String specialisedRelation5 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation6 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

        unificationWithResultChecks(parentRelation, specialisedRelation, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation5, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation6, false, false, true, UnifierType.RULE, graph);
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_BaseRoleParent(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        String baseParentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa baseRoleEntity; $y isa baseRoleEntity;}";
        String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa subSubRoleEntity; $y isa subSubRoleEntity;}";

        String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";
        String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

        unificationWithResultChecks(baseParentRelation, specialisedRelation, false, false, true, UnifierType.RULE,  graph);
        unificationWithResultChecks(baseParentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(baseParentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(baseParentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, graph);

        unificationWithResultChecks(parentRelation, specialisedRelation, false, false, false, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, false, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, false, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, false, UnifierType.RULE, graph);
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_BaseRoleParent_middleTypes(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa subRoleEntity; $y isa subRoleEntity;}";

        String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subSubRoleEntity;}";
        String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
        String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

        unificationWithResultChecks(parentRelation, specialisedRelation, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, graph);
        unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, graph);
    }

    @Test
    public void testUnification_differentRelationVariants_EXACT(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentRelationVariants.patternList(entity, anotherBaseEntity, subEntity);
        qs.forEach(q -> exactUnification(q, qs, new ArrayList<>(), graph));
    }

    @Test
    public void testUnification_differentRelationVariants_STRUCTURAL(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentRelationVariants.patternList(entity, anotherBaseEntity, subEntity);

        structuralUnification(qs.get(0), qs, new ArrayList<>(), graph);

        structuralUnification(qs.get(1), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(2), qs, subList(qs, Lists.newArrayList(3, 4)), graph);
        structuralUnification(qs.get(3), qs, subList(qs, Lists.newArrayList(2, 4)), graph);
        structuralUnification(qs.get(4), qs, subList(qs, Lists.newArrayList(2, 3)), graph);

        structuralUnification(qs.get(5), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(7, 8)), graph);
        structuralUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(6, 8)), graph);
        structuralUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(6, 7)), graph);

        structuralUnification(qs.get(9), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(10), qs, subList(qs, Lists.newArrayList(11)), graph);
        structuralUnification(qs.get(11), qs, subList(qs, Lists.newArrayList(10)), graph);
    }

    @Test
    public void testUnification_differentRelationVariants_RULE(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentRelationVariants.patternList(entity, anotherBaseEntity, subEntity);

        ruleUnification(qs.get(0), qs, qs, graph);

        ruleUnification(qs.get(1), qs, subListExcluding(qs, Lists.newArrayList(3)), graph);
        ruleUnification(qs.get(2), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 11)), graph);
        ruleUnification(qs.get(3), qs, subListExcluding(qs, Lists.newArrayList(1, 2, 4, 9, 10, 11)), graph);
        ruleUnification(qs.get(4), qs, subListExcluding(qs, Lists.newArrayList(2, 3, 10)), graph);

        ruleUnification(qs.get(5), qs, subListExcluding(qs, Lists.newArrayList(7, 9, 10, 11)), graph);
        ruleUnification(qs.get(6), qs, subListExcluding(qs, Lists.newArrayList(7, 8, 9, 10, 11)), graph);
        ruleUnification(qs.get(7), qs, subListExcluding(qs, Lists.newArrayList(5, 6, 8)), graph);
        ruleUnification(qs.get(8), qs, subListExcluding(qs, Lists.newArrayList(6, 7, 9, 10, 11)), graph);

        ruleUnification(qs.get(9), qs, subListExcluding(qs, Lists.newArrayList(3, 5, 6, 8)), graph);
        ruleUnification(qs.get(10), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 5, 6, 8, 11)), graph);
        ruleUnification(qs.get(11), qs, subListExcluding(qs, Lists.newArrayList(2, 3, 5, 6, 8, 10)), graph);
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_EXACT(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentRelationVariantsWithRelationVariable.patternList(entity, anotherBaseEntity, subEntity, relation, anotherRelation);

        exactUnification(qs.get(0), qs, subList(qs, Lists.newArrayList(1)), graph);
        exactUnification(qs.get(1), qs, subList(qs, Lists.newArrayList(0)), graph);
        subListExcluding(qs, Lists.newArrayList(0, 1)).forEach(q -> exactUnification(q, qs, new ArrayList<>(), graph));
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_STRUCTURAL(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentRelationVariantsWithRelationVariable.patternList(entity, anotherBaseEntity, subEntity, relation, anotherRelation);

        structuralUnification(qs.get(0), qs, subList(qs, Lists.newArrayList(1)), graph);
        structuralUnification(qs.get(1), qs, subList(qs, Lists.newArrayList(0)), graph);
        structuralUnification(qs.get(2), qs, new ArrayList<>(), graph);

        structuralUnification(qs.get(3), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(4), qs, subList(qs, Lists.newArrayList(5, 6)), graph);
        structuralUnification(qs.get(5), qs, subList(qs, Lists.newArrayList(4, 6)), graph);
        structuralUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(4, 5)), graph);

        structuralUnification(qs.get(7), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(9, 10)), graph);
        structuralUnification(qs.get(9), qs, subList(qs, Lists.newArrayList(8, 10)), graph);
        structuralUnification(qs.get(10), qs, subList(qs, Lists.newArrayList(8, 9)), graph);

        structuralUnification(qs.get(11), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(12), qs, subList(qs, Lists.newArrayList(13)), graph);
        structuralUnification(qs.get(13), qs, subList(qs, Lists.newArrayList(12)), graph);

        structuralUnification(qs.get(14), qs, subList(qs, Lists.newArrayList(15)), graph);
        structuralUnification(qs.get(15), qs, subList(qs, Lists.newArrayList(14)), graph);
        structuralUnification(qs.get(16), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(17), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(18), qs, new ArrayList<>(), graph);
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_RULE(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentRelationVariantsWithRelationVariable.patternList(entity, anotherBaseEntity, subEntity, relation, anotherRelation);

        ruleUnification(qs.get(0), qs, subList(qs, Lists.newArrayList(1)), graph);
        ruleUnification(qs.get(1), qs, subList(qs, Lists.newArrayList(0)), graph);
        ruleUnification(qs.get(2), qs, qs, graph);

        ruleUnification(qs.get(3), qs, subListExcluding(qs, Lists.newArrayList(5, 16)), graph);
        ruleUnification(qs.get(4), qs, subListExcluding(qs, Lists.newArrayList(5, 6, 13, 16)), graph);
        ruleUnification(qs.get(5), qs, subList(qs, Lists.newArrayList(0, 1, 2, 7, 8, 9, 10, 14, 15, 16, 17)), graph);
        ruleUnification(qs.get(6), qs, subListExcluding(qs, Lists.newArrayList(4, 5, 6, 12, 16, 18)), graph);

        ruleUnification(qs.get(7), qs, subListExcluding(qs, Lists.newArrayList(9, 11, 12, 13, 18)), graph);
        ruleUnification(qs.get(8), qs, subListExcluding(qs, Lists.newArrayList(9, 10, 11, 12, 13, 18)), graph);
        ruleUnification(qs.get(9), qs, subListExcluding(qs, Lists.newArrayList(7, 8, 9, 10, 17)), graph);
        ruleUnification(qs.get(10), qs, subListExcluding(qs, Lists.newArrayList(8, 9, 10, 11, 12, 13, 17, 18)), graph);

        ruleUnification(qs.get(11), qs, subListExcluding(qs, Lists.newArrayList(5, 7, 8, 10, 16, 17)), graph);
        ruleUnification(qs.get(12), qs, subListExcluding(qs, Lists.newArrayList(5, 6, 7, 8, 10, 13, 16, 17)), graph);
        ruleUnification(qs.get(13), qs, subListExcluding(qs, Lists.newArrayList(4, 5, 7, 8, 10, 12, 16, 17, 18)), graph);

        ruleUnification(qs.get(14), qs, subListExcluding(qs, Lists.newArrayList(15)), graph);
        ruleUnification(qs.get(15), qs, subListExcluding(qs, Lists.newArrayList(14, 16, 17, 18)), graph);

        ruleUnification(qs.get(16), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 6, 11, 12, 13, 15, 18)), graph);
        ruleUnification(qs.get(17), qs, subListExcluding(qs, Lists.newArrayList(9, 10, 11, 12, 13, 15, 18)), graph);
        ruleUnification(qs.get(18), qs, subListExcluding(qs, Lists.newArrayList(5, 6, 7, 8, 10, 13, 15, 16, 17)), graph);
    }

    @Test
    public void testUnification_differentTypeVariants_EXACT(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentTypeVariants.patternList(resource, anotherResource);
        subListExcluding(qs, Lists.newArrayList(3, 4, 7, 8)).forEach(q -> exactUnification(q, qs, new ArrayList<>(), graph));
        exactUnification(qs.get(3), qs, Collections.singletonList(qs.get(4)), graph);
        exactUnification(qs.get(4), qs, Collections.singletonList(qs.get(3)), graph);
        exactUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(8)), graph);
        exactUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(7)), graph);
    }

    @Test
    public void testUnification_differentTypeVariants_STRUCTURAL(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentTypeVariants.patternList(resource, anotherResource);
        subListExcluding(qs, Lists.newArrayList(3, 4, 6, 7, 8)).forEach(q -> structuralUnification(q, qs, new ArrayList<>(), graph));
    
        structuralUnification(qs.get(3), qs, Collections.singletonList(qs.get(4)), graph);
        structuralUnification(qs.get(4), qs, Collections.singletonList(qs.get(3)), graph);

        structuralUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(7, 8)), graph);
        structuralUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(6, 8)), graph);
        structuralUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(6, 7)), graph);
    }

    @Test
    public void testUnification_differentTypeVariants_RULE(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentTypeVariants.patternList(resource, anotherResource);

        ruleUnification(qs.get(0), qs, qs, graph);
        ruleUnification(qs.get(1), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 13, 14, 15, 16, 17, 18)), graph);
        ruleUnification(qs.get(2), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 13, 14, 15, 16, 17, 18)), graph);
        ruleUnification(qs.get(3), qs, subListExcluding(qs, Lists.newArrayList(1, 2, 6, 7, 8, 9, 10, 11, 12)), graph);
        ruleUnification(qs.get(4), qs, subListExcluding(qs, Lists.newArrayList(1, 2, 6, 7, 8, 9, 10, 11, 12)), graph);
        ruleUnification(qs.get(5), qs, qs, graph);

        ruleUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 9, 10, 11, 12)), graph);
        ruleUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 8, 9, 10, 11, 12)), graph);
        ruleUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 7, 9, 10, 11, 12)), graph);

        ruleUnification(qs.get(9), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 6, 7, 8, 11)), graph);
        ruleUnification(qs.get(10), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 6, 7, 8, 11, 12)), graph);

        ruleUnification(qs.get(11), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 6, 7, 8, 9, 10, 12)), graph);
        ruleUnification(qs.get(12), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 6, 7, 8, 10, 11)), graph);

        ruleUnification(qs.get(13), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 16, 17, 18)), graph);
        ruleUnification(qs.get(14), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 15, 17, 18)), graph);

        ruleUnification(qs.get(15), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 14, 16, 17, 18)), graph);
        ruleUnification(qs.get(16), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 13, 15, 17, 18)), graph);
        ruleUnification(qs.get(17), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 13, 14, 15, 16, 18)), graph);
        ruleUnification(qs.get(18), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 13, 14, 15, 16, 17)), graph);
    }

    @Test
    public void testUnification_differentResourceVariants_EXACT(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentResourceVariants.patternList(entity, anotherEntity, resource, anotherResource);

        exactUnification(qs.get(0), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(1), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(2), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(3), qs, new ArrayList<>(), graph);

        exactUnification(qs.get(4), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(5), qs, new ArrayList<>(), graph);

        exactUnification(qs.get(6), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(7), qs, new ArrayList<>(), graph);

        exactUnification(qs.get(8), qs, Collections.singletonList(qs.get(10)), graph);
        exactUnification(qs.get(9), qs, Collections.singletonList(qs.get(11)), graph);
        exactUnification(qs.get(10), qs, Collections.singletonList(qs.get(8)), graph);
        exactUnification(qs.get(11), qs, Collections.singletonList(qs.get(9)), graph);

        exactUnification(qs.get(12), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(13), qs, new ArrayList<>(), graph);

        exactUnification(qs.get(14), qs, Collections.singletonList(qs.get(16)), graph);
        exactUnification(qs.get(15), qs, Collections.singletonList(qs.get(17)), graph);
        exactUnification(qs.get(16), qs, Collections.singletonList(qs.get(14)), graph);
        exactUnification(qs.get(17), qs, Collections.singletonList(qs.get(15)), graph);

        exactUnification(qs.get(18), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(19), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(20), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(21), qs, new ArrayList<>(), graph);

        exactUnification(qs.get(22), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(23), qs, Collections.singletonList(qs.get(24)), graph);
        exactUnification(qs.get(24), qs, Collections.singletonList(qs.get(23)), graph);

        exactUnification(qs.get(25), qs, subList(qs, Lists.newArrayList(26)), graph);
        exactUnification(qs.get(26), qs, subList(qs, Lists.newArrayList(25)), graph);

        exactUnification(qs.get(27), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(28), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(29), qs, new ArrayList<>(), graph);
        exactUnification(qs.get(30), qs, new ArrayList<>(), graph);
    }

    @Test
    public void testUnification_differentResourceVariants_STRUCTURAL(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentResourceVariants.patternList(entity, anotherEntity, resource, anotherResource);

        structuralUnification(qs.get(0), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(1), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(2), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(3), qs, new ArrayList<>(), graph);

        structuralUnification(qs.get(4), qs, Collections.singletonList(qs.get(5)), graph);
        structuralUnification(qs.get(5), qs, Collections.singletonList(qs.get(4)), graph);

        structuralUnification(qs.get(6), qs, Collections.singletonList(qs.get(7)), graph);
        structuralUnification(qs.get(7), qs, Collections.singletonList(qs.get(6)), graph);

        structuralUnification(qs.get(8), qs, Collections.singletonList(qs.get(10)), graph);
        structuralUnification(qs.get(9), qs, Collections.singletonList(qs.get(11)), graph);
        structuralUnification(qs.get(10), qs, Collections.singletonList(qs.get(8)), graph);
        structuralUnification(qs.get(11), qs, Collections.singletonList(qs.get(9)), graph);

        structuralUnification(qs.get(12), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(13), qs, new ArrayList<>(), graph);

        structuralUnification(qs.get(14), qs, Collections.singletonList(qs.get(16)), graph);
        structuralUnification(qs.get(15), qs, Collections.singletonList(qs.get(17)), graph);
        structuralUnification(qs.get(16), qs, Collections.singletonList(qs.get(14)), graph);
        structuralUnification(qs.get(17), qs, Collections.singletonList(qs.get(15)), graph);

        structuralUnification(qs.get(18), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(19), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(20), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(21), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(22), qs, new ArrayList<>(), graph);

        structuralUnification(qs.get(23), qs, Collections.singletonList(qs.get(24)), graph);
        structuralUnification(qs.get(24), qs, Collections.singletonList(qs.get(23)), graph);
        structuralUnification(qs.get(25), qs, subList(qs, Lists.newArrayList(26, 29)), graph);
        structuralUnification(qs.get(26), qs, subList(qs, Lists.newArrayList(25, 29)), graph);
        structuralUnification(qs.get(27), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(28), qs, new ArrayList<>(), graph);
        structuralUnification(qs.get(29), qs, subList(qs, Lists.newArrayList(25, 26)), graph);
        structuralUnification(qs.get(30), qs, new ArrayList<>(), graph);
    }

    @Test
    public void testUnification_differentResourceVariants_RULE(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        List<String> qs = TestQueryPattern.differentResourceVariants.patternList(entity, anotherEntity, resource, anotherResource);

        ruleUnification(qs.get(0), qs, subList(qs, Lists.newArrayList(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)), graph);
        ruleUnification(qs.get(1), qs, subListExcluding(qs, Lists.newArrayList(0, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 28)), graph);
        ruleUnification(qs.get(2), qs, subListExcluding(qs, Lists.newArrayList(0, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 28, 30)), graph);
        ruleUnification(qs.get(3), qs, subListExcluding(qs, Lists.newArrayList(28)), graph);

        ruleUnification(qs.get(4), qs, subList(qs, Lists.newArrayList(0, 3, 6, 7, 8, 9, 10, 11, 12, 13)), graph);
        ruleUnification(qs.get(5), qs, subList(qs, Lists.newArrayList(0, 3, 6, 7, 8, 9, 10, 11, 12, 13)), graph);

        ruleUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 8, 9, 10, 11, 12, 13)), graph);
        ruleUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 8, 9, 10, 11, 12, 13)), graph);

        ruleUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 10, 12)), graph);
        ruleUnification(qs.get(9), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 11, 12, 13)), graph);
        ruleUnification(qs.get(10), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 8, 12)), graph);
        ruleUnification(qs.get(11), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 9, 12, 13)), graph);

        ruleUnification(qs.get(12), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13)), graph);
        ruleUnification(qs.get(13), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 9, 11, 12 )), graph);

        ruleUnification(qs.get(14), qs, subList(qs, Lists.newArrayList(1, 2, 3, 16, 19, 20, 21)), graph);
        ruleUnification(qs.get(15), qs, subList(qs, Lists.newArrayList(1, 2, 3, 17, 18, 20, 21)), graph);
        ruleUnification(qs.get(16), qs, subList(qs, Lists.newArrayList(1, 2, 3, 14, 19, 20, 21)), graph);
        ruleUnification(qs.get(17), qs, subList(qs, Lists.newArrayList(1, 2, 3, 15, 18, 20, 21)), graph);

        ruleUnification(qs.get(18), qs, subList(qs, Lists.newArrayList(1, 2, 3, 15, 17, 19, 20, 21, 22, 23, 24, 25, 26, 27, 29, 30)), graph);
        ruleUnification(qs.get(19), qs, subList(qs, Lists.newArrayList(1, 2, 3, 14, 16, 18, 20, 21)), graph);
        ruleUnification(qs.get(20), qs, subList(qs, Lists.newArrayList(1, 2, 3, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 29, 30)), graph);
        ruleUnification(qs.get(21), qs, subList(qs, Lists.newArrayList(1, 2, 3, 14, 15, 16, 17, 18, 19, 20)), graph);

        ruleUnification(qs.get(22), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 29, 30)), graph);
        ruleUnification(qs.get(23), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 29)), graph);
        ruleUnification(qs.get(24), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 29)), graph);

        ruleUnification(qs.get(25), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 27, 29)), graph);
        ruleUnification(qs.get(26), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 27, 29)), graph);

        ruleUnification(qs.get(27), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 25, 26, 29)), graph);
        ruleUnification(qs.get(28), subListExcluding(qs, Lists.newArrayList(28)), new ArrayList<>(), graph);
        ruleUnification(qs.get(29), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 27, 29)), graph);
        ruleUnification(qs.get(30), qs, subList(qs, Lists.newArrayList(1, 3, 18, 20, 22)), graph);
    }

    private void unification(String child, List<String> queries, List<String> queriesWithUnifier, UnifierType unifierType, EmbeddedGraknTx graph){
        queries.forEach(parent -> unification(child, parent, queriesWithUnifier.contains(parent) || parent.equals(child), unifierType, graph));
    }

    private void structuralUnification(String child, List<String> queries, List<String> queriesWithUnifier, EmbeddedGraknTx graph){
        unification(child, queries, queriesWithUnifier, UnifierType.STRUCTURAL, graph);
    }

    private void exactUnification(String child, List<String> queries, List<String> queriesWithUnifier, EmbeddedGraknTx graph){
        unification(child, queries, queriesWithUnifier, UnifierType.EXACT, graph);
    }

    private void ruleUnification(String child, List<String> queries, List<String> queriesWithUnifier, EmbeddedGraknTx graph){
        unification(child, queries, queriesWithUnifier, UnifierType.RULE, graph);
    }


    private MultiUnifier unification(String childString, String parentString, boolean unifierExists, UnifierType unifierType, EmbeddedGraknTx graph){
        ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childString), graph);
        ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentString), graph);

        if (unifierType.equivalence() != null) queryEquivalence(child, parent, unifierExists, unifierType.equivalence());
        MultiUnifier multiUnifier = child.getMultiUnifier(parent, unifierType);
        assertEquals("Unexpected unifier: " + multiUnifier + " between the child - parent pair:\n" + child + " :\n" + parent, unifierExists, !multiUnifier.isEmpty());
        if (unifierExists && unifierType != UnifierType.RULE){
            MultiUnifier multiUnifierInverse = parent.getMultiUnifier(child, unifierType);
            assertEquals("Unexpected unifier inverse: " + multiUnifier + " between the child - parent pair:\n" + parent + " :\n" + child, unifierExists, !multiUnifierInverse.isEmpty());
            assertEquals(multiUnifierInverse, multiUnifier.inverse());
        }
        return multiUnifier;
    }
    
    /**
     * checks the correctness and uniqueness of an EXACT unifier required to unify child query with parent
     * @param parentString parent query string
     * @param childString child query string
     * @param checkInverse flag specifying whether the inverse equality u^{-1}=u(parent, child) of the unifier u(child, parent) should be checked
     * @param ignoreTypes flag specifying whether the types should be disregarded and only role players checked for containment
     * @param checkEquality if true the parent and child answers will be checked for equality, otherwise they are checked for containment of child answers in parent
     */
    private void unificationWithResultChecks(String parentString, String childString, boolean checkInverse, boolean checkEquality, boolean ignoreTypes, UnifierType unifierType, EmbeddedGraknTx<?> graph){
        ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childString), graph);
        ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentString), graph);
        Unifier unifier = unification(childString, parentString, true, unifierType, graph).getUnifier();

        List<ConceptMap> childAnswers = child.getQuery().execute();
        List<ConceptMap> unifiedAnswers = childAnswers.stream()
                .map(a -> a.unify(unifier))
                .filter(a -> !a.isEmpty())
                .collect(Collectors.toList());
        List<ConceptMap> parentAnswers = parent.getQuery().execute();

        if (checkInverse) {
            Unifier inverse = parent.getMultiUnifier(child, unifierType).getUnifier();
            assertEquals(unifier.inverse(), inverse);
            assertEquals(unifier, inverse.inverse());
        }

        assertTrue(!childAnswers.isEmpty());
        assertTrue(!unifiedAnswers.isEmpty());
        assertTrue(!parentAnswers.isEmpty());

        Set<Var> parentNonTypeVariables = Sets.difference(parent.getAtom().getVarNames(), Sets.newHashSet(parent.getAtom().getPredicateVariable()));
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
                Set<Var> childNonTypeVariables = Sets.difference(child.getAtom().getVarNames(), Sets.newHashSet(child.getAtom().getPredicateVariable()));
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


    /**
     * ##################################
     *
     *     Equivalence Tests
     *
     * ##################################
     */

    @Test
    public void testEquivalence_DifferentIsaVariants(){
        testEquivalence_DifferentTypeVariants(genericSchema.tx(), "isa", "baseRoleEntity", "subRoleEntity");
    }

    @Test
    public void testEquivalence_DifferentSubVariants(){
        testEquivalence_DifferentTypeVariants(genericSchema.tx(), "sub", "baseRoleEntity", "baseRole1");
    }

    @Test
    public void testEquivalence_DifferentPlaysVariants(){
        testEquivalence_DifferentTypeVariants(genericSchema.tx(), "plays", "baseRole1", "baseRole2");
    }

    @Test
    public void testEquivalence_DifferentRelatesVariants(){
        testEquivalence_DifferentTypeVariants(genericSchema.tx(), "relates", "baseRole1", "baseRole2");
    }

    @Test
    public void testEquivalence_DifferentHasVariants(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
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

    @Test
    public void testEquivalence_TypesWithSameLabel(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
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
    public void testEquivalence_DifferentRelationInequivalentVariants(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();

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
        EmbeddedGraknTx<?> graph = genericSchema.tx();
        String query = "{(baseRole1: $x, baseRole2: $y);}";
        String query2 = "{(baseRole1: $x, baseRole2: $x);}";

        equivalence(query, query2, false, ReasonerQueryEquivalence.AlphaEquivalence, graph);
        equivalence(query, query2, false, ReasonerQueryEquivalence.StructuralEquivalence, graph);
    }

    @Test
    public void testEquivalence_RelationsWithTypedRolePlayers(){
        EmbeddedGraknTx<?> graph = genericSchema.tx();
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
        EmbeddedGraknTx<?> graph = genericSchema.tx();
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
        EmbeddedGraknTx<?> graph = genericSchema.tx();
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
        EmbeddedGraknTx<?> graph = genericSchema.tx();
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
        EmbeddedGraknTx<?> graph = genericSchema.tx();
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
