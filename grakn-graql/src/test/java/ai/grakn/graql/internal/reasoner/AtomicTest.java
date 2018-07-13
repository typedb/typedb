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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.IsaAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtils;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicTest {

    @ClassRule
    public static final SampleKBContext roleInferenceSet = SampleKBContext.load("roleInferenceTest.gql");

    @ClassRule
    public static final SampleKBContext ruleApplicabilitySet = SampleKBContext.load("ruleApplicabilityTest.gql");

    @ClassRule
    public static final SampleKBContext resourceApplicabilitySet = SampleKBContext.load("resourceApplicabilityTest.gql");

    @ClassRule
    public static final SampleKBContext reifiedResourceApplicabilitySet = SampleKBContext.load("reifiedResourceApplicabilityTest.gql");

    @ClassRule
    public static final SampleKBContext unificationTestSet = SampleKBContext.load("unificationTest.gql");

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Test
    public void testAtomsAreCorrectlyIdentifiedAsRecursive(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String recRelString = "{($x, $y) isa binary;}";
        String nrecRelString = "{($x, $y) isa ternary;}";
        ReasonerAtomicQuery recQuery = ReasonerQueries.atomic(conjunction(recRelString, graph), graph);
        ReasonerAtomicQuery nrecQuery = ReasonerQueries.atomic(conjunction(nrecRelString, graph), graph);
        assertTrue(recQuery.getAtom().isRecursive());
        assertTrue(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testAtomFactoryProducesAtomsOfCorrectType(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String atomString = "{$x isa baseRoleEntity;}";
        String relString = "{($x, $y, $z) isa binary;}";
        String resString = "{$x has resource 'value';}";

        Atom atom = ReasonerQueries.atomic(conjunction(atomString, graph), graph).getAtom();
        Atom relation = ReasonerQueries.atomic(conjunction(relString, graph), graph).getAtom();
        Atom res = ReasonerQueries.atomic(conjunction(resString, graph), graph).getAtom();

        assertTrue(atom.isType());
        assertTrue(relation.isRelation());
        assertTrue(res.isResource());
    }

    /**
     * ##################################
     *
     *       Equality Tests
     *
     * ##################################
     */

    @Test
    public void testEquality_DifferentIsaVariants(){
        testEquality_DifferentTypeVariants(unificationTestSet.tx(), "isa", "baseRoleEntity", "subRoleEntity");
    }

    @Test
    public void testEquality_DifferentSubVariants(){
        testEquality_DifferentTypeVariants(unificationTestSet.tx(), "sub", "baseRoleEntity", "role1");
    }

    @Test
    public void testEquality_DifferentPlaysVariants(){
        testEquality_DifferentTypeVariants(unificationTestSet.tx(), "plays", "role1", "role2");
    }

    @Test
    public void testEquality_DifferentRelatesVariants(){
        testEquality_DifferentTypeVariants(unificationTestSet.tx(), "relates", "role1", "role2");
    }

    @Test
    public void testEquality_DifferentHasVariants(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String patternString = "{$x has resource;}";
        String patternString2 = "{$y has resource;}";
        String patternString3 = "{$x has " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + ";}";

        atomicEquality(patternString, patternString, true, graph);
        atomicEquality(patternString, patternString2, false, graph);
        atomicEquality(patternString, patternString3, false, graph);
        atomicEquality(patternString2, patternString3, false, graph);
    }

    @Test
    public void testEquivalence_DifferentRelationVariants(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String pattern = "{(role1: $x, role2: $y) isa binary;}";
        String directPattern = "{(role1: $x, role2: $y) isa! binary;}";
        String pattern2 = "{$r (role1: $x, role2: $y) isa binary;}";
        String pattern3 = "{$z (role1: $x, role2: $y) isa binary;}";
        String pattern4 = "{(role1: $x, role2: $y);}";
        String pattern5 = "{(role1: $z, role2: $v) isa binary;}";
        String pattern6 = "{(role: $x, role2: $y) isa binary;}";
        String pattern7 = "{(role1: $x, role2: $y) isa $type;}";
        String pattern8 = "{(role1: $x, role2: $y) isa $type;$type label binary;}";

        atomicEquality(pattern, pattern, true, graph);
        atomicEquality(pattern, directPattern, false, graph);
        atomicEquality(pattern, pattern2, false, graph);
        atomicEquality(pattern, pattern3, false, graph);
        atomicEquality(pattern, pattern4, false, graph);
        atomicEquality(pattern, pattern5, false, graph);
        atomicEquality(pattern, pattern6, false, graph);
        atomicEquality(pattern, pattern7, false, graph);
        atomicEquality(pattern, pattern8, false, graph);
    }

    private void testEquality_DifferentTypeVariants(EmbeddedGraknTx<?> graph, String keyword, String label, String label2){
        String variantAString = "{$x " + keyword + " " + label + ";}";
        String variantAString2 = "{$y " + keyword + " " + label + ";}";
        String variantAString3 = "{$y " + keyword + " " + label2 + ";}";
        atomicEquality(variantAString, variantAString, true, graph);
        atomicEquality(variantAString, variantAString2, false, graph);
        atomicEquality(variantAString2, variantAString3, false, graph);

        String variantBString = "{$x " + keyword + " $type;$type label " + label +";}";
        String variantBString2 = "{$x " + keyword + " $type;$type label " + label2 +";}";
        String variantBString3 = "{$x " + keyword + " $var;$var label " + label +";}";
        String variantBString4 = "{$y " + keyword + " $type;$type label " + label +";}";
        atomicEquality(variantBString, variantBString, true, graph);
        atomicEquality(variantBString, variantBString2, false, graph);
        atomicEquality(variantBString, variantBString3, true, graph);
        atomicEquality(variantBString, variantBString4, false, graph);

        String variantCString = "{$x " + keyword + " $y;}";
        String variantCString2 = "{$x " + keyword + " $z;}";
        atomicEquality(variantCString, variantCString, true, graph);
        atomicEquality(variantCString, variantCString2, true, graph);

        atomicEquality(variantAString, variantBString, true, graph);
        atomicEquality(variantAString, variantCString, false, graph);
        atomicEquality(variantBString, variantCString, false, graph);
    }

    private void atomicEquality(String patternA, String patternB, boolean expectation, EmbeddedGraknTx<?> graph){
        Atom atomA = ReasonerQueries.atomic(conjunction(patternA, graph), graph).getAtom();
        Atom atomB = ReasonerQueries.atomic(conjunction(patternB, graph), graph).getAtom();
        atomicEquality(atomA, atomA, true);
        atomicEquality(atomB, atomB, true);
        atomicEquality(atomA, atomB, expectation);
        atomicEquality(atomB, atomA, expectation);
    }

    private void atomicEquality(Atomic a, Atomic b, boolean expectation){
        assertEquals("Atomic: " + a.toString() + " =? " + b.toString(), a.equals(b), expectation);

        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), a.hashCode() == b.hashCode(), true);
        }
    }

    /**
     * ##################################
     *
     *       ROLE INFERENCE Tests
     *
     * ##################################
     */

    @Test
    public void testRoleInference_TypedBinaryRelation(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{($x, $y); $x isa entity1; $y isa entity2;}";
        String patternString2 = "{($x, $y) isa binary; $x isa entity1; $y isa entity2;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role2"), var("y"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test
    public void testRoleInference_TypedBinaryRelation_SingleTypeMissing(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{($x, $y); $x isa entity1;}";
        String patternString2 = "{($x, $y) isa binary; $x isa entity1;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role"), var("y"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test //each type maps to a specific role
    public void testRoleInference_TypedTernaryRelationWithKnownRole(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{($x, $y, role3: $z);$x isa entity1;$y isa entity2;}";
        String patternString2 = "{($x, $y, role3: $z) isa ternary;$x isa entity1;$y isa entity2;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role2"), var("y"),
                graph.getRole("role3"), var("z"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test //without cardinality constraints the $y variable can be mapped to any of the three roles hence metarole is assigned
    public void testRoleInference_TypedTernaryRelation(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{($x, $y, $z);$x isa entity1;$y isa entity2;}";
        String patternString2 = "{($x, $y, $z) isa ternary;$x isa entity1;$y isa entity2;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role2"), var("y"),
                graph.getRole("role"), var("z"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test
    public void testRoleInference_TernaryRelationWithRepeatingRolePlayers(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{(role1: $x, role2: $y, $y);}";
        String patternString2 = "{(role1: $x, role2: $y, $y) isa ternary;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role2"), var("y"),
                graph.getRole("role"), var("y"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesPlaySubRoles_SubRolesAreCorrectlyIdentified(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{(role: $x, role: $y, role: $z); $x isa anotherEntity1; $y isa anotherEntity2; $z isa anotherEntity3;}";
        String patternString2 = "{(role: $x, role: $y, role: $z) isa ternary; $x isa anotherEntity1; $y isa anotherEntity2; $z isa anotherEntity3;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("subRole1"), var("x"),
                graph.getRole("subRole2"), var("y"),
                graph.getRole("subRole3"), var("z"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test
    public void testRoleInference_TypedTernaryRelationWithMetaRoles_MetaRolesShouldBeOverwritten(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{(role: $x, role: $y, role: $z); $x isa entity1; $y isa entity2; $z isa entity3;}";
        String patternString2 = "{(role: $x, role: $y, role: $z) isa ternary; $x isa entity1; $y isa entity2; $z isa entity3;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role2"), var("y"),
                graph.getRole("role3"), var("z"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesAreSubTypes_TopRolesShouldBeChosen(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{(role: $x, role: $y, role: $z); $x isa subEntity1; $y isa subEntity2; $z isa subEntity3;}";
        String patternString2 = "{(role: $x, role: $y, role: $z) isa ternary; $x isa subEntity1; $y isa subEntity2; $z isa subEntity3;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role2"), var("y"),
                graph.getRole("role3"), var("z"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesCanPlayMultipleRoles_MetaRoleIsChosen(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String patternString = "{($x, $y, $z); $x isa genericEntity; $y isa genericEntity; $z isa genericEntity;}";
        String patternString2 = "{($x, $y, $z) isa ternary; $x isa genericEntity; $y isa genericEntity; $z isa genericEntity;}";

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        roleInference(patternString, correctRoleMap, graph);
        roleInference(patternString2, correctRoleMap, graph);
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_NoInformationPresent(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String relationString = "{($x, $y);}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_MetaRelationType(){
        EmbeddedGraknTx<?> graph = roleInferenceSet.tx();
        String relationString = "{($x, $y) isa relationship;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
    }

    @Test //missing role is ambiguous without cardinality constraints
    public void testRoleInference_RoleHierarchyInvolved() {
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String relationString = "{($p, subRole2: $gc) isa binary;}";
        String relationString2 = "{(subRole1: $gp, $p) isa binary;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        RelationshipAtom relation2 = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(relation.getRoleVarMap());
        Multimap<Role, Var> roleMap2 = roleSetMap(relation2.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("p"),
                graph.getRole("subRole2"), var("gc"));
        ImmutableSetMultimap<Role, Var> correctRoleMap2 = ImmutableSetMultimap.of(
                graph.getRole("role"), var("p"),
                graph.getRole("subRole1"), var("gp"));
        assertEquals(correctRoleMap, roleMap);
        assertEquals(correctRoleMap2, roleMap2);
    }

    @Test //entity1 plays role1 but entity2 plays roles role1, role2 hence ambiguous and metarole has to be assigned, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRoleInference_WithMetaType(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z) isa ternary;$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa entity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
    }

    @Test //entity1 plays role1, entity2 plays 2 roles, entity3 plays 3 roles hence ambiguous and metarole has to be assigned, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRoleInference_RoleMappingUnambiguous(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z) isa ternary;$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa threeRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_AllRolePlayersHaveAmbiguousRoles(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z) isa ternary;$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
    }

    @Test //relation relates a single role so instead of assigning metarole this role should be assigned
    public void testRoleInference_RelationHasVerticalRoleHierarchy(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y) isa reifying-relation;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role1"), var("y"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
    }

    /**
     * ##################################
     *
     *     RULE APPLICABILITY Tests
     *
     * ##################################
     */

    @Test
    public void testRuleApplicability_OntologicalAtomsDoNotMatchAnyRules(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        Atom subAtom = ReasonerQueries.atomic(conjunction("{$x sub relationship;}", graph), graph).getAtom();
        Atom hasAtom = ReasonerQueries.atomic(conjunction("{$x has description;}", graph), graph).getAtom();
        Atom relatesAtom = ReasonerQueries.atomic(conjunction("{reifiable-relation relates $x;}", graph), graph).getAtom();
        Atom relatesAtom2 = ReasonerQueries.atomic(conjunction("{$x relates role1;}", graph), graph).getAtom();
        Atom playsAtom = ReasonerQueries.atomic(conjunction("{$x plays role1;}", graph), graph).getAtom();
        assertThat(subAtom.getApplicableRules().collect(toSet()), empty());
        assertThat(hasAtom.getApplicableRules().collect(toSet()), empty());
        assertThat(relatesAtom.getApplicableRules().collect(toSet()), empty());
        assertThat(relatesAtom2.getApplicableRules().collect(toSet()), empty());
        assertThat(playsAtom.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_AmbiguousRoleMapping(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
        String relationString = "{($x, $y, $z);$x isa singleRoleEntity; $y isa anotherTwoRoleEntity; $z isa twoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap((relation.getRoleVarMap())));
        assertEquals(3, relation.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_AmbiguousRoleMapping_RolePlayerTypeMismatch(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
        String relationString = "{($x, $y, $z);$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa threeRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        assertEquals(2, relation.getApplicableRules().count());
    }

    @Test //threeRoleEntity subs twoRoleEntity -> (role, role, role)
    public void testRuleApplicability_AmbiguousRoleMapping_TypeHierarchyEnablesExtraRule(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
        assertEquals(2, relation.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_MissingRelationPlayers(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();

        //inferred relation (role {role2, role3} : $x, role {role1, role2} : $y)
        String relationString = "{($x, $y);$x isa twoRoleEntity; $y isa anotherTwoRoleEntity;}";

        //inferred relation: (role1: $x, role1: $y)
        String relationString2 = "{($x, $y);$x isa singleRoleEntity; $y isa singleRoleEntity;}";

        //inferred relation: (role1: $x, role {role1, role2}: $y)
        String relationString3 = "{($x, $y);$x isa singleRoleEntity; $y isa anotherTwoRoleEntity;}";

        //inferred relation: (role1: $x, role {role1, role2, role3}: $y)
        String relationString4 = "{($x, $y);$x isa singleRoleEntity; $y isa threeRoleEntity;}";

        //inferred relation: (role {role2, role3}: $x, role {role2, role3}: $y)
        String relationString5 = "{($x, $y);$x isa twoRoleEntity; $y isa twoRoleEntity;}";

        //inferred relation: (role {role1, role2}: $x, role {role1, role2}: $y)
        String relationString6 = "{($x, $y);$x isa anotherTwoRoleEntity; $y isa anotherTwoRoleEntity;}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, graph), graph).getAtom();
        Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, graph), graph).getAtom();
        Atom relation5 = ReasonerQueries.atomic(conjunction(relationString5, graph), graph).getAtom();
        Atom relation6 = ReasonerQueries.atomic(conjunction(relationString6, graph), graph).getAtom();

        assertEquals(4, relation.getApplicableRules().count());
        assertThat(relation2.getApplicableRules().collect(toSet()), empty());
        assertEquals(4, relation3.getApplicableRules().count());
        assertEquals(3, relation4.getApplicableRules().count());
        assertThat(relation5.getApplicableRules().collect(toSet()), empty());
        assertEquals(4, relation6.getApplicableRules().count());
    }

    @Test //should assign (role : $x, role1: $y, role: $z) which is compatible with 3 ternary rules
    public void testRuleApplicability_WithWildcard(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
        String relationString = "{($x, $y, $z);$y isa singleRoleEntity; $z isa twoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        assertEquals(3, relation.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_TypedResources(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{$x isa reifiable-relation; $x has description $d;}";
        String relationString2 = "{$x isa typed-relation; $x has description $d;}";
        String relationString3 = "{$x isa relationship; $x has description $d;}";
        Atom resource = ReasonerQueries.create(conjunction(relationString, graph), graph).getAtoms(ResourceAtom.class).findFirst().orElse(null);
        Atom resource2 = ReasonerQueries.create(conjunction(relationString2, graph), graph).getAtoms(ResourceAtom.class).findFirst().orElse(null);
        Atom resource3 = ReasonerQueries.create(conjunction(relationString3, graph), graph).getAtoms(ResourceAtom.class).findFirst().orElse(null);
        assertEquals(2, resource.getApplicableRules().count());
        assertEquals(2, resource2.getApplicableRules().count());
        assertEquals(3, resource3.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_DerivedTypes(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String typeString = "{$x isa reifying-relation;}";
        String typeString2 = "{$x isa typed-relation;}";
        String typeString3 = "{$x isa description;}";
        String typeString4 = "{$x isa attribute;}";
        String typeString5 = "{$x isa relationship;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, graph), graph).getAtom();
        Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, graph), graph).getAtom();
        Atom type4 = ReasonerQueries.atomic(conjunction(typeString4, graph), graph).getAtom();
        Atom type5 = ReasonerQueries.atomic(conjunction(typeString5, graph), graph).getAtom();

        List<InferenceRule> rules = RuleUtils.getRules(graph).map(r -> new InferenceRule(r, graph)).collect(Collectors.toList());
        assertEquals(2, type.getApplicableRules().count());
        assertEquals(1, type2.getApplicableRules().count());
        assertEquals(3, type3.getApplicableRules().count());
        assertEquals(rules.stream().filter(r -> r.getHead().getAtom().isResource()).count(), type4.getApplicableRules().count());
        assertEquals(rules.stream().filter(r -> r.getHead().getAtom().isRelation()).count(), type5.getApplicableRules().count());
    }

    @Test //should assign (role: $x, role: $y) which is compatible with all rules
    public void testRuleApplicability_genericRelation(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(
                RuleUtils.getRules(graph).count(),
                relation.getApplicableRules().count()
        );
    }

    @Test
    public void testRuleApplicability_genericType(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String typeString = "{$x isa $type;}";
        String typeString2 = "{$x isa $type;$x isa entity;}";
        String typeString3 = "{$x isa $type;$x isa relationship;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, graph), graph).getAtom();
        Atom type3 = ReasonerQueries.create(conjunction(typeString3, graph), graph).getAtoms(IsaAtom.class).filter(at -> at.getSchemaConcept() == null).findFirst().orElse(null);

        assertEquals(RuleUtils.getRules(graph).count(), type.getApplicableRules().count());
        assertThat(type2.getApplicableRules().collect(toSet()), empty());
        assertEquals(RuleUtils.getRules(graph).filter(r -> r.thenTypes().allMatch(Concept::isRelationshipType)).count(), type3.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_genericTypeAsARoleplayer(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String typeString = "{(symmetricRole: $x); $x isa $type;}";
        String typeString2 = "{(role1: $x); $x isa $type;}";
        Atom type = ReasonerQueries.create(conjunction(typeString, graph), graph).getAtoms(IsaAtom.class).findFirst().orElse(null);
        Atom type2 = ReasonerQueries.create(conjunction(typeString2, graph), graph).getAtoms(IsaAtom.class).findFirst().orElse(null);
        assertThat(type.getApplicableRules().collect(toSet()), empty());
        assertEquals(
                RuleUtils.getRules(graph).filter(r -> r.thenTypes().anyMatch(c -> c.label().equals(Label.of("binary")))).count(),
                type2.getApplicableRules().count()
        );
    }

    @Test
    public void testRuleApplicability_genericTypeWithBounds(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{$x isa $type;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(
                RuleUtils.getRules(graph).count(),
                relation.getApplicableRules().count()
        );
    }

    @Test
    public void testRuleApplicability_WithWildcard_MissingMappings(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
        String relationString = "{($x, $y, $z);$y isa singleRoleEntity; $z isa singleRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test //NB: role2 sub role1
    public void testRuleApplicability_RepeatingRoleTypesWithHierarchy(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{(role1: $x1, role1: $x2, role2: $x3);}";
        String relationString2 = "{(role1: $x1, role2: $x2, role2: $x3);}";
        String relationString3 = "{(role2: $x1, role2: $x2, role2: $x3);}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().count());
        assertEquals(1, relation2.getApplicableRules().count());
        assertThat(relation3.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_genericRelationWithGenericType(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);$x isa noRoleEntity;}";
        String relationString2 = "{($x, $y);$x isa entity;}";
        String relationString3 = "{($x, $y);$x isa relationship;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Atom relation3 = ReasonerQueries.create(conjunction(relationString3, graph), graph).getAtoms(RelationshipAtom.class).findFirst().orElse(null);

        assertEquals(5, relation.getApplicableRules().count());
        assertEquals(RuleUtils.getRules(graph).filter(r -> r.thenTypes().allMatch(Concept::isRelationshipType)).count(), relation2.getApplicableRules().count());

        //TODO not filtered correctly
        //assertEquals(RuleUtils.getRules(graph).filter(r -> r.thenTypes().allMatch(Concept::isAttributeType)).count(), relation3.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_ReifiedRelationsWithType(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{(role1: $x, role2: $y) isa reifying-relation;}";
        String relationString2 = "{$x isa entity;(role1: $x, role2: $y) isa reifying-relation;}";
        String relationString3 = "{$x isa anotherTwoRoleEntity;(role1: $x, role2: $y) isa reifying-relation;}";
        String relationString4 = "{$x isa twoRoleEntity;(role1: $x, role2: $y) isa reifying-relation;}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, graph), graph).getAtom();
        Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().count());
        assertEquals(2, relation2.getApplicableRules().count());
        assertEquals(1, relation3.getApplicableRules().count());
        assertThat(relation4.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_TypePlayabilityDeterminesApplicability(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String relationString = "{$y isa singleRoleEntity;(role1:$x, role:$y, role3: $z) isa ternary;}";
        String relationString2 = "{$y isa twoRoleEntity;(role1:$x, role2:$y, role3: $z) isa ternary;}";
        String relationString3 = "{$y isa anotherTwoRoleEntity;(role1:$x, role2:$y, role3: $z) isa ternary;}";
        String relationString4 = "{$y isa noRoleEntity;(role1:$x, role2:$y, role3: $z) isa ternary;}";
        String relationString5 = "{$y isa entity;(role1:$x, role2:$y, role3: $z) isa ternary;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, graph), graph).getAtom();
        Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, graph), graph).getAtom();
        Atom relation5 = ReasonerQueries.atomic(conjunction(relationString5, graph), graph).getAtom();

        assertEquals(1, relation.getApplicableRules().count());
        assertThat(relation2.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, relation3.getApplicableRules().count());
        assertEquals(1, relation4.getApplicableRules().count());
        assertEquals(1, relation5.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_TypeRelation(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String typeString = "{$x isa reifying-relation;}";
        String typeString2 = "{$x isa ternary;}";
        String typeString3 = "{$x isa binary;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, graph), graph).getAtom();
        Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, graph), graph).getAtom();
        assertEquals(2, type.getApplicableRules().count());
        assertEquals(2, type2.getApplicableRules().count());
        assertEquals(1, type3.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_OntologicalTypes(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        String typeString = "{$x sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + ";}";
        String typeString2 = "{$x relates role1;}";
        String typeString3 = "{$x plays role1;}";
        String typeString4 = "{$x has name;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, graph), graph).getAtom();
        Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, graph), graph).getAtom();
        Atom type4 = ReasonerQueries.atomic(conjunction(typeString4, graph), graph).getAtom();
        assertThat(type.getApplicableRules().collect(toSet()), empty());
        assertThat(type2.getApplicableRules().collect(toSet()), empty());
        assertThat(type3.getApplicableRules().collect(toSet()), empty());
        assertThat(type4.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_InstancesMakeRuleInapplicable_NoRoleTypes(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        Concept concept = getConcept(graph, "name", "noRoleEntity");
        String relationString = "{" +
                "($x, $y) isa ternary;" +
                "$x id '" + concept.id().getValue() + "';" +
                "}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_InstancesMakeRuleInapplicable_NoRoleTypes_NoRelationType(){
        EmbeddedGraknTx<?> graph = ruleApplicabilitySet.tx();
        Concept concept = getConcept(graph, "name", "noRoleEntity");
        String relationString = "{" +
                "($x, $y);" +
                "$x id '" + concept.id().getValue() + "';" +
                "}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_ResourceDouble(){
        EmbeddedGraknTx<?> graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-double > 3.0;}";
        String resourceString2 = "{$x has res-double > 4.0;}";
        String resourceString3 = "{$x has res-double < 3.0;}";
        String resourceString4 = "{$x has res-double < 4.0;}";
        String resourceString5 = "{$x has res-double >= 5;}";
        String resourceString6 = "{$x has res-double <= 5;}";
        String resourceString7 = "{$x has res-double == 3.14;}";
        String resourceString8 = "{$x has res-double !== 5;}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, graph), graph).getAtom();
        Atom resource5 = ReasonerQueries.atomic(conjunction(resourceString5, graph), graph).getAtom();
        Atom resource6 = ReasonerQueries.atomic(conjunction(resourceString6, graph), graph).getAtom();
        Atom resource7 = ReasonerQueries.atomic(conjunction(resourceString7, graph), graph).getAtom();
        Atom resource8 = ReasonerQueries.atomic(conjunction(resourceString8, graph), graph).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource4.getApplicableRules().count());
        assertThat(resource5.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource6.getApplicableRules().count());
        assertEquals(1, resource7.getApplicableRules().count());
        assertEquals(1, resource8.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_ResourceLong(){
        EmbeddedGraknTx<?> graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-long > 100;}";
        String resourceString2 = "{$x has res-long > 150;}";
        String resourceString3 = "{$x has res-long < 100;}";
        String resourceString4 = "{$x has res-long < 200;}";
        String resourceString5 = "{$x has res-long >= 130;}";
        String resourceString6 = "{$x has res-long <= 130;}";
        String resourceString7 = "{$x has res-long == 123;}";
        String resourceString8 = "{$x has res-long !== 200;}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, graph), graph).getAtom();
        Atom resource5 = ReasonerQueries.atomic(conjunction(resourceString5, graph), graph).getAtom();
        Atom resource6 = ReasonerQueries.atomic(conjunction(resourceString6, graph), graph).getAtom();
        Atom resource7 = ReasonerQueries.atomic(conjunction(resourceString7, graph), graph).getAtom();
        Atom resource8 = ReasonerQueries.atomic(conjunction(resourceString8, graph), graph).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource4.getApplicableRules().count());
        assertThat(resource5.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource6.getApplicableRules().count());
        assertEquals(1, resource7.getApplicableRules().count());
        assertEquals(1, resource8.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_ResourceString(){
        EmbeddedGraknTx<?> graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-string contains 'ing';}";
        String resourceString2 = "{$x has res-string 'test';}";
        String resourceString3 = "{$x has res-string /.*(fast|string).*/;}";
        String resourceString4 = "{$x has res-string /.*/;}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, graph), graph).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource3.getApplicableRules().count());
        assertEquals(1, resource4.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_ResourceBoolean(){
        EmbeddedGraknTx<?> graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-boolean 'true';}";
        String resourceString2 = "{$x has res-boolean 'false';}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_TypeResource(){
        EmbeddedGraknTx<?> graph = resourceApplicabilitySet.tx();
        String typeString = "{$x isa resource;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        assertEquals(1, type.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_Resource_TypeMismatch(){
        EmbeddedGraknTx<?> graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x isa entity1, has resource $r;}";
        String resourceString2 = "{$x isa entity2, has resource $r;}";
        String resourceString3 = "{$x isa entity2, has resource 'test';}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_ReifiedResourceDouble(){
        EmbeddedGraknTx<?> graph = reifiedResourceApplicabilitySet.tx();
        String queryString = "{$x isa res-double > 3.0;($x, $y);}";
        String queryString2 = "{$x isa res-double > 4.0;($x, $y);}";
        String queryString3 = "{$x isa res-double < 3.0;($x, $y);}";
        String queryString4 = "{$x isa res-double < 4.0;($x, $y);}";
        String queryString5 = "{$x isa res-double >= 5;($x, $y);}";
        String queryString6 = "{$x isa res-double <= 5;($x, $y);}";
        String queryString7 = "{$x isa res-double == 3.14;($x, $y);}";
        String queryString8 = "{$x isa res-double !== 5;($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, graph), graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, graph), graph).getAtom();
        Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, graph), graph).getAtom();
        Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, graph), graph).getAtom();
        Atom atom5 = ReasonerQueries.atomic(conjunction(queryString5, graph), graph).getAtom();
        Atom atom6 = ReasonerQueries.atomic(conjunction(queryString6, graph), graph).getAtom();
        Atom atom7 = ReasonerQueries.atomic(conjunction(queryString7, graph), graph).getAtom();
        Atom atom8 = ReasonerQueries.atomic(conjunction(queryString8, graph), graph).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        assertThat(atom3.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom4.getApplicableRules().count());
        assertThat(atom5.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom6.getApplicableRules().count());
        assertEquals(1, atom7.getApplicableRules().count());
        assertEquals(1, atom8.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_ReifiedResourceLong(){
        EmbeddedGraknTx<?> graph = reifiedResourceApplicabilitySet.tx();
        String queryString = "{$x isa res-long > 100;($x, $y);}";
        String queryString2 = "{$x isa res-long > 150;($x, $y);}";
        String queryString3 = "{$x isa res-long < 100;($x, $y);}";
        String queryString4 = "{$x isa res-long < 200;($x, $y);}";
        String queryString5 = "{$x isa res-long >= 130;($x, $y);}";
        String queryString6 = "{$x isa res-long <= 130;($x, $y);}";
        String queryString7 = "{$x isa res-long == 123;($x, $y);}";
        String queryString8 = "{$x isa res-long !== 200;($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, graph), graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, graph), graph).getAtom();
        Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, graph), graph).getAtom();
        Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, graph), graph).getAtom();
        Atom atom5 = ReasonerQueries.atomic(conjunction(queryString5, graph), graph).getAtom();
        Atom atom6 = ReasonerQueries.atomic(conjunction(queryString6, graph), graph).getAtom();
        Atom atom7 = ReasonerQueries.atomic(conjunction(queryString7, graph), graph).getAtom();
        Atom atom8 = ReasonerQueries.atomic(conjunction(queryString8, graph), graph).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        assertThat(atom3.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom4.getApplicableRules().count());
        assertThat(atom5.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom6.getApplicableRules().count());
        assertEquals(1, atom7.getApplicableRules().count());
        assertEquals(1, atom8.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_ReifiedResourceString(){
        EmbeddedGraknTx<?> graph = reifiedResourceApplicabilitySet.tx();
        String queryString = "{$x isa res-string contains 'val';($x, $y);}";
        String queryString2 = "{$x isa res-string 'test';($x, $y);}";
        String queryString3 = "{$x isa res-string /.*(fast|value).*/;($x, $y);}";
        String queryString4 = "{$x isa res-string /.*/;($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, graph), graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, graph), graph).getAtom();
        Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, graph), graph).getAtom();
        Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, graph), graph).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom3.getApplicableRules().count());
        assertEquals(1, atom4.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_ReifiedResourceBoolean(){
        EmbeddedGraknTx<?> graph = reifiedResourceApplicabilitySet.tx();
        String queryString = "{$x isa res-boolean 'true';($x, $y);}";
        String queryString2 = "{$x isa res-boolean 'false';($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, graph), graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, graph), graph).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
    }

    /**
     * ##################################
     *
     *      UNIFICATION Tests
     *
     * ##################################
     */

    @Test
    public void testUnification_RelationWithRolesExchanged(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String relation = "{(role1: $x, role2: $y) isa binary;}";
        String relation2 = "{(role1: $y, role2: $x) isa binary;}";
        exactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRole(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String relation = "{(role1: $x, role: $y) isa binary;}";
        String relation2 = "{(role1: $y, role: $x) isa binary;}";
        exactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithRelationVar(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String relation = "{$x (role1: $r, role2: $z) isa binary;}";
        String relation2 = "{$r (role1: $x, role2: $y) isa binary;}";
        exactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRolesAndIds(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        Concept instance = graph.graql().<GetQuery>parse("match $x isa subRoleEntity; get;").execute().iterator().next().get(var("x"));
        String relation = "{(role: $x, role: $y) isa binary; $y id '" + instance.id().getValue() + "';}";
        String relation2 = "{(role: $z, role: $v) isa binary; $z id '" + instance.id().getValue() + "';}";
        String relation3 = "{(role: $z, role: $v) isa binary; $v id '" + instance.id().getValue() + "';}";

        exactUnification(relation, relation2, true, true, graph);
        exactUnification(relation, relation3, true, true, graph);
        exactUnification(relation2, relation3, true, true, graph);
    }

    @Test
    public void testUnification_BinaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentRelation = "{(role1: $x, role2: $y);}";
        String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v);}";
        String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x);}";
        String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v);}";
        String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x);}";
        String specialisedRelation5 = "{(subRole1: $u, anotherSubRole1: $v);}";

        exactUnification(parentRelation, specialisedRelation, false, false, graph);
        exactUnification(parentRelation, specialisedRelation2, false, false, graph);
        exactUnification(parentRelation, specialisedRelation3, false, false, graph);
        exactUnification(parentRelation, specialisedRelation4, false, false, graph);
        nonExistentUnifier(parentRelation, specialisedRelation5, graph);
    }

    @Test
    public void testUnification_BinaryRelationWithRoleHierarchy_ParentWithSubRoles(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentRelation = "{(subRole1: $x, subRole2: $y);}";
        String specialisedRelation = "{(subRole1: $u, subSubRole2: $v);}";
        String specialisedRelation2 = "{(subRole1: $y, subSubRole2: $x);}";
        String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v);}";
        String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x);}";
        String specialisedRelation5 = "{(subSubRole1: $u, role3: $v);}";
        String specialisedRelation6 = "{(role1: $u, role2: $v);}";

        exactUnification(parentRelation, specialisedRelation, false, false, graph);
        exactUnification(parentRelation, specialisedRelation2, false, false, graph);
        exactUnification(parentRelation, specialisedRelation3, false, false, graph);
        exactUnification(parentRelation, specialisedRelation4, false, false, graph);
        nonExistentUnifier(parentRelation, specialisedRelation5, graph);
        nonExistentUnifier(parentRelation, specialisedRelation6, graph);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentRelation = "{(role1: $x, role2: $y, role3: $z);}";
        String specialisedRelation = "{(role1: $u, subRole2: $v, subSubRole3: $q);}";
        String specialisedRelation2 = "{(role1: $z, subRole2: $y, subSubRole3: $x);}";
        String specialisedRelation3 = "{(subRole1: $u, subRole2: $v, subSubRole3: $q);}";
        String specialisedRelation4 = "{(subRole1: $y, subRole2: $z, subSubRole3: $x);}";
        String specialisedRelation5 = "{(subRole1: $u, subRole1: $v, subSubRole3: $q);}";

        exactUnification(parentRelation, specialisedRelation, false, true, graph);
        exactUnification(parentRelation, specialisedRelation2, false, true, graph);
        exactUnification(parentRelation, specialisedRelation3, false, false, graph);
        exactUnification(parentRelation, specialisedRelation4, false, false, graph);
        nonExistentUnifier(parentRelation, specialisedRelation5, graph);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithSubRoles(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentRelation = "{(subRole1: $x, subRole2: $y, subRole3: $z);}";
        String specialisedRelation = "{(role1: $u, subRole2: $v, subSubRole3: $q);}";
        String specialisedRelation2 = "{(subRole1: $u, subRole2: $v, subSubRole3: $q);}";
        String specialisedRelation3 = "{(subRole1: $y, subRole2: $z, subSubRole3: $x);}";
        String specialisedRelation4 = "{(subSubRole1: $u, subRole2: $v, subSubRole3: $q);}";
        String specialisedRelation5 = "{(subSubRole1: $y, subRole2: $z, subSubRole3: $x);}";
        String specialisedRelation6 = "{(subRole1: $u, subRole1: $v, subSubRole3: $q);}";

        nonExistentUnifier(parentRelation, specialisedRelation, graph);
        exactUnification(parentRelation, specialisedRelation2, false, false, graph);
        exactUnification(parentRelation, specialisedRelation3, false, false, graph);
        exactUnification(parentRelation, specialisedRelation4, false, false, graph);
        exactUnification(parentRelation, specialisedRelation5, false, false, graph);
        nonExistentUnifier(parentRelation, specialisedRelation6, graph);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles_childrenRepeatRolePlayers(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentRelation = "{(role1: $x, role2: $y, role3: $z);}";
        String specialisedRelation = "{(role1: $u, subRole2: $u, subSubRole3: $q);}";
        String specialisedRelation2 = "{(role1: $y, subRole2: $y, subSubRole3: $x);}";
        String specialisedRelation3 = "{(subRole1: $u, subRole2: $u, subSubRole3: $q);}";
        String specialisedRelation4 = "{(subRole1: $y, subRole2: $y, subSubRole3: $x);}";
        String specialisedRelation5 = "{(subRole1: $u, subRole1: $u, subSubRole3: $q);}";

        exactUnification(parentRelation, specialisedRelation, false, false, graph);
        exactUnification(parentRelation, specialisedRelation2, false, false, graph);
        exactUnification(parentRelation, specialisedRelation3, false, false, graph);
        exactUnification(parentRelation, specialisedRelation4, false, false, graph);
        nonExistentUnifier(parentRelation, specialisedRelation5, graph);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles_parentRepeatRolePlayers(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentRelation = "{(role1: $x, role2: $x, role3: $y);}";
        String specialisedRelation = "{(role1: $u, subRole2: $v, subSubRole3: $q);}";
        String specialisedRelation2 = "{(role1: $z, subRole2: $y, subSubRole3: $x);}";
        String specialisedRelation3 = "{(subRole1: $u, subRole2: $v, subSubRole3: $q);}";
        String specialisedRelation4 = "{(subRole1: $y, subRole2: $y, subSubRole3: $x);}";
        String specialisedRelation5 = "{(subRole1: $u, subRole1: $v, subSubRole3: $q);}";

        exactUnification(parentRelation, specialisedRelation, false, false, graph);
        exactUnification(parentRelation, specialisedRelation2, false, false, graph);
        exactUnification(parentRelation, specialisedRelation3, false, false, graph);
        exactUnification(parentRelation, specialisedRelation4, false, false, graph);
        nonExistentUnifier(parentRelation, specialisedRelation5, graph);
    }

    @Test
    public void testUnification_VariousResourceAtoms(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String resource = "{$x has resource $r;$r 'f';}";
        String resource2 = "{$r has resource $x;$x 'f';}";
        String resource3 = "{$r has resource 'f';}";
        String resource4 = "{$x has resource $y via $r;$y 'f';}";
        String resource5 = "{$y has resource $r via $x;$r 'f';}";
        exactUnification(resource, resource2, true, true, graph);
        exactUnification(resource, resource3, true, true, graph);
        exactUnification(resource2, resource3, true, true, graph);
        exactUnification(resource4, resource5, true, true, graph);
    }

    @Test
    public void testUnification_VariousTypeAtoms(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String type = "{$x isa baseRoleEntity;}";
        String type2 = "{$y isa baseRoleEntity;}";
        String userDefinedType = "{$y isa $x;$x label 'baseRoleEntity';}";
        String userDefinedType2 = "{$u isa $v;$v label 'baseRoleEntity';}";

        exactUnification(type, type2, true, true, graph);
        exactUnification(userDefinedType, userDefinedType2, true, true, graph);
        //TODO user defined-generated test
        //exactUnification(type, userDefinedType, true, true, graph);
    }

    @Test
    public void testUnification_ParentHasFewerRelationPlayers() {
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String childString = "{(subRole1: $y, subRole2: $x) isa binary;}";
        String parentString = "{(subRole1: $x) isa binary;}";
        String parentString2 = "{(subRole2: $y) isa binary;}";

        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(childString, graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction(parentString, graph), graph);
        ReasonerAtomicQuery parentQuery2 = ReasonerQueries.atomic(conjunction(parentString2, graph), graph);

        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();
        Atom parentAtom2 = parentQuery2.getAtom();

        List<Answer> childAnswers = childQuery.getQuery().execute();
        List<Answer> parentAnswers = parentQuery.getQuery().execute();
        List<Answer> parentAnswers2 = parentQuery2.getQuery().execute();

        Unifier unifier = childAtom.getUnifier(parentAtom);
        Unifier unifier2 = childAtom.getUnifier(parentAtom2);

        assertCollectionsEqual(
                parentAnswers,
                childAnswers.stream()
                        .map(a -> a.unify(unifier))
                        .map(a -> a.project(parentQuery.getVarNames()))
                        .distinct()
                        .collect(Collectors.toList())
        );
        assertCollectionsEqual(
                parentAnswers2,
                childAnswers.stream()
                        .map(a -> a.unify(unifier2))
                        .map(a -> a.project(parentQuery2.getVarNames()))
                        .distinct()
                        .collect(Collectors.toList())
        );
    }

    @Test
    public void testUnification_ResourceWithIndirectValuePredicate(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String resource = "{$x has resource $r;$r 'f';}";
        String resource2 = "{$r has resource $x;$x 'f';}";
        String resource3 = "{$r has resource 'f';}";

        ReasonerAtomicQuery resourceQuery = ReasonerQueries.atomic(conjunction(resource, graph), graph);
        ReasonerAtomicQuery resourceQuery2 = ReasonerQueries.atomic(conjunction(resource2, graph), graph);
        ReasonerAtomicQuery resourceQuery3 = ReasonerQueries.atomic(conjunction(resource3, graph), graph);

        String type = "{$x isa resource;$x id '" + resourceQuery.getQuery().execute().iterator().next().get("r").id().getValue()  + "';}";
        ReasonerAtomicQuery typeQuery = ReasonerQueries.atomic(conjunction(type, graph), graph);
        Atom typeAtom = typeQuery.getAtom();

        Atom resourceAtom = resourceQuery.getAtom();
        Atom resourceAtom2 = resourceQuery2.getAtom();
        Atom resourceAtom3 = resourceQuery3.getAtom();

        Unifier unifier = resourceAtom.getUnifier(typeAtom);
        Unifier unifier2 = resourceAtom2.getUnifier(typeAtom);
        Unifier unifier3 = resourceAtom3.getUnifier(typeAtom);

        Answer typeAnswer = typeQuery.getQuery().execute().iterator().next();
        Answer resourceAnswer = resourceQuery.getQuery().execute().iterator().next();
        Answer resourceAnswer2 = resourceQuery2.getQuery().execute().iterator().next();
        Answer resourceAnswer3 = resourceQuery3.getQuery().execute().iterator().next();

        assertEquals(typeAnswer.get(var("x")), resourceAnswer.unify(unifier).get(var("x")));
        assertEquals(typeAnswer.get(var("x")), resourceAnswer2.unify(unifier2).get(var("x")));
        assertEquals(typeAnswer.get(var("x")), resourceAnswer3.unify(unifier3).get(var("x")));
    }

    @Test
    public void testRewriteAndUnification(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentString = "{$r (subRole1: $x) isa binary;}";
        Atom parentAtom = ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();
        Var parentVarName = parentAtom.getVarName();

        String childPatternString = "(subRole1: $x, subRole2: $y) isa binary";
        InferenceRule testRule = new InferenceRule(
                graph.putRule("Checking Rewrite & Unification",
                        graph.graql().parser().parsePattern(childPatternString),
                        graph.graql().parser().parsePattern(childPatternString)),
                graph)
                .rewrite(parentAtom);

        RelationshipAtom headAtom = (RelationshipAtom) testRule.getHead().getAtom();
        Var headVarName = headAtom.getVarName();

        Unifier unifier = Iterables.getOnlyElement(testRule.getMultiUnifier(parentAtom));
        Unifier correctUnifier = new UnifierImpl(
                ImmutableMap.of(
                        var("x"), var("x"),
                        headVarName, parentVarName)
        );

        assertTrue(unifier.containsAll(correctUnifier));

        Multimap<Role, Var> roleMap = roleSetMap(headAtom.getRoleVarMap());
        Collection<Var> wifeEntry = roleMap.get(graph.getRole("subRole1"));
        assertEquals(wifeEntry.size(), 1);
        assertEquals(wifeEntry.iterator().next(), var("x"));
    }

    @Test
    public void testUnification_MatchAllParentAtom(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        String parentString = "{$r($a, $x);}";
        String childString = "{$rel (role1: $z, role2: $b) isa binary;}";
        Atom parent = ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();
        Atom child = ReasonerQueries.atomic(conjunction(childString, graph), graph).getAtom();

        MultiUnifier multiUnifier = child.getMultiUnifier(parent, UnifierType.RULE);
        Unifier correctUnifier = new UnifierImpl(
                ImmutableMap.of(
                        var("z"), var("a"),
                        var("b"), var("x"),
                        child.getVarName(), parent.getVarName())
        );
        Unifier correctUnifier2 = new UnifierImpl(
                ImmutableMap.of(
                        var("z"), var("x"),
                        var("b"), var("a"),
                        child.getVarName(), parent.getVarName())
        );
        assertEquals(multiUnifier.size(), 2);
        multiUnifier.forEach(u -> assertTrue(u.containsAll(correctUnifier) || u.containsAll(correctUnifier2)));
    }

    @Test
    public void testUnification_IndirectRoles(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        VarPatternAdmin basePattern = var()
                .rel(var("role1").label("subRole1"), var("y1"))
                .rel(var("role2").label("subSubRole2"), var("y2"))
                .isa("binary")
                .admin();

        ReasonerAtomicQuery baseQuery = ReasonerQueries.atomic(Patterns.conjunction(Sets.newHashSet(basePattern)), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries
                .atomic(conjunction(
                                "{($r1: $x1, $r2: $x2) isa binary;" +
                                "$r1 label 'subRole1';" +
                                "$r2 label 'subSubRole2';}"
                        , graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries
                .atomic(conjunction(
                                "{($R1: $x, $R2: $y) isa binary;" +
                                "$R1 label 'subRole1';" +
                                "$R2 label 'subSubRole2';}"
                        , graph), graph);
        exactUnification(parentQuery, childQuery, true, true);
        exactUnification(baseQuery, parentQuery, true, true);
        exactUnification(baseQuery, childQuery, true, true);
    }

    @Test
    public void testUnification_IndirectRoles_NoRelationType(){
        EmbeddedGraknTx<?> graph = unificationTestSet.tx();
        VarPatternAdmin basePattern = var()
                .rel(var("role1").label("subRole1"), var("y1"))
                .rel(var("role2").label("subSubRole2"), var("y2"))
                .admin();

        ReasonerAtomicQuery baseQuery = ReasonerQueries.atomic(Patterns.conjunction(Sets.newHashSet(basePattern)), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries
                .atomic(conjunction(
                                "{($r1: $x1, $r2: $x2);" +
                                "$r1 label 'subRole1';" +
                                "$r2 label 'subSubRole2';}"
                        , graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries
                .atomic(conjunction(
                                "{($R1: $x, $R2: $y);" +
                                "$R1 label 'subRole1';" +
                                "$R2 label 'subSubRole2';}"
                        , graph), graph);
        exactUnification(parentQuery, childQuery, true, true);
        exactUnification(baseQuery, parentQuery, true, true);
        exactUnification(baseQuery, childQuery, true, true);
    }

    private void roleInference(String patternString, ImmutableSetMultimap<Role, Var> expectedRoleMAp, EmbeddedGraknTx<?> graph){
        RelationshipAtom atom = (RelationshipAtom) ReasonerQueries.atomic(conjunction(patternString, graph), graph).getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());
        assertEquals(expectedRoleMAp, roleMap);

    }

    /**
     * checks that the child query is not unifiable with parent - a unifier does not exist
     * @param parentQuery parent query
     * @param childQuery child query
     */
    private void nonExistentUnifier(ReasonerAtomicQuery parentQuery, ReasonerAtomicQuery childQuery){
        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();
        assertTrue(childAtom.getMultiUnifier(parentAtom, UnifierType.EXACT).isEmpty());
    }

    private void nonExistentUnifier(String parentPatternString, String childPatternString, EmbeddedGraknTx<?> graph){
        nonExistentUnifier(
                ReasonerQueries.atomic(conjunction(parentPatternString, graph), graph),
                ReasonerQueries.atomic(conjunction(childPatternString, graph), graph)
        );
    }

    /**
     * checks the correctness and uniqueness of an exact unifier required to unify child query with parent
     * @param parentQuery parent query
     * @param childQuery child query
     * @param checkInverse flag specifying whether the inverse equality u^{-1}=u(parent, child) of the unifier u(child, parent) should be checked
     * @param checkEquality if true the parent and child answers will be checked for equality, otherwise they are checked for containment of child answers in parent
     */
    private void exactUnification(ReasonerAtomicQuery parentQuery, ReasonerAtomicQuery childQuery, boolean checkInverse, boolean checkEquality){
        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();

        Unifier unifier = childAtom.getMultiUnifier(parentAtom, UnifierType.EXACT).getUnifier();

        List<Answer> childAnswers = childQuery.getQuery().execute();
        List<Answer> unifiedAnswers = childAnswers.stream()
                .map(a -> a.unify(unifier))
                .filter(a -> !a.isEmpty())
                .collect(Collectors.toList());
        List<Answer> parentAnswers = parentQuery.getQuery().execute();

        if (checkInverse) {
            Unifier unifier2 = parentAtom.getUnifier(childAtom);
            assertEquals(unifier.inverse(), unifier2);
            assertEquals(unifier, unifier2.inverse());
        }

        assertTrue(!childAnswers.isEmpty());
        assertTrue(!unifiedAnswers.isEmpty());
        assertTrue(!parentAnswers.isEmpty());

        if (!checkEquality){
            assertTrue(parentAnswers.containsAll(unifiedAnswers));
        } else {
            assertCollectionsEqual(parentAnswers, unifiedAnswers);
            Unifier inverse = unifier.inverse();
            List<Answer> parentToChild = parentAnswers.stream().map(a -> a.unify(inverse)).collect(Collectors.toList());
            assertCollectionsEqual(parentToChild, childAnswers);
        }
    }

    private void exactUnification(String parentPatternString, String childPatternString, boolean checkInverse, boolean checkEquality, EmbeddedGraknTx<?> graph){
        exactUnification(
                ReasonerQueries.atomic(conjunction(parentPatternString, graph), graph),
                ReasonerQueries.atomic(conjunction(childPatternString, graph), graph),
                checkInverse,
                checkEquality);
    }

    private Concept getConcept(EmbeddedGraknTx<?> graph, String typeName, Object val){
        return graph.graql().match(var("x").has(typeName, val).admin()).get("x")
                .stream().map(ans -> ans.get("x")).findAny().orElse(null);
    }

    private Multimap<Role, Var> roleSetMap(Multimap<Role, Var> roleVarMap) {
        Multimap<Role, Var> roleMap = HashMultimap.create();
        roleVarMap.entries().forEach(e -> roleMap.put(e.getKey(), e.getValue()));
        return roleMap;
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, EmbeddedGraknTx<?> graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}

