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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtils;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
    public static final SampleKBContext typeInferenceSet = SampleKBContext.load("typeInferenceTest.gql");

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
        GraknTx graph = ruleApplicabilitySet.tx();
        String recRelString = "{($x, $y) isa binary;}";
        String nrecRelString = "{($x, $y) isa ternary;}";
        ReasonerAtomicQuery recQuery = ReasonerQueries.atomic(conjunction(recRelString, graph), graph);
        ReasonerAtomicQuery nrecQuery = ReasonerQueries.atomic(conjunction(nrecRelString, graph), graph);
        assertTrue(recQuery.getAtom().isRecursive());
        assertTrue(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testAtomFactoryProducesAtomsOfCorrectType(){
        GraknTx graph = unificationTestSet.tx();
        String atomString = "{$x isa baseRoleEntity;}";
        String relString = "{($x, $y, $z) isa binary;}";
        String resString = "{$x has res1 'value';}";

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
     *       ROLE INFERENCE Tests
     *
     * ##################################
     */

    @Test
    public void testRoleInference_TypedBinaryRelation(){
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
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
        GraknTx graph = roleInferenceSet.tx();
        String relationString = "{($x, $y);}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().getLabel())));
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_MetaRelationType(){
        GraknTx graph = roleInferenceSet.tx();
        String relationString = "{($x, $y) isa relationship;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().getLabel())));
    }

    @Test //missing role is ambiguous without cardinality constraints
    public void testRoleInference_RoleHierarchyInvolved() {
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z) isa ternary;$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().getLabel())));
    }

    @Test //relation relates a single role so instead of assigning metarole this role should be assigned
    public void testRoleInference_RelationHasVerticalRoleHierarchy(){
        GraknTx graph = ruleApplicabilitySet.tx();
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
    public void testRuleApplicability_AmbiguousRoleMapping(){
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().getLabel())));
        assertEquals(2, relation.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_MissingRelationPlayers(){
        GraknTx graph = ruleApplicabilitySet.tx();

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
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{$x isa reified-relation; $x has description $d;}";
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
        GraknTx graph = ruleApplicabilitySet.tx();
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

    @Test //should assign (role: $x, role: $y) which is compatible with 3 rules
    public void testRuleApplicability_genericRelation(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(
                RuleUtils.getRules(graph).count(),
                relation.getApplicableRules().count()
        );
    }

    @Test
    public void testRuleApplicability_WithWildcard_MissingMappings(){
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);$x isa noRoleEntity;}";
        String relationString2 = "{($x, $y);$x isa entity;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();

        assertEquals(5, relation.getApplicableRules().count());
        assertEquals(RuleUtils.getRules(graph).count(), relation2.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_ReifiedRelationsWithType(){
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
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
        GraknTx graph = ruleApplicabilitySet.tx();
        Concept concept = getConcept(graph, "name", "noRoleEntity");
        String relationString = "{" +
                "($x, $y) isa ternary;" +
                "$x id '" + concept.getId().getValue() + "';" +
                "}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_InstancesMakeRuleInapplicable_NoRoleTypes_NoRelationType(){
        GraknTx graph = ruleApplicabilitySet.tx();
        Concept concept = getConcept(graph, "name", "noRoleEntity");
        String relationString = "{" +
                "($x, $y);" +
                "$x id '" + concept.getId().getValue() + "';" +
                "}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_ResourceDouble(){
        GraknTx graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-double > 3.0;}";
        String resourceString2 = "{$x has res-double > 4.0;}";
        String resourceString3 = "{$x has res-double < 3.0;}";
        String resourceString4 = "{$x has res-double < 4.0;}";
        String resourceString5 = "{$x has res-double >= 5;}";
        String resourceString6 = "{$x has res-double <= 5;}";
        String resourceString7 = "{$x has res-double = 3.14;}";
        String resourceString8 = "{$x has res-double != 5;}";

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
        GraknTx graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-long > 100;}";
        String resourceString2 = "{$x has res-long > 150;}";
        String resourceString3 = "{$x has res-long < 100;}";
        String resourceString4 = "{$x has res-long < 200;}";
        String resourceString5 = "{$x has res-long >= 130;}";
        String resourceString6 = "{$x has res-long <= 130;}";
        String resourceString7 = "{$x has res-long = 123;}";
        String resourceString8 = "{$x has res-long != 200;}";

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
        GraknTx graph = resourceApplicabilitySet.tx();
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
        GraknTx graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-boolean 'true';}";
        String resourceString2 = "{$x has res-boolean 'false';}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_TypeResource(){
        GraknTx graph = resourceApplicabilitySet.tx();
        String typeString = "{$x isa resource;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        assertEquals(1, type.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_Resource_TypeMismatch(){
        GraknTx graph = resourceApplicabilitySet.tx();
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
        GraknTx graph = reifiedResourceApplicabilitySet.tx();
        String queryString = "{$x isa res-double val > 3.0;($x, $y);}";
        String queryString2 = "{$x isa res-double val > 4.0;($x, $y);}";
        String queryString3 = "{$x isa res-double val < 3.0;($x, $y);}";
        String queryString4 = "{$x isa res-double val < 4.0;($x, $y);}";
        String queryString5 = "{$x isa res-double val >= 5;($x, $y);}";
        String queryString6 = "{$x isa res-double val <= 5;($x, $y);}";
        String queryString7 = "{$x isa res-double val = 3.14;($x, $y);}";
        String queryString8 = "{$x isa res-double val != 5;($x, $y);}";

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
        GraknTx graph = reifiedResourceApplicabilitySet.tx();
        String queryString = "{$x isa res-long val > 100;($x, $y);}";
        String queryString2 = "{$x isa res-long val > 150;($x, $y);}";
        String queryString3 = "{$x isa res-long val < 100;($x, $y);}";
        String queryString4 = "{$x isa res-long val < 200;($x, $y);}";
        String queryString5 = "{$x isa res-long val >= 130;($x, $y);}";
        String queryString6 = "{$x isa res-long val <= 130;($x, $y);}";
        String queryString7 = "{$x isa res-long val = 123;($x, $y);}";
        String queryString8 = "{$x isa res-long val != 200;($x, $y);}";

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
        GraknTx graph = reifiedResourceApplicabilitySet.tx();
        String queryString = "{$x isa res-string val contains 'val';($x, $y);}";
        String queryString2 = "{$x isa res-string val 'test';($x, $y);}";
        String queryString3 = "{$x isa res-string val /.*(fast|value).*/;($x, $y);}";
        String queryString4 = "{$x isa res-string val /.*/;($x, $y);}";

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
        GraknTx graph = reifiedResourceApplicabilitySet.tx();
        String queryString = "{$x isa res-boolean val 'true';($x, $y);}";
        String queryString2 = "{$x isa res-boolean val 'false';($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, graph), graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, graph), graph).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
    }

    /**
     * ##################################
     *
     *      TYPE INFERENCE Tests
     *
     * ##################################
     */

    @Test
    public void testTypeInference_singleGuard() {
        GraknTx graph = typeInferenceSet.tx();

        //parent of all roles so all relations possible
        String patternString = "{$x isa noRoleEntity; ($x, $y);}";
        String subbedPatternString = "{$x id '" + conceptId(graph, "noRoleEntity") + "';($x, $y);}";

        //SRE -> rel2
        //sub(SRE)=TRE -> rel3
        String patternString2 = "{$x isa singleRoleEntity; ($x, $y);}";
        String subbedPatternString2 = "{$x id '" + conceptId(graph, "singleRoleEntity") + "';($x, $y);}";

        //TRE -> rel3
        String patternString3 = "{$x isa twoRoleEntity; ($x, $y);}";
        String subbedPatternString3 = "{$x id '" + conceptId(graph, "twoRoleEntity") + "';($x, $y);}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation2")),
                graph.getSchemaConcept(Label.of("relation3"))
        );

        typeInference(allRelations(graph), patternString, subbedPatternString, graph);
        typeInference(possibleTypes, patternString2, subbedPatternString2, graph);
        typeInference(possibleTypes, patternString3, subbedPatternString3, graph);
    }

    @Test
    public void testTypeInference_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel2, rel3} ^ {rel1, rel2, rel3} = {rel2, rel3}
        String patternString = "{$x isa singleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{($x, $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        //{rel2, rel3} ^ {rel1, rel2, rel3} = {rel2, rel3}
        String patternString2 = "{$x isa twoRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{($x, $y);" +
                "$x id '" + conceptId(graph, "twoRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        //{rel1} ^ {rel1, rel2, rel3} = {rel1}
        String patternString3 = "{$x isa yetAnotherSingleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString3 = "{($x, $y);" +
                "$x id '" + conceptId(graph, "yetAnotherSingleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation2")),
                graph.getSchemaConcept(Label.of("relation3"))
        );

        typeInference(possibleTypes, patternString, subbedPatternString, graph);
        typeInference(possibleTypes, patternString2, subbedPatternString2, graph);
        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("relation1"))), patternString3, subbedPatternString3, graph);
    }

    @Test
    public void testTypeInference_singleRole() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(role1: $x, $y);}";
        String patternString2 = "{(role2: $x, $y);}";
        String patternString3 = "{(role3: $x, $y);}";

        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("relation1"))), patternString, graph);
        typeInference(allRelations(graph), patternString2, graph);

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation2")),
                graph.getSchemaConcept(Label.of("relation3"))
        );
        typeInference(possibleTypes, patternString3, graph);
    }

    @Test
    public void testTypeInference_singleRole_subType() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(subRole2: $x, $y);}";
        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("relation3"))), patternString, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel1, rel2, rel3} ^ {rel2, rel3}
        String patternString = "{(role2: $x, $y); $y isa singleRoleEntity;}";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$y id '" + conceptId(graph, "singleRoleEntity") + "';}";
        //{rel1, rel2, rel3} ^ {rel2, rel3}
        String patternString2 = "{(role2: $x, $y); $y isa twoRoleEntity;}";
        String subbedPatternString2 = "{(role2: $x, $y);" +
                "$y id '" + conceptId(graph, "twoRoleEntity") + "';}";
        //{rel1} ^ {rel1, rel2, rel3}
        String patternString3 = "{(role1: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString3 = "{(role1: $x, $y);" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") + "';}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation2")),
                graph.getSchemaConcept(Label.of("relation3"))
        );

        typeInference(possibleTypes, patternString, subbedPatternString, graph);
        typeInference(possibleTypes, patternString2, subbedPatternString2, graph);
        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("relation1"))), patternString3, subbedPatternString3, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_bothConceptsAreSubConcepts() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel3} ^ {rel2, rel3}
        String patternString = "{(subRole2: $x, $y); $y isa twoRoleEntity;}";
        String subbedPatternString = "{(subRole2: $x, $y);" +
                "$y id '" + conceptId(graph, "twoRoleEntity") + "';}";
        //{rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{(subRole2: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{(subRole2: $x, $y);" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") + "';}";

        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("relation3"))), patternString, subbedPatternString, graph);
        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("relation3"))), patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_typeContradiction() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel1} ^ {rel2}
        String patternString = "{(role1: $x, $y); $y isa singleRoleEntity;}";
        String subbedPatternString = "{(role1: $x, $y);" +
                "$y id '" + conceptId(graph, "singleRoleEntity") + "';}";
        String patternString2 = "{(role1: $x, $y); $x isa singleRoleEntity;}";
        String subbedPatternString2 = "{(role1: $x, $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';}";

        typeInference(Collections.emptyList(), patternString, subbedPatternString, graph);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_singleRole_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();
        //{rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{$x isa singleRoleEntity;(role2: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation2")),
                graph.getSchemaConcept(Label.of("relation3"))
        );
        typeInference(possibleTypes, patternString, subbedPatternString, graph);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel1, rel2, rel3} ^ {rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{$x isa threeRoleEntity;(subRole2: $x, role3: $y); $y isa threeRoleEntity;}";
        String subbedPatternString = "{(subRole2: $x, role3: $y);" +
                "$x id '" + conceptId(graph, "threeRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "threeRoleEntity") + "';}";

        //{rel1, rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{$x isa threeRoleEntity;(role2: $x, role3: $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{(role2: $x, role3: $y);" +
                "$x id '" + conceptId(graph, "threeRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        typeInference(Collections.singletonList(graph.getSchemaConcept(Label.of("relation3"))), patternString, subbedPatternString, graph);

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation2")),
                graph.getSchemaConcept(Label.of("relation3"))
        );
        typeInference(possibleTypes, patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_contradiction() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel2, rel3} ^ {rel1} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{$x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{(role1: $x, role2: $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        //{rel2, rel3} ^ {rel1} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{$x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherSingleRoleEntity;}";
        String subbedPatternString2 = "{(role1: $x, role2: $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherSingleRoleEntity") +"';}";

        typeInference(Collections.emptyList(), patternString, subbedPatternString, graph);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_metaGuards() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{($x, $y);$x isa entity; $y isa entity;}";
        typeInference(allRelations(graph), patternString, graph);
    }

    @Test
    public void testTypeInference_genericRelation() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{($x, $y);}";
        typeInference(allRelations(graph), patternString, graph);
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
        GraknTx graph = unificationTestSet.tx();
        String relation = "{(role1: $x, role2: $y) isa binary;}";
        String relation2 = "{(role1: $y, role2: $x) isa binary;}";
        exactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRole(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{(role1: $x, role: $y) isa binary;}";
        String relation2 = "{(role1: $y, role: $x) isa binary;}";
        exactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithRelationVar(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{$x (role1: $r, role2: $z) isa binary;}";
        String relation2 = "{$r (role1: $x, role2: $y) isa binary;}";
        exactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRolesAndIds(){
        GraknTx graph = unificationTestSet.tx();
        Concept instance = graph.graql().<GetQuery>parse("match $x isa subRoleEntity; get;").execute().iterator().next().get(var("x"));
        String relation = "{(role: $x, role: $y) isa binary; $y id '" + instance.getId().getValue() + "';}";
        String relation2 = "{(role: $z, role: $v) isa binary; $z id '" + instance.getId().getValue() + "';}";
        String relation3 = "{(role: $z, role: $v) isa binary; $v id '" + instance.getId().getValue() + "';}";

        exactUnification(relation, relation2, true, true, graph);
        exactUnification(relation, relation3, true, true, graph);
        exactUnification(relation2, relation3, true, true, graph);
    }

    @Test
    public void testUnification_BinaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
        String resource = "{$x has res1 $r;$r val 'f';}";
        String resource2 = "{$r has res1 $x;$x val 'f';}";
        String resource3 = "{$r has res1 'f';}";
        String resource4 = "{$x has res1 $y via $r;$y val 'f';}";
        String resource5 = "{$y has res1 $r via $x;$r val 'f';}";
        exactUnification(resource, resource2, true, true, graph);
        exactUnification(resource, resource3, true, true, graph);
        exactUnification(resource2, resource3, true, true, graph);
        exactUnification(resource4, resource5, true, true, graph);
    }

    @Test
    public void testUnification_VariousTypeAtoms(){
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
        String resource = "{$x has res1 $r;$r val 'f';}";
        String resource2 = "{$r has res1 $x;$x val 'f';}";
        String resource3 = "{$r has res1 'f';}";

        ReasonerAtomicQuery resourceQuery = ReasonerQueries.atomic(conjunction(resource, graph), graph);
        ReasonerAtomicQuery resourceQuery2 = ReasonerQueries.atomic(conjunction(resource2, graph), graph);
        ReasonerAtomicQuery resourceQuery3 = ReasonerQueries.atomic(conjunction(resource3, graph), graph);

        String type = "{$x isa res1;$x id '" + resourceQuery.getQuery().execute().iterator().next().get("r").getId().getValue()  + "';}";
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
        GraknTx graph = unificationTestSet.tx();
        String parentString = "{$r (subRole1: $x) isa binary;}";
        Atom parentAtom = ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();
        Var parentVarName = parentAtom.getVarName();

        String childPatternString = "(subRole1: $x, subRole2: $y) isa binary";
        InferenceRule testRule = new InferenceRule(
                graph.putRule("Checking Rewrite & Unification",
                        graph.graql().parser().parsePattern(childPatternString),
                        graph.graql().parser().parsePattern(childPatternString)),
                graph)
                .rewriteToUserDefined(parentAtom);

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
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
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
        GraknTx graph = unificationTestSet.tx();
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

    private void roleInference(String patternString, ImmutableSetMultimap<Role, Var> expectedRoleMAp, GraknTx graph){
        RelationshipAtom atom = (RelationshipAtom) ReasonerQueries.atomic(conjunction(patternString, graph), graph).getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());
        assertEquals(expectedRoleMAp, roleMap);

    }
    private void typeInference(List<RelationshipType> possibleTypes, String pattern, GraknTx graph){
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(pattern, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        if (possibleTypes.size() == 1){
            assertEquals(possibleTypes, relationshipTypes);
            assertEquals(atom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
        } else {
            assertTrue(CollectionUtils.isEqualCollection(possibleTypes, relationshipTypes));
            assertEquals(atom.getSchemaConcept(), null);
        }
    }

    private void typeInference(List<RelationshipType> possibleTypes, String pattern, String subbedPattern, GraknTx graph){
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(pattern, graph), graph);
        ReasonerAtomicQuery subbedQuery = ReasonerQueries.atomic(conjunction(subbedPattern, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        RelationshipAtom subbedAtom = (RelationshipAtom) subbedQuery.getAtom();

        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        List<RelationshipType> subbedRelationshipTypes = subbedAtom.inferPossibleRelationTypes(new QueryAnswer());
        if (possibleTypes.size() == 1){
            assertEquals(possibleTypes, relationshipTypes);
            assertEquals(relationshipTypes, subbedRelationshipTypes);
            assertEquals(atom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
            assertEquals(subbedAtom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
        } else {
            assertTrue(CollectionUtils.isEqualCollection(possibleTypes, relationshipTypes));
            assertTrue(CollectionUtils.isEqualCollection(relationshipTypes, subbedRelationshipTypes));
            assertEquals(atom.getSchemaConcept(), null);
            assertEquals(subbedAtom.getSchemaConcept(), null);
        }
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

    private void nonExistentUnifier(String parentPatternString, String childPatternString, GraknTx graph){
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

    private void exactUnification(String parentPatternString, String childPatternString, boolean checkInverse, boolean checkEquality, GraknTx graph){
        exactUnification(
                ReasonerQueries.atomic(conjunction(parentPatternString, graph), graph),
                ReasonerQueries.atomic(conjunction(childPatternString, graph), graph),
                checkInverse,
                checkEquality);
    }

    private List<RelationshipType> allRelations(GraknTx tx){
        RelationshipType metaType = tx.getRelationshipType(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue());
        return metaType.subs().filter(t -> !t.equals(metaType)).collect(Collectors.toList());
    }

    private ConceptId conceptId(GraknTx graph, String type){
        return graph.getEntityType(type).instances().map(Concept::getId).findFirst().orElse(null);
    }

    private Concept getConcept(GraknTx graph, String typeName, Object val){
        return graph.graql().match(var("x").has(typeName, val).admin()).get("x").findAny().orElse(null);
    }

    private Multimap<Role, Var> roleSetMap(Multimap<Role, Var> roleVarMap) {
        Multimap<Role, Var> roleMap = HashMultimap.create();
        roleVarMap.entries().forEach(e -> roleMap.put(e.getKey(), e.getValue()));
        return roleMap;
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}

