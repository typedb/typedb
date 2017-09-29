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
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.CWKB;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicTest {

    @ClassRule
    public static final SampleKBContext cwKB = SampleKBContext.preLoad(CWKB.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext typeInferenceSet = SampleKBContext.preLoad("typeInferenceTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext ruleApplicabilitySet = SampleKBContext.preLoad("ruleApplicabilityTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext resourceApplicabilitySet = SampleKBContext.preLoad("resourceApplicabilityTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext ruleApplicabilitySetWithTypes = SampleKBContext.preLoad("ruleApplicabilityTestWithTypes.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext ruleApplicabilityInstanceTypesSet = SampleKBContext.preLoad("testSet19.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext ruleApplicabilitySingleRoleSet = SampleKBContext.preLoad("testSet22.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext unificationTestSet = SampleKBContext.preLoad("unificationTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestSetup.usingTinker());
    }

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testAtomsAreCorrectlyIdentifiedAsRecursive(){
        GraknTx graph = ruleApplicabilitySingleRoleSet.tx();
        String recRelString = "{($x, $y) isa knows-trans;}";
        String nrecRelString = "{($x, $y) isa knows;}";
        ReasonerAtomicQuery recQuery = ReasonerQueries.atomic(conjunction(recRelString, graph), graph);
        ReasonerAtomicQuery nrecQuery = ReasonerQueries.atomic(conjunction(nrecRelString, graph), graph);
        assertTrue(recQuery.getAtom().isRecursive());
        assertTrue(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testAtomFactoryProducesAtomsOfCorrectType(){
        GraknTx graph = unificationTestSet.tx();
        String atomString = "{$x isa entity1;}";
        String relString = "{($x, $y, $z) isa relation1;}";
        String resString = "{$x has res1 'value';}";

        Atom atom = ReasonerQueries.atomic(conjunction(atomString, graph), graph).getAtom();
        Atom relation = ReasonerQueries.atomic(conjunction(relString, graph), graph).getAtom();
        Atom res = ReasonerQueries.atomic(conjunction(resString, graph), graph).getAtom();

        assertTrue(atom.isType());
        assertTrue(relation.isRelation());
        assertTrue(res.isResource());
    }

    @Test //each type can only play a specific role in the relation hence mapping unambiguous
    public void testRoleInference_BasedOnPresentTypes_AllVarsHaveType(){
        GraknTx graph = cwKB.tx();
        String patternString = "{($z, $y) isa owns; $z isa country; $y isa rocket;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("item-owner"), var("z"),
                graph.getRole("owned-item"), var("y"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test //Without cardinality constraints $y variable can be mapped either to item-owner or owned-item so meta role is inserted
    public void testRoleInference_BasedOnPresentTypes_SomeVarsHaveType(){
        GraknTx graph = cwKB.tx();
        String patternString2 = "{isa owns, ($z, $y); $z isa country;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());
        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("item-owner"), var("z"),
                graph.getRole("role"), var("y"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test //each type maps to a specific role
    public void testRoleInference_WithWildcardRelationPlayer(){
        GraknTx graph = cwKB.tx();
        String patternString = "{($z, $y, seller: $x) isa transaction;$z isa country;$y isa rocket;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom2 = (RelationshipAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom2.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("seller"), var("x"),
                graph.getRole("transaction-item"), var("y"),
                graph.getRole("buyer"), var("z"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test //without cardinality constraints the $y variable can be mapped to any of the three roles hence metarole is assigned
    public void testRoleInference_WithWildcardRelationPlayer_NoExplicitRoles(){
        GraknTx graph = cwKB.tx();
        String patternString = "{($z, $y, $x) isa transaction;$z isa country;$x isa person;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("seller"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("buyer"), var("z"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test
    public void testRoleInference_RepeatingRolePlayers_NonRepeatingRoleAbsent(){
        GraknTx graph = cwKB.tx();
        String patternString = "{(buyer: $y, seller: $y, $x), isa transaction;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("x"),
                graph.getRole("seller"), var("y"),
                graph.getRole("buyer"), var("y"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test
    public void testRoleInference_RepeatingRolePlayers_RepeatingRoleAbsent(){
        GraknTx graph = cwKB.tx();
        String patternString = "{(buyer: $y, $y, transaction-item: $x), isa transaction;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("transaction-item"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("buyer"), var("y"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test //missing role is ambiguous without cardinality constraints
    public void testRoleInference_RoleHierarchyInvolved() {
        GraknTx graph = unificationTestSet.tx();
        String relationString = "{($p, superRole2: $gc) isa relation1;}";
        String relationString2 = "{(superRole1: $gp, $p) isa relation1;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        RelationshipAtom relation2 = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(relation.getRoleVarMap());
        Multimap<Role, Var> roleMap2 = roleSetMap(relation2.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("p"),
                graph.getRole("superRole2"), var("gc"));
        ImmutableSetMultimap<Role, Var> correctRoleMap2 = ImmutableSetMultimap.of(
                graph.getRole("role"), var("p"),
                graph.getRole("superRole1"), var("gp"));
        assertEquals(correctRoleMap, roleMap);
        assertEquals(correctRoleMap2, roleMap2);
    }

    @Test //entity1 plays role1 but entity2 plays roles role1, role2 hence ambiguous and metarole has to be assigned, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRoleInference_WithMetaType(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity1; $y isa entity2; $z isa entity;}";
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
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity1; $y isa entity2; $z isa entity3;}";
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
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity2; $y isa entity3; $z isa entity4;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().getLabel())));
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_NoInformationPresent(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y) isa relation1;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().getLabel())));
    }

    @Test //relation relates a single role so instead of assigning metarole this role should be assigned
    public void testRoleInference_RelationHasSingleRole(){
        GraknTx graph = ruleApplicabilitySingleRoleSet.tx();
        String relationString = "{($x, $y) isa knows;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("friend"), var("x"),
                graph.getRole("friend"), var("y"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
    }

    @Test //should assign (role1: $x, role: $y, role: $z) which is compatible with 2 rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_RoleMappingUnambiguous(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$x isa entity1; $y isa entity2; $z isa entity3;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().collect(toSet()).size());
    }

    @Test //should assign (role1: $x, role: $y, role: $z) which is compatible with 2 rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_RoleMappingUnambiguous2(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$x isa entity1; $y isa entity2; $z isa entity4;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().collect(toSet()).size());
    }

    @Test //should assign (role: $x, role: $y, role: $z) which is compatible with 2 rules
    public void testRuleApplicability_RoleMappingAmbiguous(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$x isa entity2; $y isa entity3; $z isa entity4;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().collect(toSet()).size());
    }

    @Test //should assign (role: $x, role1: $y, role: $z) which is compatible with 2 rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_WithWildcard(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$y isa entity1; $z isa entity2;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().collect(toSet()).size());
    }

    @Test //should assign (role: $x, role: $y) which is compatible with 3 rules
    public void testRuleApplicability_MatchAllAtom(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(4, relation.getApplicableRules().collect(toSet()).size());
    }

    @Test //should assign (role: $x, role1: $y, role1: $z) which is incompatible with any of the rule heads
    public void testRuleApplicability_WithWildcard_MissingMappings(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$y isa entity1; $z isa entity5;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test //should assign (role: $x, role: $y) which matches two rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_MissingRelationPlayers(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);$x isa entity2; $y isa entity4;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().collect(toSet()).size());
    }

    @Test //should assign (role1: $x, role1: $y) which is inadequate for any of the rules
    public void testRuleApplicability_MissingRelationPlayers2(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);$x isa entity1; $y isa entity5;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_RepeatingRoleTypes(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{(role1: $x1, role1: $x2, role2: $x3);}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_RepeatingRoleTypes2(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{(role1: $x1, role2: $x2, role2: $x3);}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().collect(toSet()).size());
    }

    @Test
    public void testRuleApplicability_TypePreventsFromApplyingTheRule(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);$x isa entity6;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_ReifiedRelationsWithType(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{(role1: $x, role2: $y) isa relation3;}";
        String relationString2 = "{$x isa entity2;(role1: $x, role2: $y) isa relation3;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        RelationshipAtom relation2 = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().collect(toSet()).size());
        assertEquals(1, relation2.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_TypeRelation(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String typeString = "{$x isa relation3;}";
        TypeAtom type = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        assertEquals(2, type.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_OntologicalTypes(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String typeString = "{$x sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + ";}";
        String typeString2 = "{$x relates role1;}";
        String typeString3 = "{$x plays role1;}";
        String typeString4 = "{$x has res1;}";
        TypeAtom type = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        TypeAtom type2 = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString2, graph), graph).getAtom();
        TypeAtom type3 = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString3, graph), graph).getAtom();
        TypeAtom type4 = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString4, graph), graph).getAtom();
        assertThat(type.getApplicableRules().collect(toSet()), empty());
        assertThat(type2.getApplicableRules().collect(toSet()), empty());
        assertThat(type3.getApplicableRules().collect(toSet()), empty());
        assertThat(type4.getApplicableRules().collect(toSet()), empty());
    }

    @Test //test rule applicability for atom with unspecified roles with missing relation players but with possible ambiguous role mapping
    public void testRuleApplicability_MissingRelationPlayers_TypeContradiction(){
        GraknTx graph = ruleApplicabilitySetWithTypes.tx();
        String relationString = "{($x, $y);$x isa entity2; $y isa entity4;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_AmbiguousRoleMapping_TypeContradiction(){
        GraknTx graph = ruleApplicabilitySetWithTypes.tx();
        String relationString = "{($x, $y, $z);$x isa entity2; $y isa entity3; $z isa entity4;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_InstanceSubTypeMatchesRule(){
        GraknTx graph = ruleApplicabilityInstanceTypesSet.tx();
        String relationString = "{$x isa entity1;(role1: $x, role2: $y) isa relation1;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().collect(toSet()).size());
    }

    //NB: although the rule will be triggered it will find no results
    @Ignore
    @Test
    public void testRuleApplicability_InstancesDoNotMatchRule_NoRoleTypes(){
        GraknTx graph = ruleApplicabilityInstanceTypesSet.tx();
        Concept concept = getConcept(graph, "name", "b");
        Concept concept2 = getConcept(graph, "name", "b2");
        String relationString = "{" +
                "($x, $y) isa relation1;" +
                "$x id '" + concept.getId().getValue() + "';" +
                "$y id '" + concept2.getId().getValue() + "';" +
                "}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(relationString, graph), graph);
        RelationshipAtom relation = (RelationshipAtom) query.getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    //NB: although the rule will be triggered it will find no results
    @Ignore
    @Test
    public void testRuleApplicability_InstancesDoNotMatchRule_NoRoleTypes_NoRelationType(){
        GraknTx graph = ruleApplicabilityInstanceTypesSet.tx();
        Concept concept = getConcept(graph, "name", "b");
        Concept concept2 = getConcept(graph, "name", "b2");
        String relationString = "{" +
                "($x, $y);" +
                "$x id '" + concept.getId().getValue() + "';" +
                "$y id '" + concept2.getId().getValue() + "';" +
                "}";

        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
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

        ResourceAtom resource = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        ResourceAtom resource2 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        ResourceAtom resource3 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        ResourceAtom resource4 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString4, graph), graph).getAtom();
        ResourceAtom resource5 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString5, graph), graph).getAtom();
        ResourceAtom resource6 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString6, graph), graph).getAtom();
        ResourceAtom resource7 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString7, graph), graph).getAtom();
        ResourceAtom resource8 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString8, graph), graph).getAtom();

        assertEquals(resource.getApplicableRules().count(), 1);
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        assertEquals(resource4.getApplicableRules().count(), 1);
        assertThat(resource5.getApplicableRules().collect(toSet()), empty());
        assertEquals(resource6.getApplicableRules().count(), 1);
        assertEquals(resource7.getApplicableRules().count(), 1);
        assertEquals(resource8.getApplicableRules().count(), 1);
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

        ResourceAtom resource = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        ResourceAtom resource2 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        ResourceAtom resource3 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        ResourceAtom resource4 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString4, graph), graph).getAtom();
        ResourceAtom resource5 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString5, graph), graph).getAtom();
        ResourceAtom resource6 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString6, graph), graph).getAtom();
        ResourceAtom resource7 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString7, graph), graph).getAtom();
        ResourceAtom resource8 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString8, graph), graph).getAtom();

        assertEquals(resource.getApplicableRules().count(), 1);
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        assertEquals(resource4.getApplicableRules().count(), 1);
        assertThat(resource5.getApplicableRules().collect(toSet()), empty());
        assertEquals(resource6.getApplicableRules().count(), 1);
        assertEquals(resource7.getApplicableRules().count(), 1);
        assertEquals(resource8.getApplicableRules().count(), 1);
    }

    @Test
    public void testRuleApplicability_ResourceString(){
        GraknTx graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-string contains 'ing';}";
        String resourceString2 = "{$x has res-string 'test';}";
        String resourceString3 = "{$x has res-string /.*(fast|string).*/;}";
        String resourceString4 = "{$x has res-string /.*/;}";

        ResourceAtom resource = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        ResourceAtom resource2 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        ResourceAtom resource3 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        ResourceAtom resource4 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString4, graph), graph).getAtom();

        assertEquals(resource.getApplicableRules().count(), 1);
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertEquals(resource3.getApplicableRules().count(), 1);
        assertEquals(resource4.getApplicableRules().count(), 1);
    }

    @Test
    public void testRuleApplicability_ResourceBoolean(){
        GraknTx graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x has res-boolean 'true';}";
        String resourceString2 = "{$x has res-boolean 'false';}";

        ResourceAtom resource = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        ResourceAtom resource2 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        assertEquals(resource.getApplicableRules().count(), 1);
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_TypeResource(){
        GraknTx graph = resourceApplicabilitySet.tx();
        String typeString = "{$x isa res1;}";
        TypeAtom type = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        assertEquals(1, type.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_Resource_TypeMismatch(){
        GraknTx graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x isa entity1, has res1 $r;}";
        String resourceString2 = "{$x isa entity2, has res1 $r;}";
        String resourceString3 = "{$x isa entity2, has res1 'test';}";

        ResourceAtom resource = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        ResourceAtom resource2 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        ResourceAtom resource3 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        assertEquals(resource.getApplicableRules().count(), 1);
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testTypeInference_singleGuard() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{$x isa entity1; ($x, $y);}";
        String patternString2 = "{$x isa subEntity1; ($x, $y);}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        RelationshipAtom atom2 = (RelationshipAtom) query2.getAtom();

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation1")),
                graph.getSchemaConcept(Label.of("relation3"))
        );
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        List<RelationshipType> relationshipTypes2 = atom2.inferPossibleRelationTypes(new QueryAnswer());

        assertTrue(CollectionUtils.isEqualCollection(relationshipTypes, possibleTypes));
        assertTrue(CollectionUtils.isEqualCollection(relationshipTypes2, possibleTypes));

        assertEquals(atom.getSchemaConcept(), null);
        assertEquals(atom2.getSchemaConcept(), null);
    }

    @Test
    public void testTypeInference_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{$x isa entity1; ($x, $y); $y isa entity2;}";
        String patternString2 = "{$x isa subEntity1; ($x, $y); $y isa entity2;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        RelationshipAtom atom2 = (RelationshipAtom) query2.getAtom();

        List<RelationshipType> possibleTypes = Collections.singletonList(
                graph.getSchemaConcept(Label.of("relation1"))
        );
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        List<RelationshipType> relationshipTypes2 = atom2.inferPossibleRelationTypes(new QueryAnswer());

        assertEquals(relationshipTypes, possibleTypes);
        assertEquals(relationshipTypes2, possibleTypes);
        assertEquals(atom.getSchemaConcept(), graph.getSchemaConcept(Label.of("relation1")));
        assertEquals(atom2.getSchemaConcept(), graph.getSchemaConcept(Label.of("relation1")));
    }

    @Test
    public void testTypeInference_singleRole() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(role2: $x, $y);}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation1")),
                graph.getSchemaConcept(Label.of("relation2")),
                graph.getSchemaConcept(Label.of("relation3"))
        );

        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        assertTrue(CollectionUtils.isEqualCollection(relationshipTypes, possibleTypes));
        assertEquals(atom.getSchemaConcept(), null);
    }

    @Test
    public void testTypeInference_singleRole_subType() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(subRole2: $x, $y);}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> possibleTypes = Collections.singletonList(
                graph.getSchemaConcept(Label.of("relation3"))
        );
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertEquals(relationshipTypes, possibleTypes);
        assertEquals(atom.getSchemaConcept(), graph.getSchemaConcept(Label.of("relation3")));
    }

    @Test
    public void testTypeInference_singleRole_singleGuard() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(role2: $x, $y); $y isa entity1;}";
        String patternString2 = "{(role2: $x, $y); $y isa subEntity1;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();
        RelationshipAtom atom2 = (RelationshipAtom) query2.getAtom();

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation1")),
                graph.getSchemaConcept(Label.of("relation3"))
        );
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        List<RelationshipType> relationshipTypes2 = atom2.inferPossibleRelationTypes(new QueryAnswer());

        assertTrue(CollectionUtils.isEqualCollection(relationshipTypes, possibleTypes));
        assertTrue(CollectionUtils.isEqualCollection(relationshipTypes2, possibleTypes));

        assertEquals(atom.getSchemaConcept(), null);
        assertEquals(atom2.getSchemaConcept(), null);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_bothAreSuperTypes() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(subRole2: $x, $y); $y isa subEntity1;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> possibleTypes = Collections.singletonList(
                graph.getSchemaConcept(Label.of("relation3"))
        );
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertEquals(relationshipTypes, possibleTypes);
        assertEquals(atom.getSchemaConcept(), graph.getSchemaConcept(Label.of("relation3")));
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_contradictionOnDifferentRelationPlayers() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(role1: $x, $y); $y isa entity4;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertThat(relationshipTypes, empty());
        assertEquals(atom.getSchemaConcept(), null);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_contradictionOnSameRelationPlayer() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(role1: $x, $y); $x isa entity4;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertThat(relationshipTypes, empty());
        assertEquals(atom.getSchemaConcept(), null);
    }

    @Test
    public void testTypeInference_singleRole_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{$x isa entity1;(role2: $x, $y); $y isa entity2;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> possibleTypes = Collections.singletonList(
                graph.getSchemaConcept(Label.of("relation1"))
        );
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        assertEquals(relationshipTypes, possibleTypes);
        assertEquals(atom.getSchemaConcept(), graph.getSchemaConcept(Label.of("relation1")));
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{$x isa entity1;(role1: $x, role2: $y); $y isa entity2;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> possibleTypes = Collections.singletonList(
                graph.getSchemaConcept(Label.of("relation1"))
        );
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertEquals(relationshipTypes, possibleTypes);
        assertEquals(atom.getSchemaConcept(), graph.getSchemaConcept(Label.of("relation1")));
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_multipleRelationsPossible() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{$x isa entity3;(role2: $x, role3: $y); $y isa entity3;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation3")),
                graph.getSchemaConcept(Label.of("relation2"))
        );
        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        assertEquals(relationshipTypes, possibleTypes);
        assertEquals(atom.getSchemaConcept(), null);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_contradiction() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{$x isa entity1;(role1: $x, role2: $y); $y isa entity4;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationshipAtom atom = (RelationshipAtom) query.getAtom();

        List<RelationshipType> relationshipTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        assertThat(relationshipTypes, empty());
        assertEquals(atom.getSchemaConcept(), null);
    }

    @Test
    public void testUnification_RelationWithRolesExchanged(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{(role1: $x, role2: $y) isa relation1;}";
        String relation2 = "{(role1: $y, role2: $x) isa relation1;}";
        testUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRole(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{(role1: $x, role: $y) isa relation1;}";
        String relation2 = "{(role1: $y, role: $x) isa relation1;}";
        testUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithRelationVar(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{$x (role1: $r, role2: $z) isa relation1;}";
        String relation2 = "{$r (role1: $x, role2: $y) isa relation1;}";
        testUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRolesAndIds(){
        GraknTx graph = unificationTestSet.tx();
        Concept instance = graph.graql().<GetQuery>parse("match $x isa entity1; get;").execute().iterator().next().get(var("x"));
        String relation = "{(role: $x, role: $y) isa relation1; $y id '" + instance.getId().getValue() + "';}";
        String relation2 = "{(role: $z, role: $v) isa relation1; $z id '" + instance.getId().getValue() + "';}";
        String relation3 = "{(role: $z, role: $v) isa relation1; $v id '" + instance.getId().getValue() + "';}";

        testUnification(relation, relation2, true, true, graph);
        testUnification(relation, relation3, true, true, graph);
        testUnification(relation2, relation3, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithRoleHierarchy_OneWayUnification(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{(role1: $y, role2: $x);}";
        String specialisedRelation = "{(superRole1: $p, anotherSuperRole2: $c);}";
        String specialisedRelation2 = "{(anotherSuperRole1: $x, anotherSuperRole2: $y);}";
        String specialisedRelation3 = "{(superRole1: $x, superRole2: $y);}";
        String specialisedRelation4 = "{(anotherSuperRole1: $x, superRole2: $y);}";

        testUnification(relation, specialisedRelation, false, false, graph);
        testUnification(relation, specialisedRelation2, false, false, graph);
        testUnification(relation, specialisedRelation3, false, false, graph);
        testUnification(relation, specialisedRelation4, false, false, graph);
    }

    @Test
    public void testUnification_ParentHasFewerRelationPlayers() {
        GraknTx graph = unificationTestSet.tx();
        String childString = "{(superRole1: $y, superRole2: $x) isa relation1;}";
        String parentString = "{(superRole1: $x) isa relation1;}";
        String parentString2 = "{(superRole2: $y) isa relation1;}";

        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(childString, graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction(parentString, graph), graph);
        ReasonerAtomicQuery parentQuery2 = ReasonerQueries.atomic(conjunction(parentString2, graph), graph);

        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();
        Atom parentAtom2 = parentQuery2.getAtom();

        QueryAnswers childAnswers = queryAnswers(childQuery.getQuery());
        QueryAnswers parentAnswers = queryAnswers(parentQuery.getQuery());
        QueryAnswers parentAnswers2 = queryAnswers(parentQuery2.getQuery());

        Unifier unifier = childAtom.getUnifier(parentAtom);
        Unifier unifier2 = childAtom.getUnifier(parentAtom2);

        assertEquals(parentAnswers, childAnswers.unify(unifier).filterVars(parentQuery.getVarNames()));
        assertEquals(parentAnswers2, childAnswers.unify(unifier2).filterVars(parentQuery2.getVarNames()));
    }

    @Test
    public void testUnification_VariousResourceAtoms(){
        GraknTx graph = unificationTestSet.tx();
        String resource = "{$x has res1 $r;$r val 'f';}";
        String resource2 = "{$r has res1 $x;$x val 'f';}";
        String resource3 = "{$r has res1 'f';}";
        String resource4 = "{$x has res1 $y as $r;$y val 'f';}";
        String resource5 = "{$y has res1 $r as $x;$r val 'f';}";
        testUnification(resource, resource2, true, true, graph);
        testUnification(resource, resource3, true, true, graph);
        testUnification(resource2, resource3, true, true, graph);
        testUnification(resource4, resource5, true, true, graph);
    }

    @Test
    public void testUnification_UnifyResourceWithType(){
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

        Answer typeAnswer = queryAnswers(typeQuery.getQuery()).iterator().next();
        Answer resourceAnswer = queryAnswers(resourceQuery.getQuery()).iterator().next();
        Answer resourceAnswer2 = queryAnswers(resourceQuery2.getQuery()).iterator().next();
        Answer resourceAnswer3 = queryAnswers(resourceQuery3.getQuery()).iterator().next();

        assertEquals(typeAnswer.get(var("x")), resourceAnswer.unify(unifier).get(var("x")));
        assertEquals(typeAnswer.get(var("x")), resourceAnswer2.unify(unifier2).get(var("x")));
        assertEquals(typeAnswer.get(var("x")), resourceAnswer3.unify(unifier3).get(var("x")));
    }

    @Test
    public void testUnification_VariousTypeAtoms(){
        GraknTx graph = unificationTestSet.tx();
        String type = "{$x isa entity1;}";
        String type2 = "{$y isa $x;$x label 'entity1';}";
        String type3 = "{$y isa entity1;}";
        testUnification(type, type2, true, true, graph);
        testUnification(type, type3, true, true, graph);
        testUnification(type2, type3, true, true, graph);
    }

    @Test
    public void testRewriteAndUnification(){
        GraknTx graph = unificationTestSet.tx();
        String parentString = "{$r (superRole1: $x) isa relation1;}";
        Atom parentAtom = ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();
        Var parentVarName = parentAtom.getVarName();

        String childPatternString = "(superRole1: $x, superRole2: $y) isa relation1";
        InferenceRule testRule = new InferenceRule(
                graph.putRule("Checking Rewrite & Unification",
                        graph.graql().parser().parsePattern(childPatternString),
                        graph.graql().parser().parsePattern(childPatternString)),
                graph)
                .rewriteToUserDefined(parentAtom);

        RelationshipAtom headAtom = (RelationshipAtom) testRule.getHead().getAtom();
        Var headVarName = headAtom.getVarName();

        Unifier unifier = testRule.getUnifier(parentAtom);
        Unifier correctUnifier = new UnifierImpl(
                ImmutableMap.of(
                        var("x"), var("x"),
                        headVarName, parentVarName)
        );

        assertTrue(unifier.containsAll(correctUnifier));

        Multimap<Role, Var> roleMap = roleSetMap(headAtom.getRoleVarMap());
        Collection<Var> wifeEntry = roleMap.get(graph.getRole("superRole1"));
        assertEquals(wifeEntry.size(), 1);
        assertEquals(wifeEntry.iterator().next(), var("x"));
    }

    @Test
    public void testUnification_MatchAllParentAtom(){
        GraknTx graph = unificationTestSet.tx();
        String parentString = "{$r($a, $x);}";
        RelationshipAtom parent = (RelationshipAtom) ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();

        PatternAdmin body = graph.graql().parser().parsePattern("(role1: $z, role2: $b) isa relation1").admin();
        PatternAdmin head = graph.graql().parser().parsePattern("(role1: $z, role2: $b) isa relation1").admin();
        InferenceRule rule = new InferenceRule(graph.putRule("Rule: Checking Unification", body, head), graph);

        Unifier unifier = rule.getUnifier(parent);
        Set<Var> vars = rule.getHead().getAtom().getVarNames();
        Set<Var> correctVars = Sets.newHashSet(var("r"), var("a"), var("x"));
        assertTrue(!vars.contains(var("")));
        assertTrue(
                "Variables not in subset relation:\n" + correctVars.toString() + "\n" + vars.toString(),
                unifier.values().containsAll(correctVars)
        );
    }

    @Ignore
    @Test
    public void testUnification_IndirectRoles(){
        GraknTx graph = unificationTestSet.tx();
        String childRelation = "{($r1: $x1, $r2: $x2) isa relation1;$r1 label 'superRole1';$r2 label 'anotherSuperRole2';}";
        String parentRelation = "{($R1: $x, $R2: $y) isa relation1;$R1 label 'superRole1';$R2 label 'anotherSuperRole2';}";
        testUnification(parentRelation, childRelation, true, true, graph);
    }

    @Ignore
    @Test
    public void testUnification_IndirectRoles_NoRelationType(){
        GraknTx graph = unificationTestSet.tx();
        String childRelation = "{($r1: $x1, $r2: $x2);$r1 label 'superRole1';$r2 label 'anotherSuperRole2';}";
        String parentRelation = "{($R2: $y, $R1: $x);$R1 label 'superRole1';$R2 label 'anotherSuperRole2';}";
        testUnification(parentRelation, childRelation, true, true, graph);
    }

    @Test
    public void testWhenCreatingQueryWithNonexistentType_ExceptionIsThrown(){
        GraknTx graph = unificationTestSet.tx();
        String patternString = "{$x isa someType;}";
        exception.expect(GraqlQueryException.class);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
    }

    private void testUnification(String parentPatternString, String childPatternString, boolean checkInverse, boolean checkEquality, GraknTx graph){
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(childPatternString, graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction(parentPatternString, graph), graph);

        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();

        Unifier unifier = childAtom.getUnifier(parentAtom);

        QueryAnswers childAnswers = queryAnswers(childQuery.getQuery());
        QueryAnswers parentAnswers = queryAnswers(parentQuery.getQuery());

        if (checkInverse) {
            Unifier unifier2 = parentAtom.getUnifier(childAtom);
            assertEquals(unifier.inverse(), unifier2);
            assertEquals(unifier, unifier2.inverse());
        }

        if (!checkEquality){
            assertTrue(parentAnswers.containsAll(childAnswers.unify(unifier)));
        } else {
            assertEquals(parentAnswers, childAnswers.unify(unifier));
            assertEquals(parentAnswers.unify(unifier.inverse()), childAnswers);

        }
    }

    private QueryAnswers queryAnswers(GetQuery query) {
        return new QueryAnswers(query.stream().collect(toSet()));
    }

    private Concept getConcept(GraknTx graph, String typeName, Object val){
        return graph.graql().match(var("x").has(typeName, val).admin()).get("x").findAny().get();
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

