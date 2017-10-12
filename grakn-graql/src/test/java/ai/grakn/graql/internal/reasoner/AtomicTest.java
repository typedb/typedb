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
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtils;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.CWKB;
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
    public static final SampleKBContext unificationTestSet = SampleKBContext.preLoad("unificationTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestSetup.usingTinker());
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

    /**
     * ##################################
     *
     *       ROLE INFERENCE Tests
     *
     * ##################################
     */

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

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_NoInformationPresent(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y) isa ternary;}";
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

    @Test //should assign (role1: $x, role {role1, role2} : $y, role {role2, role3} : $z) which taking into account types is compatible with 3 ternary rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_AmbiguousRoleMapping(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa anotherTwoRoleEntity;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(3, relation.getApplicableRules().count());
    }

    @Test //should assign (role1: $x, role {role1, role2}: $y, role {role1, role2, role3}: $z) which taking into account types is compatible with 2 ternary rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_AmbiguousRoleMapping_RolePlayerTypeMismatch(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa threeRoleEntity;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().count());
    }

    @Test //threeRoleEntity subs twoRoleEntity
    public void testRuleApplicability_AmbiguousRoleMapping_TypeHierarchyEnablesExtraRule(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(3, relation.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_MissingRelationPlayers(){
        GraknTx graph = ruleApplicabilitySet.tx();

        //inferred relation (role {role1, role2} : $x, role {role2, role3} : $y)
        String relationString = "{($x, $y);$x isa twoRoleEntity; $y isa anotherTwoRoleEntity;}";

        //inferred relation: (role1: $x, role1: $y)
        String relationString2 = "{($x, $y);$x isa singleRoleEntity; $y isa singleRoleEntity;}";

        //inferred relation: (role1: $x, role {role2, role3}: $y)
        String relationString3 = "{($x, $y);$x isa singleRoleEntity; $y isa anotherTwoRoleEntity;}";

        //inferred relation: (role1: $x, role {role1, role2, role3}: $y)
        String relationString4 = "{($x, $y);$x isa singleRoleEntity; $y isa threeRoleEntity;}";

        //inferred relation: (role {role2, role3}: $x, role {role2, role3}: $y)
        String relationString5 = "{($x, $y);$x isa anotherTwoRoleEntity; $y isa anotherTwoRoleEntity;}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, graph), graph).getAtom();
        Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, graph), graph).getAtom();
        Atom relation5 = ReasonerQueries.atomic(conjunction(relationString5, graph), graph).getAtom();

        assertEquals(4, relation.getApplicableRules().count());
        assertThat(relation2.getApplicableRules().collect(toSet()), empty());
        assertEquals(4, relation3.getApplicableRules().count());
        assertEquals(4, relation4.getApplicableRules().count());
        assertThat(relation5.getApplicableRules().collect(toSet()), empty());
    }

    @Test //should assign (role1: $x, role1: $y, role: $z) which is compatible with 3 ternary rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_WithWildcard(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$y isa singleRoleEntity; $z isa twoRoleEntity;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
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
    public void testRuleApplicability_MatchAllAtom(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(
                RuleUtils.getRules(graph).count(),
                relation.getApplicableRules().count()
        );
    }

    @Test //should assign (role: $x, role1: $y, role1: $z) which is incompatible with any of the rule heads
    public void testRuleApplicability_WithWildcard_MissingMappings(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y, $z);$y isa singleRoleEntity; $z isa singleRoleEntity;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test //NB: role2 sub role1
    public void testRuleApplicability_RepeatingRoleTypes(){
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
    public void testRuleApplicability_TypePreventsFromApplyingTheRule(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{($x, $y);$x isa noRoleEntity;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
    }

    @Test
    public void testRuleApplicability_ReifiedRelationsWithType(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{(role1: $x, role2: $y) isa reifying-relation;}";
        String relationString2 = "{$x isa entity;(role1: $x, role2: $y) isa reifying-relation;}";
        String relationString3 = "{$x isa twoRoleEntity;(role1: $x, role2: $y) isa reifying-relation;}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().count());
        assertEquals(2, relation2.getApplicableRules().count());
        assertEquals(1, relation3.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_TypePlayabilityInRuleBodyNeedsChecking(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String relationString = "{$y isa anotherTwoRoleEntity;(role1:$x, role2:$y, role3: $z) isa ternary;}";
        String relationString2 = "{$y isa entity;(role1:$x, role2:$y, role3: $z) isa ternary;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, relation2.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_TypeRelation(){
        GraknTx graph = ruleApplicabilitySet.tx();
        String typeString = "{$x isa reifying-relation;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        assertEquals(2, type.getApplicableRules().count());
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
        String typeString = "{$x isa res1;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        assertEquals(1, type.getApplicableRules().count());
    }

    @Test
    public void testRuleApplicability_Resource_TypeMismatch(){
        GraknTx graph = resourceApplicabilitySet.tx();
        String resourceString = "{$x isa entity1, has res1 $r;}";
        String resourceString2 = "{$x isa entity2, has res1 $r;}";
        String resourceString3 = "{$x isa entity2, has res1 'test';}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
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

        String patternString = "{$x isa singleRoleEntity; ($x, $y);}";
        String subbedPatternString = "{$x id '" + conceptId(graph, "singleRoleEntity") + "';($x, $y);}";
        String patternString2 = "{$x isa twoRoleEntity; ($x, $y);}";
        String subbedPatternString2 = "{$x id '" + conceptId(graph, "twoRoleEntity") + "';($x, $y);}";

        RelationshipType relation1 = graph.getSchemaConcept(Label.of("relation1"));
        List<RelationshipType> possibleTypes = Collections.singletonList(relation1);

        List<RelationshipType> possibleTypes2 = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation1")),
                graph.getSchemaConcept(Label.of("relation3"))
        );

        testTypeInference(possibleTypes, patternString, subbedPatternString, graph);
        testTypeInference(possibleTypes2, patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel1} ^ {rel1, rel2} = {rel1}
        String patternString = "{$x isa singleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{($x, $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";
        //{rel1, rel3} ^ {rel1, rel2} = {rel1}
        String patternString2 = "{$x isa twoRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{($x, $y);" +
                "$x id '" + conceptId(graph, "twoRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        RelationshipType relation1 = graph.getSchemaConcept(Label.of("relation1"));
        List<RelationshipType> possibleTypes = Collections.singletonList(relation1);

        testTypeInference(possibleTypes, patternString, subbedPatternString, graph);
        testTypeInference(possibleTypes, patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_singleRole() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(role2: $x, $y);}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation1")),
                graph.getSchemaConcept(Label.of("relation2"))
        );

        testTypeInference(possibleTypes, patternString, graph);
    }

    @Test
    public void testTypeInference_singleRole_subType() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{(subRole2: $x, $y);}";
        testTypeInference(allRelations(graph), patternString, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel1, rel2} ^ {rel1}
        String patternString = "{(role2: $x, $y); $y isa singleRoleEntity;}";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$y id '" + conceptId(graph, "singleRoleEntity") + "';}";
        //{rel1, rel2} ^ {rel1, rel3}
        String patternString2 = "{(role2: $x, $y); $y isa twoRoleEntity;}";
        String subbedPatternString2 = "{(role2: $x, $y);" +
                "$y id '" + conceptId(graph, "twoRoleEntity") + "';}";
        //{rel1,} ^ {rel1, rel3}
        String patternString3 = "{(role1: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString3 = "{(role1: $x, $y);" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") + "';}";

        RelationshipType relation1 = graph.getSchemaConcept(Label.of("relation1"));
        List<RelationshipType> possibleTypes = Collections.singletonList(relation1);

        testTypeInference(possibleTypes, patternString, subbedPatternString, graph);
        testTypeInference(possibleTypes, patternString2, subbedPatternString2, graph);
        testTypeInference(possibleTypes, patternString3, subbedPatternString3, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_bothConceptsAreSubConcepts() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel1, rel2, rel3} ^ {rel1, rel3}
        String patternString = "{(subRole2: $x, $y); $y isa twoRoleEntity;}";
        String subbedPatternString = "{(subRole2: $x, $y);" +
                "$y id '" + conceptId(graph, "twoRoleEntity") + "';}";
        //{rel1, rel2, rel3} ^ {rel1, rel2}
        String patternString2 = "{(subRole2: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{(subRole2: $x, $y);" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") + "';}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation1")),
                graph.getSchemaConcept(Label.of("relation3"))
        );
        List<RelationshipType> possibleTypes2 = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation1")),
                graph.getSchemaConcept(Label.of("relation2"))
        );

        testTypeInference(possibleTypes, patternString, subbedPatternString, graph);
        testTypeInference(possibleTypes2, patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_typeContradiction() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel1} ^ {rel3}
        String patternString = "{(role1: $x, $y); $y isa anotherSingleRoleEntity;}";
        String subbedPatternString = "{(role1: $x, $y);" +
                "$y id '" + conceptId(graph, "anotherSingleRoleEntity") + "';}";
        String patternString2 = "{(role1: $x, $y); $x isa anotherSingleRoleEntity;}";
        String subbedPatternString2 = "{(role1: $x, $y);" +
                "$x id '" + conceptId(graph, "anotherSingleRoleEntity") + "';}";

        testTypeInference(Collections.emptyList(), patternString, subbedPatternString, graph);
        testTypeInference(Collections.emptyList(), patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_singleRole_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();
        //{rel1} ^ {rel1, rel2}
        String patternString = "{$x isa singleRoleEntity;(role2: $x, $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        RelationshipType relation1 = graph.getSchemaConcept(Label.of("relation1"));
        List<RelationshipType> possibleTypes = Collections.singletonList(relation1);
        testTypeInference(possibleTypes, patternString, subbedPatternString, graph);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard() {
        GraknTx graph = typeInferenceSet.tx();

        //{rel1} ^ {rel1, rel2} ^ {rel1} ^ {rel1, rel2}
        String patternString = "{$x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString = "{(role1: $x, role2: $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";
        //{rel1, rel2, rel3} ^ {rel1, rel2} ^ {rel2, rel3} ^ {rel1, rel2}
        String patternString2 = "{$x isa threeRoleEntity;(role2: $x, role3: $y); $y isa anotherTwoRoleEntity;}";
        String subbedPatternString2 = "{(role2: $x, role3: $y);" +
                "$x id '" + conceptId(graph, "threeRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherTwoRoleEntity") +"';}";

        RelationshipType relation1 = graph.getSchemaConcept(Label.of("relation1"));
        RelationshipType relation2 = graph.getSchemaConcept(Label.of("relation2"));

        testTypeInference(Collections.singletonList(relation1), patternString, subbedPatternString, graph);
        testTypeInference(Collections.singletonList(relation2), patternString2, subbedPatternString2, graph);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_multipleRelationsPossible() {
        GraknTx graph = typeInferenceSet.tx();
        //{rel1, rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{$x isa threeRoleEntity;(subRole2: $x, role3: $y); $y isa threeRoleEntity;}";
        String subbedPatternString = "{(subRole2: $x, role3: $y);" +
                "$x id '" + conceptId(graph, "threeRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "threeRoleEntity") + "';}";

        List<RelationshipType> possibleTypes = Lists.newArrayList(
                graph.getSchemaConcept(Label.of("relation3")),
                graph.getSchemaConcept(Label.of("relation2"))
        );
        testTypeInference(possibleTypes, patternString, subbedPatternString, graph);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_contradiction() {
        GraknTx graph = typeInferenceSet.tx();
        //{rel1} ^ {rel1} ^ {rel1, rel2} ^ {rel4}
        String patternString = "{$x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherSingleRoleEntity;}";
        String subbedPatternString = "{(role1: $x, role2: $y);" +
                "$x id '" + conceptId(graph, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(graph, "anotherSingleRoleEntity") +"';}";

        testTypeInference(Collections.emptyList(), patternString, subbedPatternString, graph);
    }

    @Test
    public void testTypeInference_genericRelation() {
        GraknTx graph = typeInferenceSet.tx();
        String patternString = "{($x, $y);}";
        testTypeInference(allRelations(graph), patternString, graph);
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
        String relation = "{(role1: $x, role2: $y) isa relation1;}";
        String relation2 = "{(role1: $y, role2: $x) isa relation1;}";
        testExactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRole(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{(role1: $x, role: $y) isa relation1;}";
        String relation2 = "{(role1: $y, role: $x) isa relation1;}";
        testExactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithRelationVar(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{$x (role1: $r, role2: $z) isa relation1;}";
        String relation2 = "{$r (role1: $x, role2: $y) isa relation1;}";
        testExactUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRolesAndIds(){
        GraknTx graph = unificationTestSet.tx();
        Concept instance = graph.graql().<GetQuery>parse("match $x isa entity1; get;").execute().iterator().next().get(var("x"));
        String relation = "{(role: $x, role: $y) isa relation1; $y id '" + instance.getId().getValue() + "';}";
        String relation2 = "{(role: $z, role: $v) isa relation1; $z id '" + instance.getId().getValue() + "';}";
        String relation3 = "{(role: $z, role: $v) isa relation1; $v id '" + instance.getId().getValue() + "';}";

        testExactUnification(relation, relation2, true, true, graph);
        testExactUnification(relation, relation3, true, true, graph);
        testExactUnification(relation2, relation3, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithRoleHierarchy_OneWayUnification(){
        GraknTx graph = unificationTestSet.tx();
        String relation = "{(role1: $y, role2: $x);}";
        String specialisedRelation = "{(superRole1: $p, anotherSuperRole2: $c);}";
        String specialisedRelation2 = "{(anotherSuperRole1: $x, anotherSuperRole2: $y);}";
        String specialisedRelation3 = "{(superRole1: $x, superRole2: $y);}";
        String specialisedRelation4 = "{(anotherSuperRole1: $x, superRole2: $y);}";

        testExactUnification(relation, specialisedRelation, false, false, graph);
        testExactUnification(relation, specialisedRelation2, false, false, graph);
        testExactUnification(relation, specialisedRelation3, false, false, graph);
        testExactUnification(relation, specialisedRelation4, false, false, graph);
    }

    @Test
    public void testUnification_VariousAtoms(){
        GraknTx graph = unificationTestSet.tx();
        String resource = "{$x has res1 $r;$r val 'f';}";
        String resource2 = "{$r has res1 $x;$x val 'f';}";
        String resource3 = "{$r has res1 'f';}";
        String resource4 = "{$x has res1 $y as $r;$y val 'f';}";
        String resource5 = "{$y has res1 $r as $x;$r val 'f';}";
        testExactUnification(resource, resource2, true, true, graph);
        testExactUnification(resource, resource3, true, true, graph);
        testExactUnification(resource2, resource3, true, true, graph);
        testExactUnification(resource4, resource5, true, true, graph);
    }

    @Test
    public void testUnification_VariousTypeAtoms(){
        GraknTx graph = unificationTestSet.tx();
        String type = "{$x isa entity1;}";
        String type2 = "{$y isa $x;$x label 'entity1';}";
        String type3 = "{$y isa entity1;}";
        testExactUnification(type, type2, true, true, graph);
        testExactUnification(type, type3, true, true, graph);
        testExactUnification(type2, type3, true, true, graph);
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
    public void testUnification_ResourceWithType(){
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
    public void testRewriteAndUnification(){
        GraknTx graph = unificationTestSet.tx();
        String parentString = "{$r (superRole1: $x) isa relation1;}";
        Atom parentAtom = ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();
        Var parentVarName = parentAtom.getVarName();

        String childPatternString = "(superRole1: $x, superRole2: $y) isa relation1";
        InferenceRule testRule = new InferenceRule(
                graph.putRule("Checking Rewrite & Unification",
                        graph.graql().parsePattern(childPatternString),
                        graph.graql().parsePattern(childPatternString)),
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
        Collection<Var> wifeEntry = roleMap.get(graph.getRole("superRole1"));
        assertEquals(wifeEntry.size(), 1);
        assertEquals(wifeEntry.iterator().next(), var("x"));
    }

    @Test
    public void testUnification_MatchAllParentAtom(){
        GraknTx graph = unificationTestSet.tx();
        String parentString = "{$r($a, $x);}";
        String childString = "(role1: $z, role2: $b) isa relation1";
        Atom parent = ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();
        Atom child = ReasonerQueries.atomic(conjunction(childString, graph), graph).getAtom();

        MultiUnifier multiUnifier = child.getMultiUnifier(parent, false);
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
                .rel(var("role1").label("superRole1"), var("y1"))
                .rel(var("role2").label("anotherSuperRole2"), var("y2"))
                .isa("relation1")
                .admin();

        ReasonerAtomicQuery baseQuery = ReasonerQueries.atomic(Patterns.conjunction(Sets.newHashSet(basePattern)), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries
                .atomic(conjunction(
                                "{($r1: $x1, $r2: $x2) isa relation1;" +
                                "$r1 label 'superRole1';" +
                                "$r2 label 'anotherSuperRole2';}"
                        , graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries
                .atomic(conjunction(
                                "{($R1: $x, $R2: $y) isa relation1;" +
                                "$R1 label 'superRole1';" +
                                "$R2 label 'anotherSuperRole2';}"
                        , graph), graph);
        testExactUnification(parentQuery, childQuery, true, true);
        testExactUnification(baseQuery, parentQuery, true, true);
        testExactUnification(baseQuery, childQuery, true, true);
    }

    @Test
    public void testUnification_IndirectRoles_NoRelationType(){
        GraknTx graph = unificationTestSet.tx();
        VarPatternAdmin basePattern = var()
                .rel(var("role1").label("superRole1"), var("y1"))
                .rel(var("role2").label("anotherSuperRole2"), var("y2"))
                .admin();

        ReasonerAtomicQuery baseQuery = ReasonerQueries.atomic(Patterns.conjunction(Sets.newHashSet(basePattern)), graph);
        ReasonerAtomicQuery childQuery = ReasonerQueries
                .atomic(conjunction(
                                "{($r1: $x1, $r2: $x2);" +
                                "$r1 label 'superRole1';" +
                                "$r2 label 'anotherSuperRole2';}"
                        , graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries
                .atomic(conjunction(
                                "{($R1: $x, $R2: $y);" +
                                "$R1 label 'superRole1';" +
                                "$R2 label 'anotherSuperRole2';}"
                        , graph), graph);
        testExactUnification(parentQuery, childQuery, true, true);
        testExactUnification(baseQuery, parentQuery, true, true);
        testExactUnification(baseQuery, childQuery, true, true);
    }

    private void testTypeInference(List<RelationshipType> possibleTypes, String pattern, GraknTx graph){
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

    private void testTypeInference(List<RelationshipType> possibleTypes, String pattern, String subbedPattern, GraknTx graph){
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

    private void testExactUnification(ReasonerAtomicQuery parentQuery, ReasonerAtomicQuery childQuery, boolean checkInverse, boolean checkEquality){
        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();

        Unifier unifier = childAtom.getMultiUnifier(parentAtom, true).getUnifier();

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

    private void testExactUnification(String parentPatternString, String childPatternString, boolean checkInverse, boolean checkEquality, GraknTx graph){
        testExactUnification(
                ReasonerQueries.atomic(conjunction(parentPatternString, graph), graph),
                ReasonerQueries.atomic(conjunction(childPatternString, graph), graph),
                checkInverse,
                checkEquality);
    }

    private QueryAnswers queryAnswers(GetQuery query) {
        return new QueryAnswers(query.stream().collect(toSet()));
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
        Set<VarPatternAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}

