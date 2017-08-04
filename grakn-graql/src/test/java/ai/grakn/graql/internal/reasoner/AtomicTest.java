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
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.CWGraph;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.HashSet;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicTest {

    @ClassRule
    public static final GraphContext cwGraph = GraphContext.preLoad(CWGraph.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext typeInferenceSet = GraphContext.preLoad("typeInferenceTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext ruleApplicabilitySet = GraphContext.preLoad("ruleApplicabilityTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext resourceApplicabilitySet = GraphContext.preLoad("resourceApplicabilityTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext ruleApplicabilitySetWithTypes = GraphContext.preLoad("ruleApplicabilityTestWithTypes.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext ruleApplicabilityInstanceTypesSet = GraphContext.preLoad("testSet19.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext ruleApplicabilitySingleRoleSet = GraphContext.preLoad("testSet22.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final GraphContext unificationTestSet = GraphContext.preLoad("unificationTest.gql").assumeTrue(GraknTestSetup.usingTinker());

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestSetup.usingTinker());
    }

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testAtomsAreCorrectlyIdentifiedAsRecursive(){
        GraknGraph graph = ruleApplicabilitySingleRoleSet.graph();
        String recRelString = "{($x, $y) isa knows-trans;}";
        String nrecRelString = "{($x, $y) isa knows;}";
        ReasonerAtomicQuery recQuery = ReasonerQueries.atomic(conjunction(recRelString, graph), graph);
        ReasonerAtomicQuery nrecQuery = ReasonerQueries.atomic(conjunction(nrecRelString, graph), graph);
        assertTrue(recQuery.getAtom().isRecursive());
        assertTrue(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testAtomFactoryProducesAtomsOfCorrectType(){
        GraknGraph graph = unificationTestSet.graph();
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

    @Test
    public void testAlphaEquivalence_DifferentIsaVariants(){
        testAlphaEquivalence_DifferentTypeVariants(unificationTestSet.graph(), "isa", "entity1", "superEntity1");
    }

    @Test
    public void testAlphaEquivalence_DifferentSubVariants(){
        testAlphaEquivalence_DifferentTypeVariants(unificationTestSet.graph(), "sub", "entity1", "role1");
    }

    @Test
    public void testAlphaEquivalence_DifferentPlaysVariants(){
        testAlphaEquivalence_DifferentTypeVariants(unificationTestSet.graph(), "plays", "role1", "role2");
    }

    @Test
    public void testAlphaEquivalence_DifferentRelatesVariants(){
        testAlphaEquivalence_DifferentTypeVariants(unificationTestSet.graph(), "relates", "role1", "role2");
    }

    @Test
    public void testAlphaEquivalence_DifferentHasVariants(){
        GraknGraph graph = unificationTestSet.graph();
        String patternString = "{$x has res1;}";
        String patternString2 = "{$y has res1;}";
        String patternString3 = "{$x has resource;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);

        Atom atom = ReasonerQueries.atomic(pattern, graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(pattern2, graph).getAtom();
        Atom atom3 = ReasonerQueries.atomic(pattern3, graph).getAtom();

        atomicEquivalence(atom, atom2, true);
        atomicEquivalence(atom, atom3, false);
        atomicEquivalence(atom2, atom3, false);
    }

    private void testAlphaEquivalence_DifferentTypeVariants(GraknGraph graph, String keyword, String label, String label2){
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

        Atom atom = ReasonerQueries.atomic(pattern, graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(pattern2, graph).getAtom();
        Atom atom3 = ReasonerQueries.atomic(pattern3, graph).getAtom();
        Atom atom4 = ReasonerQueries.atomic(pattern4, graph).getAtom();
        Atom atom5 = ReasonerQueries.atomic(pattern5, graph).getAtom();

        atomicEquivalence(atom, atom2, true);
        atomicEquivalence(atom, atom3, true);
        atomicEquivalence(atom, atom4, false);
        atomicEquivalence(atom, atom5, false);
        atomicEquivalence(atom2, atom3, true);
        atomicEquivalence(atom2, atom4, false);
        atomicEquivalence(atom2, atom5, false);
        atomicEquivalence(atom3, atom4, false);
        atomicEquivalence(atom3, atom5, false);
        atomicEquivalence(atom4, atom5, false);
    }

    @Test
    public void testAlphaEquivalence_TypesWithSameLabel(){
        GraknGraph graph = unificationTestSet.graph();
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

        Atom isaAtom = ReasonerQueries.atomic(isaPattern, graph).getAtom();
        Atom subAtom = ReasonerQueries.atomic(subPattern, graph).getAtom();

        Atom playsAtom = ReasonerQueries.atomic(playsPattern, graph).getAtom();
        Atom relatesAtom = ReasonerQueries.atomic(relatesPattern, graph).getAtom();
        Atom hasAtom = ReasonerQueries.atomic(hasPattern, graph).getAtom();
        Atom subAtom2 = ReasonerQueries.atomic(subPattern2, graph).getAtom();

        atomicEquivalence(isaAtom, subAtom, false);
        atomicEquivalence(playsAtom, relatesAtom, false);
        atomicEquivalence(playsAtom, hasAtom, false);
        atomicEquivalence(playsAtom, subAtom2, false);
        atomicEquivalence(relatesAtom, hasAtom, false);
        atomicEquivalence(relatesAtom, subAtom2, false);
        atomicEquivalence(hasAtom, subAtom2, false);
    }

    private void atomicEquivalence(Atomic a, Atomic b, boolean expectation){
        assertEquals(a.toString() + " =? " + b.toString(), a.isEquivalent(b), expectation);
        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), a.equivalenceHashCode() == b.equivalenceHashCode(), true);
        }
    }

    @Test
    public void testAlphaEquivalence_DifferentResourceVariants(){
        GraknGraph graph = unificationTestSet.graph();
        String patternString = "{$x has res1 'value';}";
        String patternString2 = "{$y has res1 $r;$r val 'value';}";
        String patternString3 = "{$y has res1 $r;}";
        String patternString4 = "{$y has res1 'value2';}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarPatternAdmin> pattern4 = conjunction(patternString4, graph);

        Atom atom = ReasonerQueries.atomic(pattern, graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(pattern2, graph).getAtom();
        Atom atom3 = ReasonerQueries.atomic(pattern3, graph).getAtom();
        Atom atom4 = ReasonerQueries.atomic(pattern4, graph).getAtom();

        atomicEquivalence(atom, atom2, true);
        atomicEquivalence(atom, atom3, false);
        atomicEquivalence(atom, atom4, false);
        atomicEquivalence(atom2, atom3, false);
        atomicEquivalence(atom2, atom4, false);
        atomicEquivalence(atom3, atom4, false);
    }

    @Test //tests alpha-equivalence of queries with resources with multi predicate
    public void testAlphaEquivalence_MultiPredicateResources(){
        GraknGraph graph = unificationTestSet.graph();
        String patternString = "{$x isa entity1;$x has res1 $a;$a val >23; $a val <27;}";
        String patternString2 = "{$p isa entity1;$p has res1 $a;$a val >23;}";
        String patternString3 = "{$a isa entity1;$a has res1 $p;$p val <27;$p val >23;}";
        String patternString4 = "{$x isa entity1, has res1 $a;$a val >23; $a val <27;}";
        String patternString5 = "{$x isa $type;$type label entity1;$x has res1 $a;$a val >23; $a val <27;}";

        Atom atom = ReasonerQueries.atomic(conjunction(patternString, graph), graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph).getAtom();
        Atom atom3 = ReasonerQueries.atomic(conjunction(patternString3, graph), graph).getAtom();
        Atom atom4 = ReasonerQueries.atomic(conjunction(patternString4, graph), graph).getAtom();
        Atom atom5 = ReasonerQueries.atomic(conjunction(patternString5, graph), graph).getAtom();
        atomicEquivalence(atom, atom2, false);
        atomicEquivalence(atom, atom3, true);
        atomicEquivalence(atom, atom4, true);
        atomicEquivalence(atom, atom5, true);
        atomicEquivalence(atom2, atom3, false);
        atomicEquivalence(atom2, atom4, false);
        atomicEquivalence(atom3, atom4, true);
        atomicEquivalence(atom4, atom5, true);
    }

    @Test //tests alpha-equivalence of resource atoms with different predicates
    public void testAlphaEquivalence_resourcesWithDifferentPredicates() {
        GraknGraph graph = unificationTestSet.graph();
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

        Atom atom = ReasonerQueries.atomic(pattern, graph).getAtom();
        Atom atom2 = ReasonerQueries.atomic(pattern2, graph).getAtom();
        Atom atom3 = ReasonerQueries.atomic(pattern3, graph).getAtom();
        Atom atom4 = ReasonerQueries.atomic(pattern4, graph).getAtom();
        Atom atom5 = ReasonerQueries.atomic(pattern5, graph).getAtom();

        atomicEquivalence(atom, atom2, false);
        atomicEquivalence(atom2, atom3, false);
        atomicEquivalence(atom3, atom4, true);
        atomicEquivalence(atom4, atom5, false);
    }

    @Test
    public void testAlphaEquivalence_DifferentRelationInequivalentVariants(){
        GraknGraph graph = unificationTestSet.graph();

        HashSet<String> patternStrings = Sets.newHashSet(
                "{$x isa relation1;}",
                "{($y) isa relation1;}",

                "{($x, $y);}",
                "{($x, $y) isa relation1;}",
                "{(role1: $x, role2: $y) isa relation1;}",
                "{(role: $y, role2: $z) isa relation1;}",

                "{$x ($y, $z) isa relation1;}",
                "{$x (role1: $y, role2: $z) isa relation1;}"
        );

        Set<Atom> atoms = patternStrings.stream()
                .map(s -> conjunction(s, graph))
                .map(p -> ReasonerQueries.atomic(p, graph).getAtom())
                .collect(toSet());

        atoms.forEach(at -> {
            atoms.stream()
                    .filter(a -> a != at)
                    .forEach(a -> atomicEquivalence(a, at, false));
        });
    }

    @Test
    public void testAlphaEquivalence_RelationWithRepeatingVariables(){
        GraknGraph graph = unificationTestSet.graph();
        String patternString = "{(role1: $x, role2: $y);}";
        String patternString2 = "{(role1: $x, role2: $x);}";
        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);

        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        assertNotEquals(query, query2);
    }

    @Test //each type can only play a specific role in the relation hence mapping unambiguous
    public void testRoleInference_BasedOnPresentTypes_AllVarsHaveType(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{($z, $y) isa owns; $z isa country; $y isa rocket;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("item-owner"), var("z"),
                graph.getRole("owned-item"), var("y"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test //Without cardinality constraints $y variable can be mapped either to item-owner or owned-item so meta role is inserted
    public void testRoleInference_BasedOnPresentTypes_SomeVarsHaveType(){
        GraknGraph graph = cwGraph.graph();
        String patternString2 = "{isa owns, ($z, $y); $z isa country;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());
        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("item-owner"), var("z"),
                graph.getRole("role"), var("y"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test //each type maps to a specific role
    public void testRoleInference_WithWildcardRelationPlayer(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{($z, $y, seller: $x) isa transaction;$z isa country;$y isa rocket;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom2 = (RelationAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom2.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("seller"), var("x"),
                graph.getRole("transaction-item"), var("y"),
                graph.getRole("buyer"), var("z"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test //without cardinality constraints the $y variable can be mapped to any of the three roles hence metarole is assigned
    public void testRoleInference_WithWildcardRelationPlayer_NoExplicitRoles(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{($z, $y, $x) isa transaction;$z isa country;$x isa person;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("seller"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("buyer"), var("z"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test
    public void testRoleInference_RepeatingRolePlayers_NonRepeatingRoleAbsent(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{(buyer: $y, seller: $y, $x), isa transaction;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("role"), var("x"),
                graph.getRole("seller"), var("y"),
                graph.getRole("buyer"), var("y"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test
    public void testRoleInference_RepeatingRolePlayers_RepeatingRoleAbsent(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{(buyer: $y, $y, transaction-item: $x), isa transaction;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();
        Multimap<Role, Var> roleMap = roleSetMap(atom.getRoleVarMap());

        ImmutableSetMultimap<Role, Var> correctRoleMap = ImmutableSetMultimap.of(
                graph.getRole("transaction-item"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("buyer"), var("y"));
        assertEquals(correctRoleMap, roleMap);
    }

    @Test //missing role is ambiguous without cardinality constraints
    public void testRoleInference_RoleHierarchyInvolved() {
        GraknGraph graph = unificationTestSet.graph();
        String relationString = "{($p, superRole2: $gc) isa relation1;}";
        String relationString2 = "{(superRole1: $gp, $p) isa relation1;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        RelationAtom relation2 = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
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
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity1; $y isa entity2; $z isa entity;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
    }

    @Test //entity1 plays role1, entity2 plays 2 roles, entity3 plays 3 roles hence ambiguous and metarole has to be assigned, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRoleInference_RoleMappingUnambiguous(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity1; $y isa entity2; $z isa entity3;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("role1"), var("x"),
                graph.getRole("role"), var("y"),
                graph.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_AllRolePlayersHaveAmbiguousRoles(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity2; $y isa entity3; $z isa entity4;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().getLabel())));
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_NoInformationPresent(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y) isa relation1;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().getLabel())));
    }

    @Test //relation relates a single role so instead of assigning metarole this role should be assigned
    public void testRoleInference_RelationHasSingleRole(){
        GraknGraph graph = ruleApplicabilitySingleRoleSet.graph();
        String relationString = "{($x, $y) isa knows;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                graph.getRole("friend"), var("x"),
                graph.getRole("friend"), var("y"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
    }

    @Test //should assign (role1: $x, role: $y, role: $z) which is compatible with 2 rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_RoleMappingUnambiguous(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$x isa entity1; $y isa entity2; $z isa entity3;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().size());
    }

    @Test //should assign (role1: $x, role: $y, role: $z) which is compatible with 2 rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_RoleMappingUnambiguous2(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$x isa entity1; $y isa entity2; $z isa entity4;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().size());
    }

    @Test //should assign (role: $x, role: $y, role: $z) which is compatible with 2 rules
    public void testRuleApplicability_RoleMappingAmbiguous(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$x isa entity2; $y isa entity3; $z isa entity4;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().size());
    }

    @Test //should assign (role: $x, role1: $y, role: $z) which is compatible with 2 rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_WithWildcard(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$y isa entity1; $z isa entity2;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().size());
    }

    @Test //should assign (role: $x, role: $y) which is compatible with 3 rules
    public void testRuleApplicability_MatchAllAtom(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y);}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(4, relation.getApplicableRules().size());
    }

    @Test //should assign (role: $x, role1: $y, role1: $z) which is incompatible with any of the rule heads
    public void testRuleApplicability_WithWildcard_MissingMappings(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$y isa entity1; $z isa entity5;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules(), empty());
    }

    @Test //should assign (role: $x, role: $y) which matches two rules, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRuleApplicability_MissingRelationPlayers(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y);$x isa entity2; $y isa entity4;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().size());
    }

    @Test //should assign (role1: $x, role1: $y) which is inadequate for any of the rules
    public void testRuleApplicability_MissingRelationPlayers2(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y);$x isa entity1; $y isa entity5;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules(), empty());
    }

    @Test
    public void testRuleApplicability_RepeatingRoleTypes(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{(role1: $x1, role1: $x2, role2: $x3);}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules(), empty());
    }

    @Test
    public void testRuleApplicability_RepeatingRoleTypes2(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{(role1: $x1, role2: $x2, role2: $x3);}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    @Test
    public void testRuleApplicability_TypePreventsFromApplyingTheRule(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y);$x isa entity6;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules(), empty());
    }

    @Test
    public void testRuleApplicability_ReifiedRelationsWithType(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{(role1: $x, role2: $y) isa relation3;}";
        String relationString2 = "{$x isa entity2;(role1: $x, role2: $y) isa relation3;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        RelationAtom relation2 = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString2, graph), graph).getAtom();
        assertEquals(2, relation.getApplicableRules().size());
        assertEquals(1, relation2.getApplicableRules().size());
    }

    @Test
    public void testRuleApplicability_TypeRelation(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String typeString = "{$x isa relation3;}";
        TypeAtom type = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        assertEquals(2, type.getApplicableRules().size());
    }

    @Test
    public void testRuleApplicability_OntologicalTypes(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String typeString = "{$x sub relation;}";
        String typeString2 = "{$x relates role1;}";
        String typeString3 = "{$x plays role1;}";
        String typeString4 = "{$x has res1;}";
        TypeAtom type = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        TypeAtom type2 = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString2, graph), graph).getAtom();
        TypeAtom type3 = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString3, graph), graph).getAtom();
        TypeAtom type4 = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString4, graph), graph).getAtom();
        assertThat(type.getApplicableRules(), empty());
        assertThat(type2.getApplicableRules(), empty());
        assertThat(type3.getApplicableRules(), empty());
        assertThat(type4.getApplicableRules(), empty());
    }

    @Test //test rule applicability for atom with unspecified roles with missing relation players but with possible ambiguous role mapping
    public void testRuleApplicability_MissingRelationPlayers_TypeContradiction(){
        GraknGraph graph = ruleApplicabilitySetWithTypes.graph();
        String relationString = "{($x, $y);$x isa entity2; $y isa entity4;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules(), empty());
    }

    @Test
    public void testRuleApplicability_AmbiguousRoleMapping_TypeContradiction(){
        GraknGraph graph = ruleApplicabilitySetWithTypes.graph();
        String relationString = "{($x, $y, $z);$x isa entity2; $y isa entity3; $z isa entity4;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules(), empty());
    }

    @Test
    public void testRuleApplicability_InstanceSubTypeMatchesRule(){
        GraknGraph graph = ruleApplicabilityInstanceTypesSet.graph();
        String relationString = "{$x isa entity1;(role1: $x, role2: $y) isa relation1;}";
        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    //NB: although the rule will be triggered it will find no results
    @Ignore
    @Test
    public void testRuleApplicability_InstancesDoNotMatchRule_NoRoleTypes(){
        GraknGraph graph = ruleApplicabilityInstanceTypesSet.graph();
        Concept concept = getConcept(graph, "name", "b");
        Concept concept2 = getConcept(graph, "name", "b2");
        String relationString = "{" +
                "($x, $y) isa relation1;" +
                "$x id '" + concept.getId().getValue() + "';" +
                "$y id '" + concept2.getId().getValue() + "';" +
                "}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(relationString, graph), graph);
        RelationAtom relation = (RelationAtom) query.getAtom();
        assertThat(relation.getApplicableRules(), empty());
    }

    //NB: although the rule will be triggered it will find no results
    @Ignore
    @Test
    public void testRuleApplicability_InstancesDoNotMatchRule_NoRoleTypes_NoRelationType(){
        GraknGraph graph = ruleApplicabilityInstanceTypesSet.graph();
        Concept concept = getConcept(graph, "name", "b");
        Concept concept2 = getConcept(graph, "name", "b2");
        String relationString = "{" +
                "($x, $y);" +
                "$x id '" + concept.getId().getValue() + "';" +
                "$y id '" + concept2.getId().getValue() + "';" +
                "}";

        RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, graph), graph).getAtom();
        assertThat(relation.getApplicableRules(), empty());
    }

    @Test
    public void testRuleApplicability_ResourceDouble(){
        GraknGraph graph = resourceApplicabilitySet.graph();
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

        assertEquals(resource.getApplicableRules().size(), 1);
        assertThat(resource2.getApplicableRules(), empty());
        assertThat(resource3.getApplicableRules(), empty());
        assertEquals(resource4.getApplicableRules().size(), 1);
        assertThat(resource5.getApplicableRules(), empty());
        assertEquals(resource6.getApplicableRules().size(), 1);
        assertEquals(resource7.getApplicableRules().size(), 1);
        assertEquals(resource8.getApplicableRules().size(), 1);
    }

    @Test
    public void testRuleApplicability_ResourceLong(){
        GraknGraph graph = resourceApplicabilitySet.graph();
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

        assertEquals(resource.getApplicableRules().size(), 1);
        assertThat(resource2.getApplicableRules(), empty());
        assertThat(resource3.getApplicableRules(), empty());
        assertEquals(resource4.getApplicableRules().size(), 1);
        assertThat(resource5.getApplicableRules(), empty());
        assertEquals(resource6.getApplicableRules().size(), 1);
        assertEquals(resource7.getApplicableRules().size(), 1);
        assertEquals(resource8.getApplicableRules().size(), 1);
    }

    @Test
    public void testRuleApplicability_ResourceString(){
        GraknGraph graph = resourceApplicabilitySet.graph();
        String resourceString = "{$x has res-string contains 'ing';}";
        String resourceString2 = "{$x has res-string 'test';}";
        String resourceString3 = "{$x has res-string /.*(fast|string).*/;}";
        String resourceString4 = "{$x has res-string /.*/;}";

        ResourceAtom resource = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        ResourceAtom resource2 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        ResourceAtom resource3 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        ResourceAtom resource4 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString4, graph), graph).getAtom();

        assertEquals(resource.getApplicableRules().size(), 1);
        assertThat(resource2.getApplicableRules(), empty());
        assertEquals(resource3.getApplicableRules().size(), 1);
        assertEquals(resource4.getApplicableRules().size(), 1);
    }

    @Test
    public void testRuleApplicability_ResourceBoolean(){
        GraknGraph graph = resourceApplicabilitySet.graph();
        String resourceString = "{$x has res-boolean 'true';}";
        String resourceString2 = "{$x has res-boolean 'false';}";

        ResourceAtom resource = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        ResourceAtom resource2 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        assertEquals(resource.getApplicableRules().size(), 1);
        assertThat(resource2.getApplicableRules(), empty());
    }

    @Test
    public void testRuleApplicability_TypeResource(){
        GraknGraph graph = resourceApplicabilitySet.graph();
        String typeString = "{$x isa res1;}";
        TypeAtom type = (TypeAtom) ReasonerQueries.atomic(conjunction(typeString, graph), graph).getAtom();
        assertEquals(1, type.getApplicableRules().size());
    }

    @Test
    public void testRuleApplicability_Resource_TypeMismatch(){
        GraknGraph graph = resourceApplicabilitySet.graph();
        String resourceString = "{$x isa entity1, has res1 $r;}";
        String resourceString2 = "{$x isa entity2, has res1 $r;}";
        String resourceString3 = "{$x isa entity2, has res1 'test';}";

        ResourceAtom resource = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString, graph), graph).getAtom();
        ResourceAtom resource2 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString2, graph), graph).getAtom();
        ResourceAtom resource3 = (ResourceAtom) ReasonerQueries.atomic(conjunction(resourceString3, graph), graph).getAtom();
        assertEquals(resource.getApplicableRules().size(), 1);
        assertThat(resource2.getApplicableRules(), empty());
        assertThat(resource3.getApplicableRules(), empty());
    }

    @Test
    public void testTypeInference_singleGuard() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{$x isa entity1; ($x, $y);}";
        String patternString2 = "{$x isa subEntity1; ($x, $y);}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();
        RelationAtom atom2 = (RelationAtom) query2.getAtom();

        List<RelationType> possibleTypes = Lists.newArrayList(
                graph.getOntologyConcept(Label.of("relation1")),
                graph.getOntologyConcept(Label.of("relation3"))
        );
        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        List<RelationType> relationTypes2 = atom2.inferPossibleRelationTypes(new QueryAnswer());

        assertTrue(CollectionUtils.isEqualCollection(relationTypes, possibleTypes));
        assertTrue(CollectionUtils.isEqualCollection(relationTypes2, possibleTypes));

        assertEquals(atom.getOntologyConcept(), null);
        assertEquals(atom2.getOntologyConcept(), null);
    }

    @Test
    public void testTypeInference_doubleGuard() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{$x isa entity1; ($x, $y); $y isa entity2;}";
        String patternString2 = "{$x isa subEntity1; ($x, $y); $y isa entity2;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();
        RelationAtom atom2 = (RelationAtom) query2.getAtom();

        List<RelationType> possibleTypes = Collections.singletonList(
                graph.getOntologyConcept(Label.of("relation1"))
        );
        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        List<RelationType> relationTypes2 = atom2.inferPossibleRelationTypes(new QueryAnswer());

        assertEquals(relationTypes, possibleTypes);
        assertEquals(relationTypes2, possibleTypes);
        assertEquals(atom.getOntologyConcept(), graph.getOntologyConcept(Label.of("relation1")));
        assertEquals(atom2.getOntologyConcept(), graph.getOntologyConcept(Label.of("relation1")));
    }

    @Test
    public void testTypeInference_singleRole() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{(role2: $x, $y);}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> possibleTypes = Lists.newArrayList(
                graph.getOntologyConcept(Label.of("relation1")),
                graph.getOntologyConcept(Label.of("relation2")),
                graph.getOntologyConcept(Label.of("relation3"))
        );

        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        assertTrue(CollectionUtils.isEqualCollection(relationTypes, possibleTypes));
        assertEquals(atom.getOntologyConcept(), null);
    }

    @Test
    public void testTypeInference_singleRole_subType() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{(subRole2: $x, $y);}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> possibleTypes = Collections.singletonList(
                graph.getOntologyConcept(Label.of("relation3"))
        );
        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertEquals(relationTypes, possibleTypes);
        assertEquals(atom.getOntologyConcept(), graph.getOntologyConcept(Label.of("relation3")));
    }

    @Test
    public void testTypeInference_singleRole_singleGuard() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{(role2: $x, $y); $y isa entity1;}";
        String patternString2 = "{(role2: $x, $y); $y isa subEntity1;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(patternString2, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();
        RelationAtom atom2 = (RelationAtom) query2.getAtom();

        List<RelationType> possibleTypes = Lists.newArrayList(
                graph.getOntologyConcept(Label.of("relation1")),
                graph.getOntologyConcept(Label.of("relation3"))
        );
        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        List<RelationType> relationTypes2 = atom2.inferPossibleRelationTypes(new QueryAnswer());

        assertTrue(CollectionUtils.isEqualCollection(relationTypes, possibleTypes));
        assertTrue(CollectionUtils.isEqualCollection(relationTypes2, possibleTypes));

        assertEquals(atom.getOntologyConcept(), null);
        assertEquals(atom2.getOntologyConcept(), null);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_bothAreSuperTypes() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{(subRole2: $x, $y); $y isa subEntity1;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> possibleTypes = Collections.singletonList(
                graph.getOntologyConcept(Label.of("relation3"))
        );
        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertEquals(relationTypes, possibleTypes);
        assertEquals(atom.getOntologyConcept(), graph.getOntologyConcept(Label.of("relation3")));
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_contradictionOnDifferentRelationPlayers() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{(role1: $x, $y); $y isa entity4;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertThat(relationTypes, empty());
        assertEquals(atom.getOntologyConcept(), null);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_contradictionOnSameRelationPlayer() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{(role1: $x, $y); $x isa entity4;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertThat(relationTypes, empty());
        assertEquals(atom.getOntologyConcept(), null);
    }

    @Test
    public void testTypeInference_singleRole_doubleGuard() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{$x isa entity1;(role2: $x, $y); $y isa entity2;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> possibleTypes = Collections.singletonList(
                graph.getOntologyConcept(Label.of("relation1"))
        );
        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        assertEquals(relationTypes, possibleTypes);
        assertEquals(atom.getOntologyConcept(), graph.getOntologyConcept(Label.of("relation1")));
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{$x isa entity1;(role1: $x, role2: $y); $y isa entity2;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> possibleTypes = Collections.singletonList(
                graph.getOntologyConcept(Label.of("relation1"))
        );
        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());

        assertEquals(relationTypes, possibleTypes);
        assertEquals(atom.getOntologyConcept(), graph.getOntologyConcept(Label.of("relation1")));
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_multipleRelationsPossible() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{$x isa entity3;(role2: $x, role3: $y); $y isa entity3;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> possibleTypes = Lists.newArrayList(
                graph.getOntologyConcept(Label.of("relation3")),
                graph.getOntologyConcept(Label.of("relation2"))
        );
        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        assertEquals(relationTypes, possibleTypes);
        assertEquals(atom.getOntologyConcept(), null);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_contradiction() {
        GraknGraph graph = typeInferenceSet.graph();
        String patternString = "{$x isa entity1;(role1: $x, role2: $y); $y isa entity4;}";
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
        RelationAtom atom = (RelationAtom) query.getAtom();

        List<RelationType> relationTypes = atom.inferPossibleRelationTypes(new QueryAnswer());
        assertThat(relationTypes, empty());
        assertEquals(atom.getOntologyConcept(), null);
    }

    @Test
    public void testUnification_RelationWithRolesExchanged(){
        GraknGraph graph = unificationTestSet.graph();
        String relation = "{(role1: $x, role2: $y) isa relation1;}";
        String relation2 = "{(role1: $y, role2: $x) isa relation1;}";
        testUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRole(){
        GraknGraph graph = unificationTestSet.graph();
        String relation = "{(role1: $x, role: $y) isa relation1;}";
        String relation2 = "{(role1: $y, role: $x) isa relation1;}";
        testUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithRelationVar(){
        GraknGraph graph = unificationTestSet.graph();
        String relation = "{$x (role1: $r, role2: $z) isa relation1;}";
        String relation2 = "{$r (role1: $x, role2: $y) isa relation1;}";
        testUnification(relation, relation2, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithMetaRolesAndIds(){
        GraknGraph graph = unificationTestSet.graph();
        Concept instance = graph.graql().<MatchQuery>parse("match $x isa entity1;").execute().iterator().next().get(var("x"));
        String relation = "{(role: $x, role: $y) isa relation1; $y id '" + instance.getId().getValue() + "';}";
        String relation2 = "{(role: $z, role: $v) isa relation1; $z id '" + instance.getId().getValue() + "';}";
        String relation3 = "{(role: $z, role: $v) isa relation1; $v id '" + instance.getId().getValue() + "';}";

        testUnification(relation, relation2, true, true, graph);
        testUnification(relation, relation3, true, true, graph);
        testUnification(relation2, relation3, true, true, graph);
    }

    @Test
    public void testUnification_RelationWithRoleHierarchy_OneWayUnification(){
        GraknGraph graph = unificationTestSet.graph();
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
        GraknGraph graph = unificationTestSet.graph();
        String childString = "{(superRole1: $y, superRole2: $x) isa relation1;}";
        String parentString = "{(superRole1: $x) isa relation1;}";
        String parentString2 = "{(superRole2: $y) isa relation1;}";

        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(childString, graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction(parentString, graph), graph);
        ReasonerAtomicQuery parentQuery2 = ReasonerQueries.atomic(conjunction(parentString2, graph), graph);

        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();
        Atom parentAtom2 = parentQuery2.getAtom();

        QueryAnswers childAnswers = queryAnswers(childQuery.getMatchQuery());
        QueryAnswers parentAnswers = queryAnswers(parentQuery.getMatchQuery());
        QueryAnswers parentAnswers2 = queryAnswers(parentQuery2.getMatchQuery());

        Unifier unifier = childAtom.getUnifier(parentAtom);
        Unifier unifier2 = childAtom.getUnifier(parentAtom2);

        assertEquals(parentAnswers, childAnswers.unify(unifier).filterVars(parentQuery.getVarNames()));
        assertEquals(parentAnswers2, childAnswers.unify(unifier2).filterVars(parentQuery2.getVarNames()));
    }

    @Test
    public void testUnification_VariousResourceAtoms(){
        GraknGraph graph = unificationTestSet.graph();
        String resource = "{$x has res1 $r;$r val 'f';}";
        String resource2 = "{$r has res1 $x;$x val 'f';}";
        String resource3 = "{$r has res1 'f';}";
        testUnification(resource, resource2, true, true, graph);
        testUnification(resource, resource3, true, true, graph);
        testUnification(resource2, resource3, true, true, graph);
    }

    @Test
    public void testUnification_UnifyResourceWithType(){
        GraknGraph graph = unificationTestSet.graph();
        String resource = "{$x has res1 $r;$r val 'f';}";
        String resource2 = "{$r has res1 $x;$x val 'f';}";
        String resource3 = "{$r has res1 'f';}";

        ReasonerAtomicQuery resourceQuery = ReasonerQueries.atomic(conjunction(resource, graph), graph);
        ReasonerAtomicQuery resourceQuery2 = ReasonerQueries.atomic(conjunction(resource2, graph), graph);
        ReasonerAtomicQuery resourceQuery3 = ReasonerQueries.atomic(conjunction(resource3, graph), graph);

        String type = "{$x isa res1;$x id '" + resourceQuery.getMatchQuery().execute().iterator().next().get("r").getId().getValue()  + "';}";
        ReasonerAtomicQuery typeQuery = ReasonerQueries.atomic(conjunction(type, graph), graph);
        Atom typeAtom = typeQuery.getAtom();

        Atom resourceAtom = resourceQuery.getAtom();
        Atom resourceAtom2 = resourceQuery2.getAtom();
        Atom resourceAtom3 = resourceQuery3.getAtom();

        Unifier unifier = resourceAtom.getUnifier(typeAtom);
        Unifier unifier2 = resourceAtom2.getUnifier(typeAtom);
        Unifier unifier3 = resourceAtom3.getUnifier(typeAtom);

        Answer typeAnswer = queryAnswers(typeQuery.getMatchQuery()).iterator().next();
        Answer resourceAnswer = queryAnswers(resourceQuery.getMatchQuery()).iterator().next();
        Answer resourceAnswer2 = queryAnswers(resourceQuery2.getMatchQuery()).iterator().next();
        Answer resourceAnswer3 = queryAnswers(resourceQuery3.getMatchQuery()).iterator().next();

        assertEquals(typeAnswer.get(var("x")), resourceAnswer.unify(unifier).get(var("x")));
        assertEquals(typeAnswer.get(var("x")), resourceAnswer2.unify(unifier2).get(var("x")));
        assertEquals(typeAnswer.get(var("x")), resourceAnswer3.unify(unifier3).get(var("x")));
    }

    @Test
    public void testUnification_VariousTypeAtoms(){
        GraknGraph graph = unificationTestSet.graph();
        String type = "{$x isa entity1;}";
        String type2 = "{$y isa $x;$x label 'entity1';}";
        String type3 = "{$y isa entity1;}";
        testUnification(type, type2, true, true, graph);
        testUnification(type, type3, true, true, graph);
        testUnification(type2, type3, true, true, graph);
    }

    @Test
    public void testRewriteAndUnification(){
        GraknGraph graph = unificationTestSet.graph();
        String parentString = "{$r (superRole1: $x) isa relation1;}";
        Atom parentAtom = ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();
        Var parentVarName = parentAtom.getVarName();

        String childPatternString = "(superRole1: $x, superRole2: $y) isa relation1";
        InferenceRule testRule = new InferenceRule(
                graph.admin().getMetaRuleInference().putRule(
                        graph.graql().parsePattern(childPatternString),
                        graph.graql().parsePattern(childPatternString)),
                graph)
                .rewriteToUserDefined(parentAtom);

        RelationAtom headAtom = (RelationAtom) testRule.getHead().getAtom();
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
        GraknGraph graph = unificationTestSet.graph();
        String parentString = "{$r($a, $x);}";
        RelationAtom parent = (RelationAtom) ReasonerQueries.atomic(conjunction(parentString, graph), graph).getAtom();

        PatternAdmin body = graph.graql().parsePattern("(role1: $z, role2: $b) isa relation1").admin();
        PatternAdmin head = graph.graql().parsePattern("(role1: $z, role2: $b) isa relation1").admin();
        InferenceRule rule = new InferenceRule(graph.admin().getMetaRuleInference().putRule(body, head), graph);

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
        GraknGraph graph = unificationTestSet.graph();
        String childRelation = "{($r1: $x1, $r2: $x2) isa relation1;$r1 label 'superRole1';$r2 label 'anotherSuperRole2';}";
        String parentRelation = "{($R1: $x, $R2: $y) isa relation1;$R1 label 'superRole1';$R2 label 'anotherSuperRole2';}";
        testUnification(parentRelation, childRelation, true, true, graph);
    }

    @Ignore
    @Test
    public void testUnification_IndirectRoles_NoRelationType(){
        GraknGraph graph = unificationTestSet.graph();
        String childRelation = "{($r1: $x1, $r2: $x2);$r1 label 'superRole1';$r2 label 'anotherSuperRole2';}";
        String parentRelation = "{($R2: $y, $R1: $x);$R1 label 'superRole1';$R2 label 'anotherSuperRole2';}";
        testUnification(parentRelation, childRelation, true, true, graph);
    }

    @Test
    public void testWhenCreatingQueryWithNonexistentType_ExceptionIsThrown(){
        GraknGraph graph = unificationTestSet.graph();
        String patternString = "{$x isa someType;}";
        exception.expect(GraqlQueryException.class);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString, graph), graph);
    }

    private void testUnification(String parentPatternString, String childPatternString, boolean checkInverse, boolean checkEquality, GraknGraph graph){
        ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(childPatternString, graph), graph);
        ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction(parentPatternString, graph), graph);

        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();

        Unifier unifier = childAtom.getUnifier(parentAtom);

        QueryAnswers childAnswers = queryAnswers(childQuery.getMatchQuery());
        QueryAnswers parentAnswers = queryAnswers(parentQuery.getMatchQuery());

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

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().stream().collect(toSet()));
    }

    private Concept getConcept(GraknGraph graph, String typeName, Object val){
        return graph.graql().match(var("x").has(typeName, val).admin()).execute().iterator().next().get("x");
    }

    private Multimap<Role, Var> roleSetMap(Multimap<Role, Var> roleVarMap) {
        Multimap<Role, Var> roleMap = HashMultimap.create();
        roleVarMap.entries().forEach(e -> roleMap.put(e.getKey(), e.getValue()));
        return roleMap;
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarPatternAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}

