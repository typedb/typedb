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
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graphs.CWGraph;
import ai.grakn.graphs.SNBGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.test.GraphContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicTest {

    @ClassRule
    public static final GraphContext snbGraph = GraphContext.preLoad(SNBGraph.get());

    @ClassRule
    public static final GraphContext cwGraph = GraphContext.preLoad(CWGraph.get());

    @ClassRule
    public static final GraphContext genealogyOntology = GraphContext.preLoad("genealogy/ontology.gql");

    @ClassRule
    public static final GraphContext ruleApplicabilitySet = GraphContext.preLoad("ruleApplicabilityTest.gql");

    @ClassRule
    public static final GraphContext ruleApplicabilitySetWithTypes = GraphContext.preLoad("ruleApplicabilityTestWithTypes.gql");

    @ClassRule
    public static final GraphContext ruleApplicabilityInstanceTypesSet = GraphContext.preLoad("testSet18.gql");

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testAtomRecursive(){
        GraknGraph graph = snbGraph.graph();
        String recRelString = "{($x, $y) isa resides;}";
        String nrecRelString = "{($x, $y) isa recommendation;}";
        ReasonerAtomicQuery recQuery = new ReasonerAtomicQuery(conjunction(recRelString, graph), graph);
        ReasonerAtomicQuery nrecQuery = new ReasonerAtomicQuery(conjunction(nrecRelString, graph), graph);
        assertTrue(recQuery.getAtom().isRecursive());
        assertTrue(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testAtomFactory(){
        GraknGraph graph = snbGraph.graph();
        String atomString = "{$x isa person;}";
        String relString = "{($x, $y) isa recommendation;}";
        String resString = "{$x has gender 'male';}";

        Atom atom = new ReasonerAtomicQuery(conjunction(atomString, graph), graph).getAtom();
        Atom relation = new ReasonerAtomicQuery(conjunction(relString, graph), graph).getAtom();
        Atom res = new ReasonerAtomicQuery(conjunction(resString, graph), graph).getAtom();

        assertTrue(atom.isType());
        assertTrue(relation.isRelation());
        assertTrue(res.isResource());
    }

    @Test
    public void testRoleInference(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{($z, $y) isa owns; $z isa country; $y isa rocket;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
        Atom atom = query.getAtom();
        Map<RoleType, VarName> roleMap = roleMap(atom.getRoleVarTypeMap());

        String patternString2 = "{isa owns, ($z, $y); $z isa country;}";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(conjunction(patternString2, graph), graph);
        Atom atom2 = query2.getAtom();

        Map<RoleType, VarName> roleMap2 = roleMap(atom2.getRoleVarTypeMap());
        ImmutableMap<RoleType, VarName> correctRoleMap = ImmutableMap.of(
                graph.getRoleType("item-owner"), VarName.of("z"),
                graph.getRoleType("owned-item"), VarName.of("y"));
        assertEquals(correctRoleMap, roleMap);
        assertEquals(correctRoleMap, roleMap2);
    }

    @Test
    public void testRoleInference_WithWildcard(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{($z, $y, $x), isa transaction;$z isa country;$x isa person;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
        Atom atom = query.getAtom();
        Map<RoleType, VarName> roleMap = roleMap(atom.getRoleVarTypeMap());

        String patternString2 = "{($z, $y, seller: $x), isa transaction;$z isa country;$y isa rocket;}";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(conjunction(patternString2, graph), graph);
        Atom atom2 = query2.getAtom();
        Map<RoleType, VarName> roleMap2 = roleMap(atom2.getRoleVarTypeMap());

        ImmutableMap<RoleType, VarName> correctRoleMap = ImmutableMap.of(
                graph.getRoleType("seller"), VarName.of("x"),
                graph.getRoleType("transaction-item"), VarName.of("y"),
                graph.getRoleType("buyer"), VarName.of("z"));
        assertEquals(correctRoleMap, roleMap);
        assertEquals(correctRoleMap, roleMap2);
    }

    @Test
    public void testRoleInference_SingleRoleAbsent(){
        GraknGraph graph = cwGraph.graph();
        String patternString = "{(buyer: $y, seller: $y, transaction-item: $x), isa transaction;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
        Map<RoleType, VarName> roleMap = roleMap(query.getAtom().getRoleVarTypeMap());

        String patternString2 = "{(buyer: $y, seller: $y, $x), isa transaction;}";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(conjunction(patternString2, graph), graph);
        Map<RoleType, VarName> roleMap2 = roleMap(query2.getAtom().getRoleVarTypeMap());

        String patternString3 = "{(buyer: $y, $y, transaction-item: $x), isa transaction;}";
        ReasonerAtomicQuery query3 = new ReasonerAtomicQuery(conjunction(patternString3, graph), graph);
        Map<RoleType, VarName> roleMap3 = roleMap(query3.getAtom().getRoleVarTypeMap());

        ImmutableMap<RoleType, VarName> correctRoleMap = ImmutableMap.of(
                graph.getRoleType("transaction-item"), VarName.of("x"),
                graph.getRoleType("seller"), VarName.of("y"),
                graph.getRoleType("buyer"), VarName.of("y"));
        assertEquals(correctRoleMap, roleMap);
        assertEquals(correctRoleMap, roleMap2);
        assertEquals(correctRoleMap, roleMap3);
    }

    @Test
    public void testRoleInference_RoleHierarchy(){
        GraknGraph graph = genealogyOntology.graph();
        String relationString = "{($p, son: $gc) isa parentship;}";
        String fullRelationString = "{(parent: $p, son: $gc) isa parentship;}";
        String relationString2 = "{(father: $gp, $p) isa parentship;}";
        String fullRelationString2 = "{(father: $gp, child: $p) isa parentship;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        Relation correctFullRelation = (Relation) new ReasonerAtomicQuery(conjunction(fullRelationString, graph), graph).getAtom();
        Relation relation2 = (Relation) new ReasonerAtomicQuery(conjunction(relationString2, graph), graph).getAtom();
        Relation correctFullRelation2 = (Relation) new ReasonerAtomicQuery(conjunction(fullRelationString2, graph), graph).getAtom();

        assertTrue(relation.getRoleVarTypeMap().equals(correctFullRelation.getRoleVarTypeMap()));
        assertTrue(relation2.getRoleVarTypeMap().equals(correctFullRelation2.getRoleVarTypeMap()));
    }

    //tests unambiguous role mapping
    @Test
    public void testRoleInference_UnambiguousRoleMapping(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity1; $y isa entity2; $z isa entity3;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        ImmutableMap<RoleType, VarName> roleMap = ImmutableMap.of(
                graph.getRoleType("role1"), VarName.of("x"),
                graph.getRoleType("role2"), VarName.of("y"),
                graph.getRoleType("role3"), VarName.of("z"));
        assertEquals(roleMap, roleMap(relation.getRoleVarTypeMap()));
    }

    @Test
    public void testRoleInference_WithMetaType(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity1; $y isa entity2; $z isa entity;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        ImmutableMap<RoleType, VarName> roleMap = ImmutableMap.of(
                graph.getRoleType("role1"), VarName.of("x"),
                graph.getRoleType("role2"), VarName.of("y"),
                graph.getRoleType("role3"), VarName.of("z"));
        assertEquals(roleMap, roleMap(relation.getRoleVarTypeMap()));
    }
    
    //test ambiguous role mapping
    @Test
    public void testRoleInference_AmbiguousRoleMapping(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z) isa relation1;$x isa entity2; $y isa entity3; $z isa entity4;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        Map<RoleType, Pair<VarName, Type>> roleVarTypeMap = relation.getRoleVarTypeMap();
        assertTrue(roleVarTypeMap.toString(), relation.getRoleVarTypeMap().isEmpty());
    }

    //test ambiguous role mapping
    @Test
    public void testRoleInference_AmbiguousRoleMapping2(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y) isa relation1;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        Map<RoleType, Pair<VarName, Type>> roleVarTypeMap = relation.getRoleVarTypeMap();
        assertTrue(roleVarTypeMap.toString(), relation.getRoleVarTypeMap().isEmpty());
    }
    //test rule applicability for atom with unspecified roles but with possible unambiguous role mapping
    @Test
    public void testRuleApplicabilityViaType_UnambiguousRoleMapping(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$x isa entity1; $y isa entity2; $z isa entity3;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    //test rule applicability for atom with unspecified roles but with possible unambiguous role mapping
    @Test
    public void testRuleApplicabilityViaType_UnambiguousRoleMapping2(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$x isa entity1; $y isa entity2; $z isa entity4;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    //test rule applicability for atom with unspecified roles but with possible ambiguous role mapping
    @Test
    public void testRuleApplicabilityViaType_AmbiguousRoleMapping(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$x isa entity2; $y isa entity3; $z isa entity4;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    //test rule applicability for match-all atom
    @Test
    public void testRuleApplicabilityViaType_MatchAll(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y);}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    //test rule applicability for atom with unspecified roles with missing relation players without possible role mapping
    @Test
    public void testRuleApplicabilityViaType_MissingRelationPlayers2(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y);$x isa entity1; $y isa entity5;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(0, relation.getApplicableRules().size());
    }

    //test rule applicability for atom with unspecified roles with conflicting roles inferred from types with a wildcard
    @Test
    public void testRuleApplicabilityViaType_WithWildcard(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$y isa entity1; $z isa entity2;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    //test rule applicability for atom with unspecified roles with conflicting roles inferred from types with a wildcard
    @Test
    public void testRuleApplicabilityViaType_WithWildcard_MissingMappings(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y, $z);$y isa entity1; $z isa entity5;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(0, relation.getApplicableRules().size());
    }

    @Test
    public void testRuleApplicabilityViaType_MissingRelationPlayers(){
        GraknGraph graph = ruleApplicabilitySet.graph();
        String relationString = "{($x, $y);$x isa entity2; $y isa entity4;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    //test rule applicability for atom with unspecified roles with missing relation players but with possible ambiguous role mapping
    @Test
    public void testRuleApplicabilityViaType_MissingRelationPlayers_TypeContradiction(){
        GraknGraph graph = ruleApplicabilitySetWithTypes.graph();
        String relationString = "{($x, $y);$x isa entity2; $y isa entity4;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(0, relation.getApplicableRules().size());
    }

    @Test
    public void testRuleApplicabilityViaType_AmbiguousRoleMapping_TypeContradiction(){
        GraknGraph graph = ruleApplicabilitySetWithTypes.graph();
        String relationString = "{($x, $y, $z);$x isa entity2; $y isa entity3; $z isa entity4;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(0, relation.getApplicableRules().size());
    }

    @Test
    public void testRuleApplicabilityViaType_InstanceSubTypeMatchesRule(){
        GraknGraph graph = ruleApplicabilityInstanceTypesSet.graph();
        String relationString = "{$x isa entity1;(role1: $x, role2: $y) isa relation1;}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(1, relation.getApplicableRules().size());
    }

    @Ignore
    @Test
    public void testRuleApplicabilityViaType_InstancesDoNotMatchRule(){
        GraknGraph graph = ruleApplicabilityInstanceTypesSet.graph();
        Concept concept = getConcept(graph, "name", "a");
        Concept concept2 = getConcept(graph, "name", "a2");
        String relationString = "{" +
                "($x, $y) isa relation1;" +
                "$x id '" + concept.getId().getValue() + "';" +
                "$y id '" + concept2.getId().getValue() + "';" +
                "}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(0, relation.getApplicableRules().size());
    }

    @Ignore
    @Test
    public void testRuleApplicabilityViaType_InstancesDoNotMatchRule_NoRelationType(){
        GraknGraph graph = ruleApplicabilityInstanceTypesSet.graph();
        Concept concept = getConcept(graph, "name", "a");
        Concept concept2 = getConcept(graph, "name", "a2");
        String relationString = "{" +
                "($x, $y);" +
                "$x id '" + concept.getId().getValue() + "';" +
                "$y id '" + concept2.getId().getValue() + "';" +
                "}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(relationString, graph), graph).getAtom();
        assertEquals(0, relation.getApplicableRules().size());
    }

    @Test
    public void testTypeInference(){
        String typeId = snbGraph.graph().getType(TypeLabel.of("recommendation")).getId().getValue();
        String patternString = "{($x, $y); $x isa person; $y isa product;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        Atom atom = query.getAtom();
        assertTrue(atom.getTypeId().getValue().equals(typeId));
    }

    @Test
    public void testTypeInference2(){
        String typeId = cwGraph.graph().getType(TypeLabel.of("transaction")).getId().getValue();
        String patternString = "{($z, $y, $x);$z isa country;$x isa rocket;$y isa person;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, cwGraph.graph()), cwGraph.graph());
        Atom atom = query.getAtom();
        assertTrue(atom.getTypeId().getValue().equals(typeId));
    }

    @Test
    public void testUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String relation = "{(parent: $y, child: $x);}";
        String specialisedRelation = "{(father: $p, daughter: $c);}";
        String specialisedRelation2 = "{(daughter: $p, father: $c);}";

        Atomic atom = new ReasonerAtomicQuery(conjunction(relation, graph), graph).getAtom();
        Atomic specialisedAtom = new ReasonerAtomicQuery(conjunction(specialisedRelation, graph), graph).getAtom();
        Atomic specialisedAtom2 = new ReasonerAtomicQuery(conjunction(specialisedRelation2, graph), graph).getAtom();

        Unifier unifier = specialisedAtom.getUnifier(atom);
        Unifier unifier2 = specialisedAtom2.getUnifier(atom);
        Unifier correctUnifier = new UnifierImpl(
                ImmutableMap.of(
                    VarName.of("p"), VarName.of("y"),
                    VarName.of("c"), VarName.of("x"))
        );
        Unifier correctUnifier2 = new UnifierImpl(
                ImmutableMap.of(
                    VarName.of("p"), VarName.of("x"),
                    VarName.of("c"), VarName.of("y"))
        );
        assertTrue(unifier.toString(), unifier.getMappings().containsAll(correctUnifier.getMappings()));
        assertTrue(unifier2.toString(), unifier2.getMappings().containsAll(correctUnifier2.getMappings()));
    }

    @Test
    public void testUnification2() {
        GraknGraph graph = genealogyOntology.graph();
        String childString = "{(wife: $5b7a70db-2256-4d03-8fa4-2621a354899e, husband: $0f93f968-873a-43fa-b42f-f674c224ac04) isa marriage;}";
        String parentString = "{(wife: $x) isa marriage;}";
        Atom childAtom = new ReasonerAtomicQuery(conjunction(childString, graph), graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentString, graph), graph).getAtom();

        Unifier unifiers = childAtom.getUnifier(parentAtom);
        Unifier correctUnifiers = new UnifierImpl(
                ImmutableMap.of(VarName.of("5b7a70db-2256-4d03-8fa4-2621a354899e"), VarName.of("x"))
        );
        assertTrue(unifiers.getMappings().containsAll(correctUnifiers.getMappings()));

        Unifier reverseUnifiers = parentAtom.getUnifier(childAtom);
        Unifier correctReverseUnifiers = new UnifierImpl(
                ImmutableMap.of(VarName.of("x"), VarName.of("5b7a70db-2256-4d03-8fa4-2621a354899e"))
        );
        assertTrue(
                "Unifiers not in subset relation:\n" + correctReverseUnifiers.toString() + "\n" + reverseUnifiers.toString(),
                reverseUnifiers.getMappings().containsAll(correctReverseUnifiers.getMappings())
        );
    }

    @Test
    public void testRewriteAndUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String parentString = "{$r (wife: $x) isa marriage;}";
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentString, graph), graph).getAtom();

        String childPatternString = "(wife: $x, husband: $y) isa marriage";
        InferenceRule testRule = new InferenceRule(graph.admin().getMetaRuleInference().putRule(
                graph.graql().parsePattern(childPatternString),
                graph.graql().parsePattern(childPatternString)),
                graph);
        testRule.unify(parentAtom);
        Atom headAtom = testRule.getHead().getAtom();
        Map<RoleType, VarName> roleMap = headAtom.getRoleVarTypeMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getKey()));
        assertTrue(roleMap.get(graph.getRoleType("wife")).equals(VarName.of("x")));
    }

    @Test
    public void testRewrite(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "{(father: $x1, daughter: $x2) isa parentship;}";
        ReasonerAtomicQuery childQuery = new ReasonerAtomicQuery(conjunction(childRelation, graph), graph);
        Atom childAtom = childQuery.getAtom();

        Pair<Atom, Unifier> rewrite = childAtom.rewriteToUserDefinedWithUnifiers();
        Atom rewrittenAtom = rewrite.getKey();
        Unifier unifier = rewrite.getValue();
        Set<VarName> unifiedVariables = Sets.newHashSet(VarName.of("x1"), VarName.of("x2"));
        assertTrue(rewrittenAtom.isUserDefinedName());
        assertTrue(
                "Variables not in subset relation:\n" + unifier.keySet().toString() + "\n" + unifiedVariables.toString(),
                unifiedVariables.containsAll(unifier.keySet())
        );
    }

    @Test
    public void testIndirectRoleUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "{($r1: $x1, $r2: $x2) isa parentship;$r1 label 'father';$r2 label 'daughter';}";
        String parentRelation = "{($R1: $x, $R2: $y) isa parentship;$R1 label 'father';$R2 label 'daughter';}";
        Atom childAtom = new ReasonerAtomicQuery(conjunction(childRelation, graph), graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentRelation, graph), graph).getAtom();

        Unifier unifiers = childAtom.getUnifier(parentAtom);
        Unifier correctUnifiers = new UnifierImpl(
                ImmutableMap.of(
                    VarName.of("x1"), VarName.of("x"),
                    VarName.of("x2"), VarName.of("y"),
                    VarName.of("r1"), VarName.of("R1"),
                    VarName.of("r2"), VarName.of("R2"))
        );
        assertTrue(
                "Unifiers not in subset relation:\n" + correctUnifiers.toString() + "\n" + unifiers.toString(),
                unifiers.getMappings().containsAll(correctUnifiers.getMappings())
        );
    }

    @Test
    public void testIndirectRoleUnification2(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "{($r1: $x1, $r2: $x2);$r1 label 'father';$r2 label 'daughter';}";
        String parentRelation = "{($R1: $x, $R2: $y);$R1 label 'father';$R2 label 'daughter';}";

        Atom childAtom = new ReasonerAtomicQuery(conjunction(childRelation, graph), graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(conjunction(parentRelation, graph), graph).getAtom();
        Unifier unifiers = childAtom.getUnifier(parentAtom);
        Unifier correctUnifiers = new UnifierImpl(
                ImmutableMap.of(
                    VarName.of("x1"), VarName.of("x"),
                    VarName.of("x2"), VarName.of("y"),
                    VarName.of("r1"), VarName.of("R1"),
                    VarName.of("r2"), VarName.of("R2"))
        );
        assertTrue(
                "Unifiers not in subset relation:\n" + correctUnifiers.toString() + "\n" + unifiers.toString(),
                unifiers.getMappings().containsAll(correctUnifiers.getMappings())
        );
    }

    @Test
    public void testMatchAllUnification(){
        GraknGraph graph = snbGraph.graph();
        String childString = "{($z, $b) isa recommendation;}";
        String parentString = "{($a, $x);}";
        Relation relation = (Relation) new ReasonerAtomicQuery(conjunction(childString, graph), graph).getAtom();
        Relation parentRelation = (Relation) new ReasonerAtomicQuery(conjunction(parentString, graph), graph).getAtom();
        Unifier unifier = relation.getUnifier(parentRelation);
        relation.unify(unifier);
        assertEquals(unifier.size(), 2);
        Set<VarName> vars = relation.getVarNames();
        Set<VarName> correctVars = Sets.newHashSet(VarName.of("a"), VarName.of("x"));
        assertTrue(!vars.contains(VarName.of("")));
        assertTrue(vars.containsAll(correctVars));
    }

    @Test
    public void testMatchAllUnification2(){
        GraknGraph graph = snbGraph.graph();
        String parentString = "{$r($a, $x);}";
        Relation parent = (Relation) new ReasonerAtomicQuery(conjunction(parentString, graph), graph).getAtom();

        PatternAdmin body = graph.graql().parsePattern("(recommended-customer: $z, recommended-product: $b) isa recommendation").admin();
        PatternAdmin head = graph.graql().parsePattern("(recommended-customer: $z, recommended-product: $b) isa recommendation").admin();
        InferenceRule rule = new InferenceRule(graph.admin().getMetaRuleInference().putRule(body, head), graph);

        rule.unify(parent);
        Set<VarName> vars = rule.getHead().getAtom().getVarNames();
        Set<VarName> correctVars = Sets.newHashSet(VarName.of("r"), VarName.of("a"), VarName.of("x"));
        assertTrue(!vars.contains(VarName.of("")));
        assertTrue(
                "Variables not in subset relation:\n" + correctVars.toString() + "\n" + vars.toString(),
                vars.containsAll(correctVars)
        );
    }

    @Test
    public void testValuePredicateComparison(){
        GraknGraph graph = snbGraph.graph();
        String valueString = "{$x val '0';}";
        String valueString2 = "{$x val != 0;}";
        Atomic atom = new ReasonerQueryImpl(conjunction(valueString, graph), graph).getAtoms().iterator().next();
        Atomic atom2 =new ReasonerQueryImpl(conjunction(valueString2, graph), graph).getAtoms().iterator().next();
        assertTrue(!atom.isEquivalent(atom2));
    }

    @Test
    public void testMultiPredResourceEquivalence(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{$x has age $a;$a val >23; $a val <27;}";
        String patternString2 = "{$p has age $a;$a val >23;}";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(conjunction(patternString2, graph), graph);
        assertTrue(!query.getAtom().isEquivalent(query2.getAtom()));
    }

    @Test
    public void testNonexistentType(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{$x isa someType;}";
        exception.expect(IllegalArgumentException.class);
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(conjunction(patternString, graph), graph);
    }

    private Concept getConcept(GraknGraph graph, String typeName, Object val){
        return graph.graql().match(Graql.var("x").has(typeName, val).admin()).execute().iterator().next().get("x");
    }

    private Map<RoleType, VarName> roleMap(Map<RoleType, Pair<VarName, Type>> roleVarTypeMap) {
        return roleVarTypeMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getKey()));
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}

