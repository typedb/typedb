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
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graphs.CWGraph;
import ai.grakn.graphs.SNBGraph;
import ai.grakn.test.GraphContext;
import com.google.common.collect.Sets;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.pattern.Patterns.varName;
import static ai.grakn.test.GraknTestEnv.usingTinker;
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

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
        Reasoner.linkConceptTypes(snbGraph.graph());
        Reasoner.linkConceptTypes(cwGraph.graph());
    }

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testRecursive(){
        String recRelString = "match ($x, $y) isa resides;";
        String nrecRelString = "match ($x, $y) isa recommendation;";
        ReasonerAtomicQuery recQuery = new ReasonerAtomicQuery(recRelString, snbGraph.graph());
        ReasonerAtomicQuery nrecQuery = new ReasonerAtomicQuery(nrecRelString, snbGraph.graph());
        assertTrue(recQuery.getAtom().isRecursive());
        assertTrue(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testFactory(){
        String atomString = "match $x isa person;";
        String relString = "match ($x, $y) isa recommendation;";
        String resString = "match $x has gender 'male';";

        Atom atom = new ReasonerAtomicQuery(atomString, snbGraph.graph()).getAtom();
        Atom relation = new ReasonerAtomicQuery(relString, snbGraph.graph()).getAtom();
        Atom res = new ReasonerAtomicQuery(resString, snbGraph.graph()).getAtom();

        assertTrue(atom.isType());
        assertTrue(relation.isRelation());
        assertTrue(res.isResource());
    }

    @Test
    public void testRoleInference(){
        String queryString = "match ($z, $y) isa owns; $z isa country; $y isa rocket; select $y, $z;";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(queryString, cwGraph.graph());
        Atom atom = query.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap = atom.getRoleVarTypeMap();

        String queryString2 = "match isa owns, ($z, $y); $z isa country; select $y, $z;";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(queryString2, cwGraph.graph());
        Atom atom2 = query2.getAtom();

        Map<RoleType, Pair<VarName, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertEquals(2, roleMap.size());
        assertEquals(2, roleMap2.size());
        assertEquals(roleMap.keySet(), roleMap2.keySet());
    }

    @Test
    public void testRoleInference2(){
        String queryString = "match ($z, $y, $x), isa transaction;$z isa country;$x isa person; select $x, $y, $z;";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(queryString, cwGraph.graph());
        Atom atom = query.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap = atom.getRoleVarTypeMap();

        String queryString2 = "match ($z, $y, seller: $x), isa transaction;$z isa country;$y isa rocket; select $x, $y, $z;";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(queryString2, cwGraph.graph());
        Atom atom2 = query2.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertEquals(3, roleMap.size());
        assertEquals(3, roleMap2.size());
        assertEquals(roleMap.keySet(), roleMap2.keySet());
    }

    @Test
    public void testRoleInference3(){
        GraknGraph graph = genealogyOntology.graph();
        Relation relation = (Relation) new ReasonerAtomicQuery("match ($p, son: $gc) isa parentship;", graph).getAtom();
        Relation correctFullRelation = (Relation) new ReasonerAtomicQuery("match (parent: $p, son: $gc) isa parentship;", graph).getAtom();
        Relation relation2 = (Relation) new ReasonerAtomicQuery("match (father: $gp, $p) isa parentship;", graph).getAtom();
        Relation correctFullRelation2 = (Relation) new ReasonerAtomicQuery("match (father: $gp, child: $p) isa parentship;", graph).getAtom();

        assertTrue(relation.getRoleVarTypeMap().equals(correctFullRelation.getRoleVarTypeMap()));
        assertTrue(relation2.getRoleVarTypeMap().equals(correctFullRelation2.getRoleVarTypeMap()));
    }

    @Test
    public void testTypeInference(){
        String typeId = snbGraph.graph().getType(TypeName.of("recommendation")).getId().getValue();
        String queryString = "match ($x, $y); $x isa person; $y isa product;";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(queryString, snbGraph.graph());
        Atom atom = query.getAtom();
        assertTrue(atom.getTypeId().getValue().equals(typeId));
    }

    @Test
    public void testTypeInference2(){
        String typeId = cwGraph.graph().getType(TypeName.of("transaction")).getId().getValue();
        String queryString = "match ($z, $y, $x);$z isa country;$x isa rocket;$y isa person;";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(queryString, cwGraph.graph());
        Atom atom = query.getAtom();
        assertTrue(atom.getTypeId().getValue().equals(typeId));
    }

    @Test
    public void testUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String relation = "match (parent: $y, child: $x);";
        String specialisedRelation = "match (father: $p, daughter: $c);";
        String specialisedRelation2 = "match (daughter: $p, father: $c);";

        Atomic atom = new ReasonerAtomicQuery(relation, graph).getAtom();
        Atomic specialisedAtom = new ReasonerAtomicQuery(specialisedRelation, graph).getAtom();
        Atomic specialisedAtom2 = new ReasonerAtomicQuery(specialisedRelation2, graph).getAtom();

        Map<VarName, VarName> unifiers = specialisedAtom.getUnifiers(atom);
        Map<VarName, VarName> unifiers2 = specialisedAtom2.getUnifiers(atom);
        Map<VarName, VarName> correctUnifiers = new HashMap<>();
        correctUnifiers.put(varName("p"), varName("y"));
        correctUnifiers.put(varName("c"), varName("x"));
        Map<VarName, VarName> correctUnifiers2 = new HashMap<>();
        correctUnifiers2.put(varName("p"), varName("x"));
        correctUnifiers2.put(varName("c"), varName("y"));
        assertTrue(unifiers.toString(), unifiers.entrySet().containsAll(correctUnifiers.entrySet()));
        assertTrue(unifiers2.toString(), unifiers2.entrySet().containsAll(correctUnifiers2.entrySet()));
    }

    @Test
    public void testUnification2() {
        GraknGraph graph = genealogyOntology.graph();
        String childString = "match (wife: $5b7a70db-2256-4d03-8fa4-2621a354899e, husband: $0f93f968-873a-43fa-b42f-f674c224ac04) isa marriage;";
        String parentString = "match (wife: $x) isa marriage;";
        Atom childAtom = new ReasonerAtomicQuery(childString, graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(parentString, graph).getAtom();

        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);
        Map<VarName, VarName> correctUnifiers = new HashMap<>();
        correctUnifiers.put(varName("5b7a70db-2256-4d03-8fa4-2621a354899e"), varName("x"));
        assertTrue(unifiers.entrySet().containsAll(correctUnifiers.entrySet()));

        Map<VarName, VarName> reverseUnifiers = parentAtom.getUnifiers(childAtom);
        Map<VarName, VarName> correctReverseUnifiers = new HashMap<>();
        correctReverseUnifiers.put(varName("x"), varName("5b7a70db-2256-4d03-8fa4-2621a354899e"));
        assertTrue(reverseUnifiers.entrySet().containsAll(correctReverseUnifiers.entrySet()));
    }

    @Test
    public void testRewriteAndUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String parentString = "match $r (wife: $x) isa marriage;";
        Atom parentAtom = new ReasonerAtomicQuery(parentString, graph).getAtom();

        String childPatternString = "(wife: $x, husband: $y) isa marriage";
        InferenceRule testRule = new InferenceRule(graph.admin().getMetaRuleInference().addRule(
                graph.graql().parsePattern(childPatternString),
                graph.graql().parsePattern(childPatternString)),
                graph);
        testRule.unify(parentAtom);
        Atom headAtom = testRule.getHead().getAtom();
        Map<VarName, Pair<Type, RoleType>> varTypeRoleMap = headAtom.getVarTypeRoleMap();
        assertTrue(varTypeRoleMap.get(varName("x")).getValue().equals(graph.getRoleType("wife")));
    }

    @Test
    public void testRewrite(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "match (father: $x1, daughter: $x2) isa parentship;";
        String parentRelation = "match $r (father: $x, daughter: $y) isa parentship;";
        ReasonerAtomicQuery childQuery = new ReasonerAtomicQuery(childRelation, graph);
        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(parentRelation, graph).getAtom();

        Pair<Atom, Map<VarName, VarName>> rewrite = childAtom.rewrite(parentAtom, childQuery);
        Atom rewrittenAtom = rewrite.getKey();
        Map<VarName, VarName> unifiers = rewrite.getValue();
        Set<VarName> unifiedVariables = Sets.newHashSet(varName("x1"), varName("x2"));
        assertTrue(rewrittenAtom.isUserDefinedName());
        assertTrue(unifiedVariables.containsAll(unifiers.keySet()));
    }

    @Test
    public void testIndirectRoleUnification(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "match ($r1: $x1, $r2: $x2) isa parentship;$r1 type-name 'father';$r2 type-name 'daughter';";
        String parentRelation = "match ($R1: $x, $R2: $y) isa parentship;$R1 type-name 'father';$R2 type-name 'daughter';";
        Atom childAtom = new ReasonerAtomicQuery(childRelation, graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(parentRelation, graph).getAtom();

        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);
        Map<VarName, VarName> correctUnifiers = new HashMap<>();
        correctUnifiers.put(varName("x1"), varName("x"));
        correctUnifiers.put(varName("x2"), varName("y"));
        correctUnifiers.put(varName("r1"), varName("R1"));
        correctUnifiers.put(varName("r2"), varName("R2"));
        assertTrue(unifiers.entrySet().containsAll(correctUnifiers.entrySet()));
    }

    @Test
    public void testIndirectRoleUnification2(){
        GraknGraph graph = genealogyOntology.graph();
        String childRelation = "match ($r1: $x1, $r2: $x2);$r1 type-name 'father';$r2 type-name 'daughter';";
        String parentRelation = "match ($R1: $x, $R2: $y);$R1 type-name 'father';$R2 type-name 'daughter';";

        Atom childAtom = new ReasonerAtomicQuery(childRelation, graph).getAtom();
        Atom parentAtom = new ReasonerAtomicQuery(parentRelation, graph).getAtom();
        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);
        Map<VarName, VarName> correctUnifiers = new HashMap<>();
        correctUnifiers.put(varName("x1"), varName("x"));
        correctUnifiers.put(varName("x2"), varName("y"));
        correctUnifiers.put(varName("r1"), varName("R1"));
        correctUnifiers.put(varName("r2"), varName("R2"));
        assertTrue(unifiers.entrySet().containsAll(correctUnifiers.entrySet()));
    }

    @Test
    public void testMatchAllUnification(){
        Relation relation = (Relation) new ReasonerAtomicQuery("match ($z, $b) isa recommendation;", snbGraph.graph()).getAtom();
        Relation parentRelation = (Relation) new ReasonerAtomicQuery("match ($a, $x);", snbGraph.graph()).getAtom();
        Map<VarName, VarName> unifiers = relation.getUnifiers(parentRelation);
        relation.unify(unifiers);
        assertEquals(unifiers.size(), 2);
        Set<VarName> vars = relation.getVarNames();
        Set<VarName> correctVars = new HashSet<>();
        correctVars.add(varName("a"));
        correctVars.add(varName("x"));
        assertTrue(!vars.contains(varName("")));
        assertTrue(vars.containsAll(correctVars));
    }

    @Test
    public void testMatchAllUnification2(){
        Relation parent = (Relation) new ReasonerAtomicQuery("match $r($a, $x);", snbGraph.graph()).getAtom();
        PatternAdmin body = snbGraph.graph().graql().parsePattern("($z, $b) isa recommendation").admin();
        PatternAdmin head = snbGraph.graph().graql().parsePattern("($z, $b) isa recommendation").admin();
        InferenceRule rule = new InferenceRule(snbGraph.graph().admin().getMetaRuleInference().addRule(body, head), snbGraph.graph());

        rule.unify(parent);
        Set<VarName> vars = rule.getHead().getAtom().getVarNames();
        Set<VarName> correctVars = new HashSet<>();
        correctVars.add(varName("r"));
        correctVars.add(varName("a"));
        correctVars.add(varName("x"));
        assertTrue(!vars.contains(varName("")));
        assertTrue(vars.toString(), vars.containsAll(correctVars));
    }

    @Test
    public void testValuePredicateComparison(){
        Atomic atom = new ReasonerQueryImpl("match $x value '0';", snbGraph.graph()).getAtoms().iterator().next();
        Atomic atom2 =new ReasonerQueryImpl("match $x value != '0';", snbGraph.graph()).getAtoms().iterator().next();
        assertTrue(!atom.isEquivalent(atom2));
    }

    @Test
    public void testMultiPredResourceEquivalence(){
        String queryString = "match $x has age $a;$a value >23; $a value <27;";
        String queryString2 = "match $p has age $a;$a value >23;";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(queryString, snbGraph.graph());
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(queryString2, snbGraph.graph());
        assertTrue(!query.getAtom().isEquivalent(query2.getAtom()));
    }
}
