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
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarName;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.test.graql.reasoner.graphs.CWGraph;
import ai.grakn.test.graql.reasoner.graphs.SNBGraph;
import ai.grakn.test.graql.reasoner.graphs.TestGraph;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.pattern.Patterns.varName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicTest extends AbstractEngineTest{

    private static GraknGraph snbGraph;
    private static GraknGraph cwGraph;

    @BeforeClass
    public static void onStartup(){
        assumeTrue(usingTinker());
        snbGraph = SNBGraph.getGraph();
        cwGraph = CWGraph.getGraph();
        Reasoner.linkConceptTypes(snbGraph);
        Reasoner.linkConceptTypes(cwGraph);
    }

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testRecursive(){
        GraknGraph graph = snbGraph;
        String recRelString = "match ($x, $y) isa resides;";
        String nrecRelString = "match ($x, $y) isa recommendation;";

        AtomicQuery recQuery = new AtomicQuery(recRelString, graph);
        AtomicQuery nrecQuery = new AtomicQuery(nrecRelString, graph);

        assert(recQuery.getAtom().isRecursive());
        assert(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testFactory(){
        GraknGraph graph = snbGraph;
        String atomString = "match $x isa person;";
        String relString = "match ($x, $y) isa recommendation;";
        String resString = "match $x has gender 'male';";

        Atom atom = new AtomicQuery(atomString, snbGraph).getAtom();
        Atom relation = new AtomicQuery(relString, snbGraph).getAtom();
        Atom res = new AtomicQuery(resString, snbGraph).getAtom();

        assert(atom.isType());
        assert(relation.isRelation());
        assert(res.isResource());
    }

    @Test
    public void testRoleInference(){
        GraknGraph graph = cwGraph;
        String queryString = "match ($z, $y) isa owns; $z isa country; $y isa rocket; select $y, $z;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap = atom.getRoleVarTypeMap();

        String queryString2 = "match isa owns, ($z, $y); $z isa country; select $y, $z;";
        AtomicQuery query2 = new AtomicQuery(queryString2, graph);
        Atom atom2 = query2.getAtom();

        Map<RoleType, Pair<VarName, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertEquals(2, roleMap.size());
        assertEquals(2, roleMap2.size());
        assertEquals(roleMap.keySet(), roleMap2.keySet());
    }

    @Test
    public void testRoleInference2(){
        GraknGraph graph = cwGraph;
        String queryString = "match ($z, $y, $x), isa transaction;$z isa country;$x isa person; select $x, $y, $z;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap = atom.getRoleVarTypeMap();

        String queryString2 = "match ($z, $y, seller: $x), isa transaction;$z isa country;$y isa rocket; select $x, $y, $z;";
        AtomicQuery query2 = new AtomicQuery(queryString2, graph);
        Atom atom2 = query2.getAtom();
        Map<RoleType, Pair<VarName, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertEquals(3, roleMap.size());
        assertEquals(3, roleMap2.size());
        assertEquals(roleMap.keySet(), roleMap2.keySet());
    }

    @Test
    public void testRoleInference3(){
        GraknGraph graph = TestGraph.getGraph(null, "genealogy/ontology.gql");

        Relation relation = (Relation) new AtomicQuery("match ($p, son: $gc) isa parentship;", graph).getAtom();
        Relation correctFullRelation = (Relation) new AtomicQuery("match (parent: $p, son: $gc) isa parentship;", graph).getAtom();
        Relation relation2 = (Relation) new AtomicQuery("match (father: $gp, $p) isa parentship;", graph).getAtom();
        Relation correctFullRelation2 = (Relation) new AtomicQuery("match (father: $gp, child: $p) isa parentship;", graph).getAtom();

        assertTrue(relation.getRoleVarTypeMap().equals(correctFullRelation.getRoleVarTypeMap()));
        assertTrue(relation2.getRoleVarTypeMap().equals(correctFullRelation2.getRoleVarTypeMap()));
    }

    @Test
    public void testTypeInference(){
        GraknGraph graph = snbGraph;
        String typeId = graph.getType("recommendation").getId();
        String queryString = "match ($x, $y); $x isa person; $y isa product;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        assertTrue(atom.getTypeId().equals(typeId));
    }

    @Test
    public void testTypeInference2(){
        GraknGraph graph = cwGraph;
        String typeId = graph.getType("transaction").getId();
        String queryString = "match ($z, $y, $x);$z isa country;$x isa rocket;$y isa person;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        assertTrue(atom.getTypeId().equals(typeId));
    }

    @Test
    public void testUnification(){
        GraknGraph graph = TestGraph.getGraph(null, "genealogy/ontology.gql");

        String relation = "match (parent: $y, child: $x);";
        String specialisedRelation = "match (father: $p, daughter: $c);";
        String specialisedRelation2 = "match (daughter: $p, father: $c);";

        Atomic atom = new AtomicQuery(relation, graph).getAtom();
        Atomic specialisedAtom = new AtomicQuery(specialisedRelation, graph).getAtom();
        Atomic specialisedAtom2 = new AtomicQuery(specialisedRelation2, graph).getAtom();

        Map<VarName, VarName> unifiers = specialisedAtom.getUnifiers(atom);
        Map<VarName, VarName> unifiers2 = specialisedAtom2.getUnifiers(atom);
        Map<String, String> correctUnifiers = new HashMap<>();
        correctUnifiers.put("p", "y");
        correctUnifiers.put("c", "x");
        Map<String, String> correctUnifiers2 = new HashMap<>();
        correctUnifiers2.put("p", "x");
        correctUnifiers2.put("c", "y");
        assertTrue(unifiers.entrySet().containsAll(correctUnifiers.entrySet()));
        assertTrue(unifiers2.entrySet().containsAll(correctUnifiers2.entrySet()));
    }

    @Test
    public void testMatchAllUnification(){
        GraknGraph graph = snbGraph;
        Relation relation = (Relation) new AtomicQuery("match ($z, $b) isa recommendation;", graph).getAtom();
        Relation parentRelation = (Relation) new AtomicQuery("match ($a, $x);", graph).getAtom();
        Map<VarName, VarName> unifiers = relation.getUnifiers(parentRelation);
        relation.unify(unifiers);
        assertTrue(unifiers.size() == 2);
        Set<VarName> vars = relation.getVarNames();
        Set<VarName> correctVars = new HashSet<>();
        correctVars.add(varName("a"));
        correctVars.add(varName("x"));
        assertTrue(!vars.contains(varName("")));
        assertTrue(vars.containsAll(correctVars));

    }

    @Test
    public void testMatchAllUnification2(){
        GraknGraph graph = snbGraph;
        Relation parent = (Relation) new AtomicQuery("match $r($a, $x);", graph).getAtom();
        PatternAdmin body = graph.graql().parsePattern("($z, $b) isa recommendation").admin();
        PatternAdmin head = graph.graql().parsePattern("($z, $b) isa recommendation").admin();
        InferenceRule rule = new InferenceRule(graph.admin().getMetaRuleInference().addRule(body, head), graph);

        rule.unify(parent);
        Set<VarName> vars = rule.getHead().getAtom().getVarNames();
        Set<VarName> correctVars = new HashSet<>();
        correctVars.add(varName("r"));
        correctVars.add(varName("a"));
        correctVars.add(varName("x"));
        assertTrue(!vars.contains(varName("")));
        assertTrue(vars.containsAll(correctVars));
    }


    @Test
    public void testValuePredicateComparison(){
        Atomic atom = new Query("match $x value '0';", snbGraph).getAtoms().iterator().next();
        Atomic atom2 =new Query("match $x value != '0';", snbGraph).getAtoms().iterator().next();
        assertTrue(!atom.isEquivalent(atom2));
    }

    @Test
    public void testMultiPredResourceEquivalence(){
        GraknGraph graph = SNBGraph.getGraph();
        String queryString = "match $x has age $a;$a value >23; $a value <27;";
        String queryString2 = "match $p has age $a;$a value >23;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        AtomicQuery query2 = new AtomicQuery(queryString2, graph);
        assertTrue(!query.getAtom().isEquivalent(query2.getAtom()));
    }
}
