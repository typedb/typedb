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
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.test.graql.reasoner.graphs.CWGraph;
import ai.grakn.test.graql.reasoner.graphs.SNBGraph;
import ai.grakn.test.graql.reasoner.graphs.TestGraph;
import ai.grakn.util.ErrorMessage;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static java.util.stream.Collectors.toSet;
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

        ReasonerAtomicQuery recQuery = new ReasonerAtomicQuery(recRelString, graph);
        ReasonerAtomicQuery nrecQuery = new ReasonerAtomicQuery(nrecRelString, graph);

        assert(recQuery.getAtom().isRecursive());
        assert(!nrecQuery.getAtom().isRecursive());
    }

    @Test
    public void testFactory(){
        GraknGraph graph = snbGraph;
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
        GraknGraph graph = cwGraph;
        String queryString = "match ($z, $y) isa owns; $z isa country; $y isa rocket; select $y, $z;";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        String queryString2 = "match isa owns, ($z, $y); $z isa country; select $y, $z;";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(queryString2, graph);
        Atom atom2 = query2.getAtom();

        Map<RoleType, Pair<String, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertEquals(2, roleMap.size());
        assertEquals(2, roleMap2.size());
        assertEquals(roleMap.keySet(), roleMap2.keySet());
    }

    @Test
    public void testRoleInference2(){
        GraknGraph graph = cwGraph;
        String queryString = "match ($z, $y, $x), isa transaction;$z isa country;$x isa person; select $x, $y, $z;";
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        String queryString2 = "match ($z, $y, seller: $x), isa transaction;$z isa country;$y isa rocket; select $x, $y, $z;";
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(queryString2, graph);
        Atom atom2 = query2.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertEquals(3, roleMap.size());
        assertEquals(3, roleMap2.size());
        assertEquals(roleMap.keySet(), roleMap2.keySet());
    }

    @Test
    public void testRoleInference3(){
        GraknGraph graph = TestGraph.getGraph(null, "genealogy/ontology.gql");

        Relation relation = (Relation) new ReasonerAtomicQuery("match ($p, son: $gc) isa parentship;", graph).getAtom();
        Relation correctFullRelation = (Relation) new ReasonerAtomicQuery("match (parent: $p, son: $gc) isa parentship;", graph).getAtom();
        Relation relation2 = (Relation) new ReasonerAtomicQuery("match (father: $gp, $p) isa parentship;", graph).getAtom();
        Relation correctFullRelation2 = (Relation) new ReasonerAtomicQuery("match (father: $gp, child: $p) isa parentship;", graph).getAtom();

        assertTrue(relation.getRoleVarTypeMap().equals(correctFullRelation.getRoleVarTypeMap()));
        assertTrue(relation2.getRoleVarTypeMap().equals(correctFullRelation2.getRoleVarTypeMap()));
    }

    @Test
    public void testUnification(){
        GraknGraph graph = TestGraph.getGraph(null, "genealogy/ontology.gql");

        String relation = "match (parent: $y, child: $x);";
        String specialisedRelation = "match (father: $p, daughter: $c);";
        String specialisedRelation2 = "match (daughter: $p, father: $c);";

        Atomic atom = new ReasonerAtomicQuery(relation, graph).getAtom();
        Atomic specialisedAtom = new ReasonerAtomicQuery(specialisedRelation, graph).getAtom();
        Atomic specialisedAtom2 = new ReasonerAtomicQuery(specialisedRelation2, graph).getAtom();

        Map<String, String> unifiers = specialisedAtom.getUnifiers(atom);
        Map<String, String> unifiers2 = specialisedAtom2.getUnifiers(atom);
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
        Relation relation = (Relation) new ReasonerAtomicQuery("match ($z, $b) isa recommendation;", graph).getAtom();
        Relation parentRelation = (Relation) new ReasonerAtomicQuery("match ($a, $x);", graph).getAtom();
        Map<String, String> unifiers = relation.getUnifiers(parentRelation);
        relation.unify(unifiers);
        assertTrue(unifiers.size() == 2);
        Set<String> vars = relation.getVarNames();
        Set<String> correctVars = new HashSet<>();
        correctVars.add("a");
        correctVars.add("x");
        assertTrue(!vars.contains(""));
        assertTrue(vars.containsAll(correctVars));

    }

    @Test
    public void testMatchAllUnification2(){
        GraknGraph graph = snbGraph;
        Relation parent = (Relation) new ReasonerAtomicQuery("match $r($a, $x);", graph).getAtom();
        PatternAdmin body = graph.graql().parsePattern("($z, $b) isa recommendation").admin();
        PatternAdmin head = graph.graql().parsePattern("($z, $b) isa recommendation").admin();
        InferenceRule rule = new InferenceRule(graph.admin().getMetaRuleInference().addRule(body, head), graph);

        rule.unify(parent);
        Set<String> vars = rule.getHead().getAtom().getVarNames();
        Set<String> correctVars = new HashSet<>();
        correctVars.add("r");
        correctVars.add("a");
        correctVars.add("x");
        assertTrue(!vars.contains(""));
        assertTrue(vars.containsAll(correctVars));
    }


    @Test
    public void testValuePredicateComparison(){
        GraknGraph graph = snbGraph;
        String valueString = "{$x value '0';}";
        String valueString2 = "{$x value != 0;}";
        Atomic atom = new ReasonerQueryImpl(conjunction(valueString, graph), graph).getAtoms().iterator().next();
        Atomic atom2 =new ReasonerQueryImpl(conjunction(valueString2, graph), graph).getAtoms().iterator().next();
        assertTrue(!atom.isEquivalent(atom2));
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
