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
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.test.graql.reasoner.graphs.CWGraph;
import ai.grakn.test.graql.reasoner.graphs.SNBGraph;
import ai.grakn.util.ErrorMessage;
import javafx.util.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicTest extends AbstractEngineTest{

    private static GraknGraph snbGraph;
    private static GraknGraph cwGraph;
    private static Reasoner snbReasoner;
    private static Reasoner cwReasoner;

    @BeforeClass
    public static void onStartup(){
        assumeTrue(usingTinker());
        snbGraph = SNBGraph.getGraph();
        cwGraph = CWGraph.getGraph();
        snbReasoner = new Reasoner(snbGraph);
        cwReasoner = new Reasoner(cwGraph);
    }

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testNonVar(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        GraknGraph graph = snbGraph;
        QueryBuilder qb = graph.graql();
        String atomString = "match $x isa person;";

        Query query = new Query(atomString, graph);
        Atomic atom = AtomicFactory.create(qb.<MatchQuery>parse(atomString).admin().getPattern());
    }

    @Test
    public void testNonVa2r(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        GraknGraph graph = snbGraph;
        QueryBuilder qb = graph.graql();
        String atomString = "match $x isa person;";

        Query query = new Query(atomString, graph);
        Atomic atom =  AtomicFactory.create(qb.<MatchQuery>parse(atomString).admin().getPattern(), query);
    }

    @Test
    public void testParentMissing(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        GraknGraph graph = snbGraph;
        QueryBuilder qb = graph.graql();
        String recRelString = "match ($x, $y) isa resides;";

        Atomic recRel = AtomicFactory.create(qb.<MatchQuery>parse(recRelString).admin().getPattern().getPatterns().iterator().next());
        assert(recRel.isRecursive());
    }

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
        QueryBuilder qb = graph.graql();
        String atomString = "match $x isa person;";
        String relString = "match ($x, $y) isa recommendation;";
        String resString = "match $x has gender 'male';";

        Atomic atom = AtomicFactory.create(qb.<MatchQuery>parse(atomString).admin().getPattern().getPatterns().iterator().next());
        Atomic relation = AtomicFactory.create(qb.<MatchQuery>parse(relString).admin().getPattern().getPatterns().iterator().next());
        Atomic res = AtomicFactory.create(qb.<MatchQuery>parse(resString).admin().getPattern().getPatterns().iterator().next());

        assert(((Atom) atom).isType());
        assert(((Atom) relation).isRelation());
        assert(((Atom) res).isResource());
    }

    @Test
    public void testRoleInference(){
        GraknGraph graph = cwGraph;
        String queryString = "match ($z, $y) isa owns; $z isa country; $y isa rocket; select $y, $z;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        String queryString2 = "match isa owns, ($z, $y); $z isa country; select $y, $z;";
        AtomicQuery query2 = new AtomicQuery(queryString2, graph);
        Atom atom2 = query2.getAtom();

        Map<RoleType, Pair<String, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertTrue(roleMap.size() == 2 && roleMap2.size() == 2);
        assertTrue(roleMap.keySet().equals(roleMap2.keySet()));
    }

    @Test
    public void testRoleInference2(){
        GraknGraph graph = cwGraph;
        String queryString = "match ($z, $y, $x), isa transaction;$z isa country;$x isa person; select $x, $y, $z;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        String queryString2 = "match ($z, $y, seller: $x), isa transaction;$z isa country;$y isa rocket; select $x, $y, $z;";
        AtomicQuery query2 = new AtomicQuery(queryString2, graph);
        Atom atom2 = query2.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap2 = atom2.getRoleVarTypeMap();
        assertTrue(roleMap.size() == 3 && roleMap2.size() == 3);
        assertTrue(roleMap.keySet().equals(roleMap2.keySet()));
    }


    @Test
    public void testValuePredicateComparison(){
        GraknGraph graph = snbGraph;
        QueryBuilder qb = graph.graql();
        Atomic atom = AtomicFactory.create(qb.parsePatterns("$x value '0';").iterator().next().admin());
        Atomic atom2 = AtomicFactory.create(qb.parsePatterns("$x value != '0';").iterator().next().admin());
        assertTrue(!atom.isEquivalent(atom2));
    }

    @Test
    public void testBinaryComparison() {
        GraknGraph graph = snbGraph;
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String recRelString = "match $x has name $y;";
        String nrecRelString = "match $x has name $y;";

        Atomic resAtom = AtomicFactory
                .create(qb.<MatchQuery>parse(recRelString).admin().getPattern().getPatterns().iterator().next()
                        , new Query(recRelString, graph));
        Atomic resAtom2 = AtomicFactory
                .create(qb.<MatchQuery>parse(nrecRelString).admin().getPattern().getPatterns().iterator().next()
                        , new Query(recRelString, graph));
    }
}
