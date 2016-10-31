/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.test.graql.reasoner;

import com.google.common.collect.Sets;
import io.grakn.GraknGraph;
import io.grakn.concept.RelationType;
import io.grakn.concept.RoleType;
import io.grakn.concept.Type;
import io.grakn.graql.Graql;
import io.grakn.graql.MatchQuery;
import io.grakn.graql.QueryBuilder;
import io.grakn.graql.Reasoner;
import io.grakn.graql.internal.reasoner.atom.Atom;
import io.grakn.graql.internal.reasoner.atom.Atomic;
import io.grakn.graql.internal.reasoner.atom.AtomicFactory;
import io.grakn.graql.internal.reasoner.atom.Predicate;
import io.grakn.graql.internal.reasoner.atom.Relation;
import io.grakn.graql.internal.reasoner.query.AtomicQuery;
import io.grakn.graql.internal.reasoner.query.Query;
import io.grakn.test.graql.reasoner.graphs.CWGraph;
import io.grakn.test.graql.reasoner.graphs.GenericGraph;
import io.grakn.test.graql.reasoner.graphs.SNBGraph;
import io.grakn.util.ErrorMessage;
import javafx.util.Pair;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static io.grakn.graql.internal.reasoner.Utility.computeRoleCombinations;
import static org.junit.Assert.assertTrue;

public class AtomicTest {

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testNonVar(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        String atomString = "match $x isa person;";

        Query query = new Query(atomString, graph);
        Atomic atom = AtomicFactory.create(qb.<MatchQuery>parse(atomString).admin().getPattern());
    }

    @Test
    public void testNonVa2r(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        String atomString = "match $x isa person;";

        Query query = new Query(atomString, graph);
        Atomic atom =  AtomicFactory.create(qb.<MatchQuery>parse(atomString).admin().getPattern(), query);
    }

    @Test
    public void testParentMissing(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        String recRelString = "match ($x, $y) isa resides;";

        Atomic recRel = AtomicFactory.create(qb.<MatchQuery>parse(recRelString).admin().getPattern().getPatterns().iterator().next());

        assert(recRel.isRecursive());
    }

    @Test
    public void testRecursive(){
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String recRelString = "match ($x, $y) isa resides;";
        String nrecRelString = "match ($x, $y) isa recommendation;";

        Atomic recRel = AtomicFactory
                .create(qb.<MatchQuery>parse(recRelString).admin().getPattern().getPatterns().iterator().next()
                        , new Query(recRelString, graph));
        Atomic nrecRel = AtomicFactory
                .create(qb.<MatchQuery>parse(nrecRelString).admin().getPattern().getPatterns().iterator().next()
                        , new Query(recRelString, graph));

        assert(recRel.isRecursive());
        assert(!nrecRel.isRecursive());
    }

    @Test
    public void testFactory(){
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        String atomString = "match $x isa person;";
        String relString = "match ($x, $y) isa recommendation;";
        String subString = "match $x id 'Bob';";
        String resString = "match $x has gender 'male';";

        Atomic atom = AtomicFactory.create(qb.<MatchQuery>parse(atomString).admin().getPattern().getPatterns().iterator().next());
        Atomic relation = AtomicFactory.create(qb.<MatchQuery>parse(relString).admin().getPattern().getPatterns().iterator().next());
        Atomic sub = AtomicFactory.create(qb.<MatchQuery>parse(subString).admin().getPattern().getPatterns().iterator().next());
        Atomic res = AtomicFactory.create(qb.<MatchQuery>parse(resString).admin().getPattern().getPatterns().iterator().next());

        assert(((Atom) atom).isType());
        assert(((Atom) relation).isRelation());
        assert(((Atom) res).isResource());
        assert(((Predicate) sub).isIdPredicate());
    }

    @Test
    public void testRoleInference(){
        GraknGraph graph = CWGraph.getGraph();
        String queryString = "match isa owns, ($z, $y); $z isa country; $y isa weapon; select $y, $z;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        queryString = "match isa owns, ($z, $y); $z isa country; select $y, $z;";
        query = new AtomicQuery(queryString, graph);
        atom = query.getAtom();

        Map<RoleType, Pair<String, Type>> roleMap2 = atom.getRoleVarTypeMap();
        assert(roleMap.size() == 2 && roleMap2.size() == 2);
    }

    @Test
    public void testRoleInference2(){
        GraknGraph graph = CWGraph.getGraph();
        String queryString = "match ($z, $y, $x), isa transaction;$z isa country;$x isa person; select $x, $y, $z;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        Atom atom = query.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        queryString = "match ($z, $y, seller: $x), isa transaction;$z isa country;$y isa weapon; select $x, $y, $z;";
        query = new AtomicQuery(queryString, graph);
        atom = query.getAtom();
        Map<RoleType, Pair<String, Type>> roleMap2 = atom.getRoleVarTypeMap();
        assert(roleMap.size() == 3 && roleMap2.size() == 3);
    }

    @Test
    public void testRelationConstructor(){
        GraknGraph graph = GenericGraph.getGraph("geo-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);

        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery MQ = qb.parse(queryString);
        AtomicQuery query = new AtomicQuery(MQ, graph);

        Atom atom = query.getAtom();
        Set<String> vars = atom.getVarNames();

        String relTypeId = atom.getTypeId();
        RelationType relType = graph.getRelationType(relTypeId);
        Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

        Set<Map<String, String>> roleMaps = new HashSet<>();
        computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

        Collection<Relation> rels = new LinkedList<>();
        roleMaps.forEach( map -> rels.add(new Relation(relTypeId, map, null)));
    }

    @Test
    public void testRelationConstructor2(){
        GraknGraph graph = GenericGraph.getGraph("geo-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);

        String queryString = "match ($x, $y, $z) isa ternary-relation-test;";
        MatchQuery MQ = qb.parse(queryString);
        AtomicQuery query = new AtomicQuery(MQ, graph);

        Atom atom = query.getAtom();
        Map<RoleType, Pair<String, Type>> rmap = atom.getRoleVarTypeMap();

        Set<String> vars = atom.getVarNames();

        String relTypeId = atom.getTypeId();
        RelationType relType = graph.getRelationType(relTypeId);
        Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

        Set<Map<String, String>> roleMaps = new HashSet<>();
        computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

        Collection<Relation> rels = new LinkedList<>();
        roleMaps.forEach( map -> rels.add(new Relation(relTypeId, map, null)));
    }

    @Test
    public void testValuePredicateComparison(){
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Atomic atom = AtomicFactory.create(qb.parsePatterns("$x value '0';").iterator().next().admin());
        Atomic atom2 = AtomicFactory.create(qb.parsePatterns("$x value != '0';").iterator().next().admin());
        assertTrue(!atom.isEquivalent(atom2));
    }
}
