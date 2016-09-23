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

package io.mindmaps.graql.reasoner;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.internal.reasoner.predicate.AtomicFactory;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.Relation;
import io.mindmaps.graql.reasoner.graphs.CWGraph;
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import io.mindmaps.util.ErrorMessage;
import javafx.util.Pair;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static io.mindmaps.graql.internal.reasoner.Utility.computeRoleCombinations;

public class AtomicTest {

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testNonVar(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        MindmapsGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        String atomString = "match $x isa person;";

        Query query = new Query(atomString, graph);
        Atomic atom = AtomicFactory.create(qb.parseMatch(atomString).admin().getPattern());
    }

    @Test
    public void testNonVa2r(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        MindmapsGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        String atomString = "match $x isa person;";

        Query query = new Query(atomString, graph);
        Atomic atom =  AtomicFactory.create(qb.parseMatch(atomString).admin().getPattern(), query);
    }

    @Test
    public void testParentMissing(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage());

        MindmapsGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        String recRelString = "match ($x, $y) isa resides;";

        Atomic recRel = AtomicFactory.create(qb.parseMatch(recRelString).admin().getPattern().getPatterns().iterator().next());

        assert(recRel.isRecursive());
    }

    @Test
    public void testRecursive(){
        MindmapsGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String recRelString = "match ($x, $y) isa resides;";
        String nrecRelString = "match ($x, $y) isa recommendation;";

        Atomic recRel = AtomicFactory
                .create(qb.parseMatch(recRelString).admin().getPattern().getPatterns().iterator().next()
                        , new Query(recRelString, graph));
        Atomic nrecRel = AtomicFactory
                .create(qb.parseMatch(nrecRelString).admin().getPattern().getPatterns().iterator().next()
                        , new Query(recRelString, graph));

        assert(recRel.isRecursive());
        assert(!nrecRel.isRecursive());
    }

    @Test
    public void testFactory(){
        MindmapsGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = Graql.withGraph(graph);
        String atomString = "match $x isa person;";
        String relString = "match ($x, $y) isa recommendation;";
        String subString = "match $x id 'Bob';";
        String resString = "match $x has gender 'male';";

        Atomic atom = AtomicFactory.create(qb.parseMatch(atomString).admin().getPattern().getPatterns().iterator().next());
        Atomic relation = AtomicFactory.create(qb.parseMatch(relString).admin().getPattern().getPatterns().iterator().next());
        Atomic sub = AtomicFactory.create(qb.parseMatch(subString).admin().getPattern().getPatterns().iterator().next());
        Atomic res = AtomicFactory.create(qb.parseMatch(resString).admin().getPattern().getPatterns().iterator().next());

        assert(atom.isUnary() && atom.isType());
        assert(relation.isRelation());
        assert(sub.isSubstitution());
        assert(res.isResource());
    }

    @Test
    public void testRecursiveAtom(){

    }

    @Test
    public void testRoleInference(){
        MindmapsGraph graph = CWGraph.getGraph();

        String queryString = "match isa owns, ($z, $y); $z isa country; $y isa weapon; select $y, $z";
        Query query = new Query(queryString, graph);
        Atomic atom = query.getAtomsWithType(graph.getType("owns")).iterator().next();

        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        queryString = "match isa owns, ($z, $y); $z isa country; select $y, $z";
        query = new Query(queryString, graph);
        atom = query.getAtomsWithType(graph.getType("owns")).iterator().next();

        Map<RoleType, Pair<String, Type>> roleMap2 = atom.getRoleVarTypeMap();

        assert(roleMap.size() == 2 && roleMap2.size() == 2);
    }

    @Test
    public void testRoleInference2(){
        MindmapsGraph graph = CWGraph.getGraph();

        String queryString = "match ($z, $y, $x), isa transaction;$z isa country;$x isa person; select $x, $y, $z";
        Query query = new Query(queryString, graph);
        Atomic atom = query.getAtomsWithType(graph.getType("transaction")).iterator().next();
        Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();

        queryString = "match ($z, $y, seller: $x), isa transaction;$z isa country;$y isa weapon; select $x, $y, $z";
        query = new Query(queryString, graph);
        atom = query.getAtomsWithType(graph.getType("transaction")).iterator().next();
        Map<RoleType, Pair<String, Type>> roleMap2 = atom.getRoleVarTypeMap();

        assert(roleMap.size() == 3 && roleMap2.size() == 3);
    }

    @Test
    public void testRelationConstructor(){
        MindmapsGraph graph = GenericGraph.getGraph("geo-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);

        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";

        MatchQuery MQ = qb.parseMatch(queryString);
        Query query = new Query(MQ, graph);

        Atomic atom = query.selectAtoms().iterator().next();
        Set<String> vars = atom.getVarNames();

        String relTypeId = atom.getTypeId();
        RelationType relType = graph.getRelationType(relTypeId);
        Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

        Set<Map<String, String>> roleMaps = new HashSet<>();
        computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

        Collection<Relation> rels = new LinkedList<>();
        roleMaps.forEach( map -> rels.add(new Relation(relTypeId, map)));
    }

    @Test
    public void testRelationConstructor2(){
        MindmapsGraph graph = GenericGraph.getGraph("geo-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);

        String queryString = "match ($x, $y, $z) isa ternary-relation-test;";

        MatchQuery MQ = qb.parseMatch(queryString);
        Query query = new Query(MQ, graph);

        Atomic atom = query.selectAtoms().iterator().next();
        Map<RoleType, Pair<String, Type>> rmap = atom.getRoleVarTypeMap();

        Set<String> vars = atom.getVarNames();

        String relTypeId = atom.getTypeId();
        RelationType relType = graph.getRelationType(relTypeId);
        Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

        Set<Map<String, String>> roleMaps = new HashSet<>();
        computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

        Collection<Relation> rels = new LinkedList<>();
        roleMaps.forEach( map -> rels.add(new Relation(relTypeId, map)));
    }





}
