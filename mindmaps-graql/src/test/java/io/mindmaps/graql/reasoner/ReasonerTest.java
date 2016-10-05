/*
 * MindmapsDB  A Distributed Semantic Database
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
import io.mindmaps.concept.Rule;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.graql.internal.reasoner.rule.InferenceRule;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.mindmaps.graql.internal.reasoner.Utility.createReflexiveRule;
import static io.mindmaps.graql.internal.reasoner.Utility.createSubPropertyRule;
import static io.mindmaps.graql.internal.reasoner.Utility.createTransitiveRule;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReasonerTest {

    @Test
    public void testSubPropertyRule() {
        MindmapsGraph graph = SNBGraph.getGraph();

        Map<String, String> roleMap = new HashMap<>();
        RelationType parent = graph.getRelationType("sublocate");
        RelationType child = graph.getRelationType("resides");

        roleMap.put(graph.getRoleType("member-location").getId(), graph.getRoleType("subject-location").getId());
        roleMap.put(graph.getRoleType("container-location").getId(), graph.getRoleType("located-subject").getId());

        String body = "match (subject-location: $x, located-subject: $x1) isa resides;";
        String head = "match (member-location: $x, container-location: $x1) isa sublocate;";

        InferenceRule R2 = new InferenceRule(graph.putRule("test", body, head, graph.getMetaRuleInference()), graph);

        Rule rule = createSubPropertyRule("testRule", parent , child, roleMap, graph);
        InferenceRule R = new InferenceRule(rule, graph);

        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testTransitiveRule() {
        MindmapsGraph graph = SNBGraph.getGraph();

        Rule rule = createTransitiveRule("testRule", graph.getRelationType("sublocate"),
                graph.getRoleType("member-location").getId(), graph.getRoleType("container-location").getId(), graph);

        InferenceRule R = new InferenceRule(rule, graph);

        String body = "match (member-location: $x, container-location: $z) isa sublocate;" +
                      "(member-location: $z, container-location: $y) isa sublocate;" +
                      "select $x, $y;";
        String head = "match (member-location: $x, container-location: $y) isa sublocate;";

        InferenceRule R2 = new InferenceRule(graph.putRule("test", body, head, graph.getMetaRuleInference()), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testReflexiveRule() {
        MindmapsGraph graph = SNBGraph.getGraph();
        Rule rule = createReflexiveRule("testRule", graph.getRelationType("knows"), graph);
        InferenceRule R = new InferenceRule(rule, graph);

        String body = "match ($x, $y) isa knows;select $x;";
        String head = "match ($x, $x) isa knows;";

        InferenceRule R2 = new InferenceRule(graph.putRule("test", body, head, graph.getMetaRuleInference()), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }

    @Test
    public void testIdComma(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa Person, id 'Bob';";
        Query query = new Query(queryString, graph);
        assertTrue(query.getAtoms().size() == 2);
    }

    @Test
    public void testComma(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has firstname 'Bob', id 'Bob', value 'Bob', has age <21;";
        String queryString2 = "match $x isa person; $x has firstname 'Bob';$x id 'Bob';$x value 'Bob';$x has age <21;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    //TODO create ValuePredicate instead being a part of Substitution
    @Ignore
    public void testComma2(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, value <21 value >18;";
        String queryString2 = "match $x isa person;$x value <21;$x value >18;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x isa person, has firstname $y;";
        String queryString2 = "match $x isa person;$x has firstname $y;";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        assertTrue(query.equals(query2));
    }

    @Test
    public void testResourceAsVar2() {
        MindmapsGraph graph = SNBGraph.getGraph();
        String queryString = "match $x has firstname $y;";
        AtomicQuery query = new AtomicQuery(queryString, graph);
        assertTrue(query.getSelectedNames().size() == 2);
    }

    @Test
    public void testVarContraction(){
        MindmapsGraph graph = SNBGraph.getGraph();
        createReflexiveRule("testRule", graph.getRelationType("knows"), graph);
        String queryString = "match ($x, $y) isa knows;select $y;";
        String explicitQuery = "match $y isa person;$y id 'Bob' or $y id 'Charlie';";
        Query query = new Query(queryString, graph);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
    }

    @Test
    @Ignore
    //propagated sub [x/Bob] prevents from capturing the right inference
    public void testVarContraction2(){
        MindmapsGraph graph = SNBGraph.getGraph();
        createReflexiveRule("testRule", graph.getRelationType("knows"), graph);
        String queryString = "match ($x, $y) isa knows;$x id 'Bob';select $y;";
        String explicitQuery = "match $y isa person;$y id 'Bob' or $y id 'Charlie';";
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        assertEquals(reasoner.resolve(new Query(queryString, graph)), Sets.newHashSet(qb.parseMatch(explicitQuery)));
    }

    @Test
    @Ignore
    //Bug with unification, perhaps should unify select vars not atom vars
    public void testVarContraction3(){
        MindmapsGraph graph = SNBGraph.getGraph();
        String body = "match $x isa person;";
        String head = "match ($x, $x) isa knows;";
        graph.putRule("test", body, head, graph.getMetaRuleInference());

        String queryString = "match ($x, $y) isa knows;$x id 'Bob';select $y;";
        String explicitQuery = "match $y isa person;$y id 'Bob' or $y id 'Charlie';";
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);
        assertEquals(reasoner.resolve(new Query(queryString, graph)), Sets.newHashSet(qb.parseMatch(explicitQuery)));
    }

}
