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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.graql.internal.reasoner.rule.InferenceRule;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.mindmaps.graql.Reasoner.createSubPropertyRule;
import static io.mindmaps.graql.Reasoner.createTransitiveRule;
import static org.junit.Assert.assertTrue;

public class ReasonerTest {

    @Test
    public void testSubPropertyRule() {
        MindmapsGraph graph = SNBGraph.getGraph();

        Map<RoleType, RoleType> roleMap = new HashMap<>();
        RelationType parent = graph.getRelationType("sublocate");
        RelationType child = graph.getRelationType("resides");

        roleMap.put(graph.getRoleType("member-location"), graph.getRoleType("subject-location"));
        roleMap.put(graph.getRoleType("container-location"), graph.getRoleType("located-subject"));

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
                graph.getRoleType("member-location"), graph.getRoleType("container-location"), graph);
        InferenceRule R = new InferenceRule(rule, graph);

        String body = "match (member-location: $x, container-location: $z) isa sublocate;" +
                      "(member-location: $z, container-location: $y) isa sublocate;" +
                      "select $x, $y;";
        String head = "match (member-location: $x, container-location: $y) isa sublocate;";

        InferenceRule R2 = new InferenceRule(graph.putRule("test", body, head, graph.getMetaRuleInference()), graph);
        assertTrue(R.getHead().equals(R2.getHead()));
        assertTrue(R.getBody().equals(R2.getBody()));
    }
}
