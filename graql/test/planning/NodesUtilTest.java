/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.planning;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.planning.gremlin.fragment.Fragments;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.spanningtree.graph.EdgeNode;
import grakn.core.kb.graql.planning.spanningtree.graph.InstanceNode;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import graql.lang.statement.Variable;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.planning.NodesUtil.propagateLabels;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class NodesUtilTest {

    @Test
    public void testPropagateLabelsOverInIsa() {
        Map<Node, Set<Node>> mockParentToChildQPGraph = new HashMap<>();
        Fragment labelFragment = Fragments.label(null, new Variable("typeVar"), ImmutableSet.of(Label.of("someLabel")));
        Fragment inIsaFragment = Fragments.inIsa(null, new Variable("typeVar"), new Variable("instanceVar"), true);

        Node labelNode = labelFragment.getNodes().iterator().next();
        labelNode.getFragmentsWithoutDependency().add(labelFragment);
        Set<Node> inIsaNodes = inIsaFragment.getNodes();
        Node inIsaMiddleNode = inIsaNodes.stream()
                .filter(node -> node instanceof EdgeNode)
                .findAny()
                .orElse(null);

        assertNotNull(inIsaMiddleNode);

        InstanceNode instanceVarNode = (InstanceNode) inIsaNodes.stream()
                .filter(node -> node instanceof InstanceNode && node.getNodeId().toString().contains("instanceVar"))
                .findAny()
                .orElse(null);

        assertNotNull(instanceVarNode);

        mockParentToChildQPGraph.put(labelNode, Collections.singleton(inIsaMiddleNode));
        mockParentToChildQPGraph.put(inIsaMiddleNode, Collections.singleton(instanceVarNode));

        propagateLabels(mockParentToChildQPGraph);

        assertEquals(Label.of("someLabel"), instanceVarNode.getInstanceLabel());
    }

    @Test
    public void testPropagateLabelsOverOutIsa() {
        // It's extremely unlikely that the OutIsa will ever be taken
        // so this test should only be here for completeness.
        // because the OutIsa code path is so rare, only have about 90% confidence that that results in
        // the label being the child of the middle edge node which is the child of the instance at the QP graph level

        Map<Node, Set<Node>> mockParentToChildQPGraph = new HashMap<>();
        Fragment labelFragment = Fragments.label(null, new Variable("typeVar"), ImmutableSet.of(Label.of("someLabel")));
        Fragment outIsaFragment = Fragments.outIsa(null, new Variable("instanceVar"), new Variable("typeVar"));

        Node labelNode = labelFragment.getNodes().iterator().next();
        labelNode.getFragmentsWithoutDependency().add(labelFragment);
        Set<Node> outIsaNodes = outIsaFragment.getNodes();
        Node outIsaMiddleNode = outIsaNodes.stream()
                .filter(node -> node instanceof EdgeNode)
                .findAny()
                .orElse(null);

        assertNotNull(outIsaMiddleNode);

        InstanceNode instanceVarNode = (InstanceNode) outIsaNodes.stream()
                .filter(node -> node instanceof InstanceNode && node.getNodeId().toString().contains("instanceVar"))
                .findAny()
                .orElse(null);

        assertNotNull(instanceVarNode);

        mockParentToChildQPGraph.put(instanceVarNode, Collections.singleton(outIsaMiddleNode));
        mockParentToChildQPGraph.put(outIsaMiddleNode, Collections.singleton(labelNode));

        propagateLabels(mockParentToChildQPGraph);

        assertEquals(Label.of("someLabel"), instanceVarNode.getInstanceLabel());
    }
}
