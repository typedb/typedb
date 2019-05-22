package grakn.core.graql.gremlin;

import com.google.common.collect.ImmutableSet;
import grakn.core.concept.Label;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.fragment.Fragments;
import grakn.core.graql.gremlin.spanningtree.graph.EdgeNode;
import grakn.core.graql.gremlin.spanningtree.graph.InstanceNode;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import graql.lang.statement.Variable;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.gremlin.NodesUtil.propagateLabels;
import static junit.framework.TestCase.assertEquals;

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
                .orElseThrow(() -> new RuntimeException("Test error - no edge node found"));

        InstanceNode instanceVarNode = (InstanceNode) inIsaNodes.stream()
                .filter(node -> node instanceof InstanceNode && node.getNodeId().toString().contains("instanceVar"))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Test error - no instanceVar node found"));

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
                .orElseThrow(() -> new RuntimeException("Test error - no edge node found"));

        InstanceNode instanceVarNode = (InstanceNode) outIsaNodes.stream()
                .filter(node -> node instanceof InstanceNode && node.getNodeId().toString().contains("instanceVar"))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Test error - no instanceVar node found"));

        mockParentToChildQPGraph.put(instanceVarNode, Collections.singleton(outIsaMiddleNode));
        mockParentToChildQPGraph.put(outIsaMiddleNode, Collections.singleton(labelNode));

        propagateLabels(mockParentToChildQPGraph);

        assertEquals(Label.of("someLabel"), instanceVarNode.getInstanceLabel());
    }
}
