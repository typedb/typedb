package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

import static ai.grakn.graql.internal.util.StringConverter.idToString;
import static ai.grakn.util.Schema.EdgeLabel.SHORTCUT;
import static ai.grakn.util.Schema.EdgeProperty.FROM_ROLE;
import static ai.grakn.util.Schema.EdgeProperty.RELATION_TYPE_ID;
import static ai.grakn.util.Schema.EdgeProperty.TO_ROLE;

class ShortcutFragment extends AbstractFragment {

    private final Optional<String> relationType;
    private final Optional<String> roleStart;
    private final Optional<String> roleEnd;

    ShortcutFragment(
            Optional<String> relationType, Optional<String> roleStart, Optional<String> roleEnd,
            String start, String end
    ) {
        super(start, end);
        this.relationType = relationType;
        this.roleStart = roleStart;
        this.roleEnd = roleEnd;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.outE(SHORTCUT.getLabel());
        roleStart.ifPresent(rs -> edgeTraversal.has(FROM_ROLE.name(), rs));
        roleEnd.ifPresent(re -> edgeTraversal.has(TO_ROLE.name(), re));
        relationType.ifPresent(rt -> edgeTraversal.has(RELATION_TYPE_ID.name(), rt));
        edgeTraversal.inV();
    }

    @Override
    public String getName() {
        String start = roleStart.map(rs -> idToString(rs) + " ").orElse("");
        String type = relationType.map(rt -> ":" + idToString(rt)).orElse("");
        String end = roleEnd.map(re -> " " + idToString(re)).orElse("");
        return "-[" + start + "shortcut" + type + end + "]->";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_RELATION;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost * NUM_SHORTCUT_EDGES_PER_INSTANCE;
    }
}
