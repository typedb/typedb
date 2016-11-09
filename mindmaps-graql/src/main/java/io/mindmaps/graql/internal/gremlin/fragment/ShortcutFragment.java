package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

import static io.mindmaps.graql.internal.util.StringConverter.idToString;
import static io.mindmaps.util.Schema.EdgeLabel.SHORTCUT;
import static io.mindmaps.util.Schema.EdgeProperty.FROM_ROLE;
import static io.mindmaps.util.Schema.EdgeProperty.RELATION_TYPE_ID;
import static io.mindmaps.util.Schema.EdgeProperty.TO_ROLE;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ShortcutFragment that = (ShortcutFragment) o;

        if (relationType != null ? !relationType.equals(that.relationType) : that.relationType != null) return false;
        if (roleStart != null ? !roleStart.equals(that.roleStart) : that.roleStart != null) return false;
        return roleEnd != null ? roleEnd.equals(that.roleEnd) : that.roleEnd == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (relationType != null ? relationType.hashCode() : 0);
        result = 31 * result + (roleStart != null ? roleStart.hashCode() : 0);
        result = 31 * result + (roleEnd != null ? roleEnd.hashCode() : 0);
        return result;
    }
}
