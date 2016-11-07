package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.graql.internal.util.StringConverter.idToString;
import static io.mindmaps.util.Schema.BaseType.CASTING;
import static io.mindmaps.util.Schema.ConceptProperty.ITEM_IDENTIFIER;

class IdFragment extends AbstractFragment {

    private final String id;

    IdFragment(String start, String id) {
        super(start);
        this.id = id;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        // Whenever looking up by ID, we have to confirm this is not a casting
        traversal.has(ITEM_IDENTIFIER.name(), id).not(__.hasLabel(CASTING.name()));
    }

    @Override
    public String getName() {
        return "[id:" + idToString(id) + "]";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.ID;
    }

    @Override
    public long indexCost() {
        return 2;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return 1;
    }
}
