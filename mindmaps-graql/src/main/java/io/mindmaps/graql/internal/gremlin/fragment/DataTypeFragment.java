package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.util.Schema.ConceptProperty.DATA_TYPE;

class DataTypeFragment extends AbstractFragment {

    private final ResourceType.DataType dataType;

    DataTypeFragment(String start, ResourceType.DataType dataType) {
        super(start);
        this.dataType = dataType;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.has(DATA_TYPE.name(), dataType.getName());
    }

    @Override
    public String getName() {
        return "[datatype:" + dataType.getName() + "]";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.VALUE_NONSPECIFIC;
    }
}
