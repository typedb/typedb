package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class ValueFragment extends AbstractFragment {

    private final ValuePredicateAdmin predicate;

    ValueFragment(String start, ValuePredicateAdmin predicate) {
        super(start);
        this.predicate = predicate;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        Schema.ConceptProperty value = getValueProperty();
        traversal.has(value.name(), predicate.getPredicate());
    }

    @Override
    public String getName() {
        return "[value:" + predicate + "]";
    }

    @Override
    public FragmentPriority getPriority() {
        if (predicate.isSpecific()) {
            return FragmentPriority.VALUE_SPECIFIC;
        } else {
            return FragmentPriority.VALUE_NONSPECIFIC;
        }
    }

    /**
     * @return the correct VALUE property to check on the vertex for the given predicate
     */
    private Schema.ConceptProperty getValueProperty() {
        Object value = predicate.getInnerValues().iterator().next();
        return ResourceType.DataType.SUPPORTED_TYPES.get(value.getClass().getTypeName()).getConceptProperty();
    }
}
