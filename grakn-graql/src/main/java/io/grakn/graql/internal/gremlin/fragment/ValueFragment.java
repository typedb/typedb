package io.grakn.graql.internal.gremlin.fragment;

import io.grakn.concept.ResourceType;
import io.grakn.graql.admin.ValuePredicateAdmin;
import io.grakn.graql.internal.gremlin.FragmentPriority;
import io.grakn.util.Schema;
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

    @Override
    public long indexCost() {
        if (predicate.isSpecific()) {
            return 1;
        } else {
            return super.indexCost();
        }
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost;
    }

    /**
     * @return the correct VALUE property to check on the vertex for the given predicate
     */
    private Schema.ConceptProperty getValueProperty() {
        Object value = predicate.getInnerValues().iterator().next();
        return ResourceType.DataType.SUPPORTED_TYPES.get(value.getClass().getTypeName()).getConceptProperty();
    }
}
