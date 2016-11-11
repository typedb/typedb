package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.concept.ResourceType;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.internal.gremlin.FragmentPriority;
import ai.grakn.util.Schema;
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
    public long fragmentCost(long previousCost) {
        if (predicate.isSpecific()) {
            return 1;
        } else {
            return previousCost;
        }
    }

    /**
     * @return the correct VALUE property to check on the vertex for the given predicate
     */
    private Schema.ConceptProperty getValueProperty() {
        Object value = predicate.getInnerValues().iterator().next();
        return ResourceType.DataType.SUPPORTED_TYPES.get(value.getClass().getTypeName()).getConceptProperty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ValueFragment that = (ValueFragment) o;

        return predicate != null ? predicate.equals(that.predicate) : that.predicate == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (predicate != null ? predicate.hashCode() : 0);
        return result;
    }
}
