package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.util.Schema.ConceptProperty.VALUE_BOOLEAN;
import static ai.grakn.util.Schema.ConceptProperty.VALUE_DOUBLE;
import static ai.grakn.util.Schema.ConceptProperty.VALUE_LONG;
import static ai.grakn.util.Schema.ConceptProperty.VALUE_STRING;

class ValueFlagFragment extends AbstractFragment {

    ValueFlagFragment(String start) {
        super(start);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.or(
                __.has(VALUE_STRING.name()),
                __.has(VALUE_LONG.name()),
                __.has(VALUE_DOUBLE.name()),
                __.has(VALUE_BOOLEAN.name())
        );
    }

    @Override
    public String getName() {
        return "[value]";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.VALUE_NONSPECIFIC;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost;
    }
}
