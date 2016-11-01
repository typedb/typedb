package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.graql.internal.util.StringConverter.valueToString;
import static ai.grakn.util.Schema.ConceptProperty.REGEX;

class RegexFragment extends AbstractFragment {

    private final String regex;

    RegexFragment(String start, String regex) {
        super(start);
        this.regex = regex;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.has(REGEX.name(), regex);
    }

    @Override
    public String getName() {
        return "[regex:" + valueToString(regex) + "]";
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
