package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.graql.internal.util.StringConverter.valueToString;
import static io.mindmaps.util.Schema.ConceptProperty.REGEX;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RegexFragment that = (RegexFragment) o;

        return regex != null ? regex.equals(that.regex) : that.regex == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (regex != null ? regex.hashCode() : 0);
        return result;
    }
}
