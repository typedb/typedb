package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.concept.ResourceType;
import ai.grakn.graql.internal.gremlin.FragmentPriority;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.util.Schema.ConceptProperty.DATA_TYPE;

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

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost / ResourceType.DataType.SUPPORTED_TYPES.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DataTypeFragment that = (DataTypeFragment) o;

        return dataType != null ? dataType.equals(that.dataType) : that.dataType == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (dataType != null ? dataType.hashCode() : 0);
        return result;
    }
}
