package ai.grakn.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graql.internal.query.MockComputeQueryBuilderImpl;
import ai.grakn.graql.internal.query.Queries;

import java.util.Optional;

/**
 *
 */
public class MockQueryBuilderImpl extends QueryBuilderImpl {
    private int numWorkers;
    private Optional<GraknGraph> graph;

    MockQueryBuilderImpl() {
        super();
    }

    public MockQueryBuilderImpl(GraknGraph graph) {
        super(graph);
    }

    public MockQueryBuilderImpl(GraknGraph graph, int numWorkers) {
        super(graph);
        this.graph = Optional.of(graph);
        this.numWorkers = numWorkers;
    }

    /**
     * @return a compute query builder for building analytics query
     */
    @Override
    public ComputeQueryBuilder compute(){
        return new MockComputeQueryBuilderImpl(graph, numWorkers);
    }
}
