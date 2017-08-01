package ai.grakn.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graql.internal.query.ComputeQueryBuilderImplMock;
import ai.grakn.graql.internal.query.QueryBuilderImpl;

import java.util.Optional;

/**
 *
 */
public class QueryBuilderImplMock extends QueryBuilderImpl {
    private int numWorkers;
    private Optional<GraknGraph> graph;

    QueryBuilderImplMock() {
        super();
    }

    public QueryBuilderImplMock(GraknGraph graph) {
        super(graph);
    }

    public QueryBuilderImplMock(GraknGraph graph, int numWorkers) {
        super(graph);
        this.graph = Optional.of(graph);
        this.numWorkers = numWorkers;
    }

    /**
     * @return a compute query builder for building analytics query
     */
    @Override
    public ComputeQueryBuilder compute(){
        return new ComputeQueryBuilderImplMock(graph, numWorkers);
    }
}
