package ai.grakn.graql.internal.query;

import ai.grakn.GraknGraph;
import ai.grakn.graql.ComputeQueryBuilder;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.query.analytics.ComputeQueryBuilderImpl;
import ai.grakn.graql.internal.query.analytics.CountQueryImplMock;
import ai.grakn.graql.internal.query.analytics.DegreeQueryImplMock;
import ai.grakn.graql.internal.query.analytics.MaxQueryImplMock;
import ai.grakn.graql.internal.query.analytics.MeanQueryImplMock;
import ai.grakn.graql.internal.query.analytics.MedianQueryImplMock;
import ai.grakn.graql.internal.query.analytics.MinQueryImplMock;
import ai.grakn.graql.internal.query.analytics.StdQueryImplMock;
import ai.grakn.graql.internal.query.analytics.SumQueryImplMock;

import java.util.Optional;

/**
 *
 */
public class ComputeQueryBuilderImplMock extends ComputeQueryBuilderImpl {
    private Optional<GraknGraph> graph;
    private int numWorkers;

    public ComputeQueryBuilderImplMock(Optional<GraknGraph> graph) {
        super(graph);
    }


    public ComputeQueryBuilderImplMock(Optional<GraknGraph> graph, int numWorkers) {
        super(graph);
        this.graph = graph;
        this.numWorkers = numWorkers;
    }

    @Override
    public ComputeQueryBuilder withGraph(GraknGraph graph) {
        this.graph = Optional.of(graph);
        return this;
    }

    @Override
    public CountQuery count() {
        return new CountQueryImplMock(graph, numWorkers);
    }

    @Override
    public DegreeQuery degree() {
        return new DegreeQueryImplMock(graph, numWorkers);
    }

    @Override
    public MinQuery min() {
        return new MinQueryImplMock(graph, numWorkers);
    }

    @Override
    public MaxQuery max() {
        return new MaxQueryImplMock(graph, numWorkers);
    }

    @Override
    public MeanQuery mean() {
        return new MeanQueryImplMock(graph, numWorkers);
    }

    @Override
    public SumQuery sum() {
        return new SumQueryImplMock(graph, numWorkers);
    }

    @Override
    public StdQuery std() {
        return new StdQueryImplMock(graph, numWorkers);
    }

    @Override
    public MedianQuery median() {
        return new MedianQueryImplMock(graph, numWorkers);
    }

}
