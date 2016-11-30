package ai.grakn.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graql.analytics.*;

public interface ComputeQueryBuilder {

    ComputeQueryBuilder withGraph(GraknGraph graph);

    CountQuery count();

    MinQuery min();

    MaxQuery max();

    SumQuery sum();

    MeanQuery mean();

    StdQuery std();

    MedianQuery median();

    PathQuery path();
}
