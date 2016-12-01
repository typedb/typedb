package ai.grakn.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graql.analytics.*;

import java.util.Map;
import java.util.Set;

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

    ClusterQuery<Map<String, Long>> cluster();

    DegreeQuery<Map<Long, Set<String>>> degree();
}
