package ai.grakn.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graql.analytics.MinQuery;

public interface ComputeQueryBuilder {

    ComputeQueryBuilder withGraph(GraknGraph graph);

    MinQuery min();
}
