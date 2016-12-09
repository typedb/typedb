package ai.grakn.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graql.analytics.*;

import java.util.Map;
import java.util.Set;

public interface ComputeQueryBuilder {

    /**
     * @param graph the graph to execute the compute query on
     * @return a compute query builder with the graph set
     */
    ComputeQueryBuilder withGraph(GraknGraph graph);

    /**
     * @return a count query that will count the number of instances
     */
    CountQuery count();

    /**
     * @return a min query that will find the min value of the given resource types
     */
    MinQuery min();

    /**
     * @return a max query that will find the max value of the given resource types
     */
    MaxQuery max();

    /**
     * @return a sum query that will compute the sum of values of the given resource types
     */
    SumQuery sum();

    /**
     * @return a mean query that will compute the mean of values of the given resource types
     */
    MeanQuery mean();

    /**
     * @return a std query that will compute the standard deviation of values of the given resource types
     */
    StdQuery std();

    /**
     * @return a median query that will compute the median of values of the given resource types
     */
    MedianQuery median();

    /**
     * @return a path query that will find the shortest path between two instances
     */
    PathQuery path();

    /**
     * @return a cluster query that will find the clusters in the graph
     */
    ClusterQuery<Map<String, Long>> cluster();

    /**
     * @return a degree query that will compute the degree of instances
     */
    DegreeQuery<Map<Long, Set<String>>> degree();
}
