package ai.grakn;

/**
 * A factory instance which produces graphs bound to the same persistence layer and keyspace.
 */
public interface GraknGraphFactory {
    /**
     *
     * @return A new or existing grakn graph
     */
    GraknGraph getGraph();

    /**
     *
     * @return A new or existing grakn graph with batch loading enabled
     */
    GraknGraph getGraphBatchLoading();

    /**
     *
     * @return A new or existing grakn graph computer
     */
    GraknComputer getGraphComputer();
}
