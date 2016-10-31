package io.grakn;

/**
 * A factory instance which produces graphs bound to the same persistence layer and keyspace.
 */
public interface MindmapsGraphFactory {
    /**
     *
     * @return A new or existing grakn graph
     */
    MindmapsGraph getGraph();

    /**
     *
     * @return A new or existing grakn graph with batch loading enabled
     */
    MindmapsGraph getGraphBatchLoading();

    /**
     *
     * @return A new or existing grakn graph computer
     */
    MindmapsComputer getGraphComputer();
}
