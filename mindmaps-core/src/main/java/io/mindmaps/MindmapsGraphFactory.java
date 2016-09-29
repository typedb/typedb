package io.mindmaps;

public interface MindmapsGraphFactory {
    /**
     *
     * @return A new or existing mindmaps graph with the defined keyspace
     */
    MindmapsGraph getGraph();

    /**
     *
     * @return A new or existing mindmaps graph with the defined keyspace connecting to the specified remote uri with batch loading enabled
     */
    MindmapsGraph getGraphBatchLoading();

    /**
     *
     * @return A new or existing mindmaps graph compute with the defined keyspace
     */
    MindmapsComputer getGraphComputer();
}
