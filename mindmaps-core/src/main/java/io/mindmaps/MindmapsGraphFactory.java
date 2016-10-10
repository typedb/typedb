package io.mindmaps;

/**
 * A factory instance which produces graphs bound to the same persistence layer and keyspace.
 */
public interface MindmapsGraphFactory {
    /**
     *
     * @return A new or existing mindmaps graph
     */
    MindmapsGraph getGraph();

    /**
     *
     * @return A new or existing mindmaps graph with batch loading enabled
     */
    MindmapsGraph getGraphBatchLoading();

    /**
     *
     * @return A new or existing mindmaps graph computer
     */
    MindmapsComputer getGraphComputer();
}
