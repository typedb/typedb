package io.mindmaps;

public interface MindmapsGraphFactory {
    /**
     *
     * @param name The desired name for the mindmaps graph
     * @return A new or existing mindmaps graph with the defined name
     */
    MindmapsGraph getGraph(String name);

    /**
     *
     * @param name The desired name for the mindmaps graph
     * @return A new or existing mindmaps graph with the defined name connecting to the specified remote uri with batch loading enabled
     */
    MindmapsGraph getGraphBatchLoading(String name);

    /**
     *
     * @return A new or existing mindmaps graph compute with the defined name
     */
    MindmapsComputer getGraphComputer(String name);
}
