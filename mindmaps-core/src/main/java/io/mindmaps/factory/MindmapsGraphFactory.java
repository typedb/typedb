package io.mindmaps.factory;

import io.mindmaps.core.dao.MindmapsGraph;

public interface MindmapsGraphFactory {
    /**
     *
     * @param config Optional configurations for instantiating the graph.
     * @return An instance of Mindmaps graph with a new transaction
     */
    MindmapsGraph newGraph(String... config);
}
