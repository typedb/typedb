package io.mindmaps.factory;

import io.mindmaps.core.dao.MindmapsGraph;

public interface MindmapsGraphFactory {
    /**
     *
     * @param name The name of the graph we should be initialising
     * @param address The address of where the backend is. Defaults to localhost if null
     * @param pathToConfig Path to file storing optional configuration parameters. Uses defaults if left null
     * @return An instance of Mindmaps graph with a new transaction
     */
    MindmapsGraph getGraph(String name, String address, String pathToConfig);
}
