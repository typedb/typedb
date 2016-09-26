package io.mindmaps.graph.internal;

import io.mindmaps.factory.MindmapsClient;

import java.util.HashMap;
import java.util.Map;

public class Mindmaps {
    private static final String DEFAULT_ENGINE_URI = "localhost:4567";
    private static final Map<String, MindmapsClient> clients = new HashMap<>();

    /**
     *
     * @param uri The engine uri to connect to
     * @return A mindmaps client instance which can talk to the engine at the specified uri
     */
    public static MindmapsClient connect(String uri){
        return clients.computeIfAbsent(uri, MindmapsClient::new);
    }

    /**
     *
     * @return A mindmaps client instance which can talk a local instance of engine
     */
    public static MindmapsClient connect(){
        return clients.computeIfAbsent(DEFAULT_ENGINE_URI, MindmapsClient::new);
    }
}
