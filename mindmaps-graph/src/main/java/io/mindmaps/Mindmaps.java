package io.mindmaps;

import io.mindmaps.factory.MindmapsGraphFactoryImpl;

import java.util.HashMap;
import java.util.Map;

public class Mindmaps {
    private static final String DEFAULT_ENGINE_URI = "localhost:4567";
    private static final Map<String, MindmapsGraphFactoryImpl> clients = new HashMap<>();

    /**
     *
     * @param uri The engine uri to factory to
     * @return A mindmaps client instance which can talk to the engine at the specified uri
     */
    public static MindmapsGraphFactory factory(String uri){
        return clients.computeIfAbsent(uri, MindmapsGraphFactoryImpl::new);
    }

    /**
     *
     * @return A mindmaps client instance which can talk a local instance of engine
     */
    public static MindmapsGraphFactory factory(){
        return clients.computeIfAbsent(DEFAULT_ENGINE_URI, MindmapsGraphFactoryImpl::new);
    }
}
