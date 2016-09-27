package io.mindmaps;

import io.mindmaps.factory.MindmapsGraphFactoryImpl;
import io.mindmaps.factory.MindmapsGraphFactoryInMemory;

import java.util.HashMap;
import java.util.Map;

public class Mindmaps {
    public static final String DEFAULT_URI = "localhost:4567";
    public static final String IN_MEMORY = "in-memory";
    private static final Map<String, MindmapsGraphFactory> clients = new HashMap<>();

    /**
     *
     * @param location The location from which to create the graph
     * @return A mindmaps client instance which can talk to the engine at the specified uri
     */
    public static MindmapsGraphFactory factory(String location){
        if(IN_MEMORY.equals(location)){
            return clients.computeIfAbsent(location, (key) -> MindmapsGraphFactoryInMemory.getInstance());
        }
        return clients.computeIfAbsent(location, MindmapsGraphFactoryImpl::new);
    }
}
