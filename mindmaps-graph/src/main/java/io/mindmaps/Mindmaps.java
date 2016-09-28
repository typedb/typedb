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
     * @param keyspace THe keyspace of the factory to be bound to
     * @return A mindmaps client instance which can talk to the engine at the specified uri
     */
    public static MindmapsGraphFactory factory(String location, String keyspace){
        String key = location + keyspace;
        if(IN_MEMORY.equals(location)){
            return clients.computeIfAbsent(key, (k) -> new MindmapsGraphFactoryInMemory(keyspace));
        }
        return clients.computeIfAbsent(key, (k) -> new MindmapsGraphFactoryImpl(keyspace, location));
    }
}
