/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Grakn is the main entry point to connect to a Grakn Knowledge Graph.
 * <p>
 * To connect to a knowledge graph, first make sure you have a Grakn Engine server running. You can also instantiate
 * an in-memory knowledge graph for testing or experimentation purposes.
 * </p>
 * <p>
 * To establish a connection, you first need to obtain a {@link GraknGraphFactory} by calling
 * the {@link #factory(String, String)} method. A {@link GraknGraphFactory} to a given physical
 * location and specific database instance within that location. Once you've instantiated a factory, 
 * you can obtain multiple concurrent graph connections, represented by the {@link GraknGraph}
 * interface.
 * </p>
 *
 * @author Filipe Teixeira
 */
public class Grakn {
    /**
     * Constant to be passed to {@link #factory(String, String)} to specify the default localhost Grakn Engine location.
     */
    public static final String DEFAULT_URI = "localhost:4567";

    private static final String GRAIN_GRAPH_FACTORY_IMPLEMENTATION = "ai.grakn.factory.GraknGraphFactoryImpl";

    /**
     * Constant to be passed to {@link #factory(String, String)} to specify an in-memory graph.
     */
    public static final String IN_MEMORY = "in-memory";

    private static final Map<String, GraknGraphFactory> clients = new HashMap<>();

    private static <F extends GraknGraphFactory> F loadImplementation(String className,
                                                                      String location,
                                                                      String keyspace) {
        try {
            @SuppressWarnings("unchecked")
            Class<F> cl = (Class<F>)Class.forName(className);
            return cl.getConstructor(String.class, String.class).newInstance(keyspace, location);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException | NoSuchMethodException
                | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Returns a factory instances to produce concurrent connections to the Grakn knowledge graph.
     * <p>
     * This method obtains the {@link GraknGraphFactory} for the specified location and keyspace.
     * </p>
     * 
     * @param location The location from which to create the graph. For an in-memory graph,
     * use the {@link #IN_MEMORY} constant defined in this class. For the default, localhost
     * Grakn Engine location, use the {@link #DEFAULT_URI} constant provided in this class.
     * @param keyspace The keyspace, or database name, where the knowledge graph is stored. A given
     * database server will support multiple database instances. You need to explicitly name
     * the instance to be used. In general, if one doesn't exist, it will be created for you. 
     * @return A factory instance that can produce concurrent connection to the knowledge graph.
     */
    public static GraknGraphFactory factory(String location, String keyspace) {
        String finalKeyspace = keyspace.toLowerCase(Locale.getDefault());
        String key = location + finalKeyspace;
        return clients.computeIfAbsent(key, (k) -> loadImplementation(GRAIN_GRAPH_FACTORY_IMPLEMENTATION, location, finalKeyspace));
    }
}