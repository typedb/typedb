/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn;

import ai.grakn.util.SimpleURI;

import javax.annotation.CheckReturnValue;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//Docs to do @Filipe
/*
    Please can you revisit the documentation above and add any clarification  you think necessary about
    WHY you would want a toy graph (e.g. what would you be doing or not doing to use it)?
    This section of documentation is probably going to be the first place some visitors reach
    if they are classic Java developers and reach for javadocs rather than tutorials on our portal
    Please can you have a think to see if there is anything else this page, the intro, needs?
*/

/**
 <p>
 Grakn is the main entry point to connect to a Grakn Knowledge Base.

 To connect to a knowledge graph, first make sure you have a Grakn Engine server running by starting it from the shell using:
 <pre>{@code grakn server start}</pre>

 To establish a connection, you first need to obtain a {@link GraknSession} by calling
 the {@link #session(String, String)} method. A {@link GraknSession} connects to a given physical
 location and specific database instance within that location.

 Once you've instantiated a session, you can obtain multiple concurrent graph connections,
 represented by the {@link GraknTx} interface.

 If you are running the Grakn server locally then you can initialise a graph with:

 <pre>{@code GraknTx graph = Grakn.session(Grakn.DEFAULT_URI, "keyspace").getGraph();}</pre>
 If you are running the Grakn server remotely you must initialise the graph by providing the IP address of your server:

 <pre>{@code GraknTx graph = Grakn.session("127.6.21.2", "keyspace").getGraph();}</pre>
 The string “keyspace” uniquely identifies the graph and allows you to create different graphs.

 Please note that graph keyspaces are not case sensitive so the following two graphs are actually the same graph:

 <pre>{@code GraknTx graph1 = Grakn.session("127.6.21.2", "keyspace").getGraph();
 GraknTx graph2 = Grakn.session("127.6.21.2", "KeYsPaCe").getGraph();}</pre>
 All graphs are also singletons specific to their keyspaces so be aware that in the following case:

 <pre>{@code GraknTx graph1 = Grakn.session("127.6.21.2", "keyspace").getGraph();
 GraknTx graph2 = Grakn.session("127.6.21.2", "keyspace").getGraph();
 GraknTx graph3 = Grakn.session("127.6.21.2", "keyspace").getGraph();}</pre>

 any changes to <code>graph1</code>, <code>graph2</code>, or <code>graph3</code> will all be persisted to the same graph.

 You can alternatively instantiate a 'toy' knowledge graph (which runs in-memory) for experimentation purposes.
 You can initialise an in memory graph without having the Grakn server running:

 <pre>{@code GraknTx graph = Grakn.session(Grakn.IN_MEMORY, "keyspace").getGraph();}</pre>
 This in memory graph serves as a toy graph for you to become accustomed to the API without needing to setup a
 Grakn Server. It is also useful for testing purposes.
 </p>

 @see <a href="https://grakn.ai/pages/documentation/developing-with-java/java-setup.html" target="_top">GRAKN.AI Portal documentation for Java developers</a>
 @author Filipe Teixeira
 */


public class Grakn {

    private Grakn(){}

    /**
     * Constant to be passed to {@link #session(String, String)} to specify the default localhost Grakn Engine location.
     * This default constant, which is set to localhost: 4567 cannot be changed in development"
     */
    public static final SimpleURI DEFAULT_URI = new SimpleURI("localhost", 4567);

    private static final String SESSION_CLASS = "ai.grakn.factory.EmbeddedGraknSession";

    private static final String SESSION_BUILDER = "create";

    /**
     * Constant to be passed to {@link #session(String, String)} to specify an in-memory graph.
     */
    public static final String IN_MEMORY = "in-memory";

    private static final Map<String, GraknSession> clients = new ConcurrentHashMap<>();

    private static <F extends GraknSession> F loadImplementation(String className,
                                                                 String location,
                                                                 Keyspace keyspace) {
        try {
            @SuppressWarnings("unchecked")
            Class cl = Class.forName(className);
            return (F) cl.getMethod(SESSION_BUILDER, Keyspace.class, String.class).invoke(null, keyspace, location);
        } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a session instance to produce concurrent connections to the Grakn knowledge graph.
     * <p>
     * This method obtains the {@link GraknSession} for the specified location and keyspace.
     * </p>
     *
     * @param location The location from which to create the graph.
     * For the default, localhost Grakn Engine location, use the {@link #DEFAULT_URI} constant provided in this class.
     * For testing or experimentation, you can use a toy in-memory graph be specifying the {@link #IN_MEMORY} constant.
     *
     * @param keyspace The keyspace, or database name, where the knowledge graph is stored. A given
     * database server will support multiple database instances. You need to explicitly name
     * the instance to be used. Please note that graph keyspaces are not case sensitive.
     * If one doesn't exist, it will be created for you.
     * @return A session instance that can produce concurrent connection to the specified knowledge graph.
     */
    @CheckReturnValue
    public static GraknSession session(String location, String keyspace) {
        return session(location, Keyspace.of(keyspace));
    }

    @CheckReturnValue
    public static GraknSession session(SimpleURI location, String keyspace) {
        return session(location.toString(), Keyspace.of(keyspace));
    }

    @CheckReturnValue
    public static GraknSession session(SimpleURI location, Keyspace keyspace) {
        return session(location.toString(), keyspace);
    }

    @CheckReturnValue
    public static GraknSession session(String location, Keyspace keyspace) {
        String key = location + keyspace.getValue();
        return clients.computeIfAbsent(key, k -> loadImplementation(SESSION_CLASS, location, keyspace));
    }
}