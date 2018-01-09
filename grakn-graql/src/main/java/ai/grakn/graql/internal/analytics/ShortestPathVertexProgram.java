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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.analytics;

import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.javatuples.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.Utility.getVertexId;

/**
 * The vertex program for computing the shortest path between two instances.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class ShortestPathVertexProgram extends GraknVertexProgram<Tuple> {

    private static final int MAX_ITERATION = 50;

    private static final int FROM_SOURCE = 1;
    private static final int FROM_DESTINATION = -1;
    private static final int FROM_MIDDLE = 0;

    // message keys
    private static final int DIRECTION = 0;
    private static final int ID = 1;

    private static final String PATH_HAS_MIDDLE_POINT = "shortestPathVertexProgram.pathHasMiddlePoint";

    private static final String PREDECESSOR = "shortestPathVertexProgram.fromVertex";
    private static final String VISITED_IN_ITERATION = "shortestPathVertexProgram.visitedInIteration";
    private static final String VOTE_TO_HALT_SOURCE = "shortestPathVertexProgram.voteToHalt.source";
    private static final String VOTE_TO_HALT_DESTINATION = "shortestPathVertexProgram.voteToHalt.destination";
    private static final String FOUND_PATH = "shortestPathVertexProgram.foundDestination";
    public static final String PREDECESSORS_FROM_SOURCE = "shortestPathVertexProgram.predecessors.fromSource";
    public static final String PREDECESSORS_FROM_DESTINATION = "shortestPathVertexProgram.predecessors.fromDestination";
    private static final String ITERATIONS_LEFT = "shortestPathVertexProgram.iterationsLeft";

    private static final String SOURCE = "shortestPathVertexProgram.source";
    private static final String DESTINATION = "shortestPathVertexProgram.destination";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = Sets.newHashSet(
            MemoryComputeKey.of(VOTE_TO_HALT_SOURCE, Operator.and, false, true),
            MemoryComputeKey.of(VOTE_TO_HALT_DESTINATION, Operator.and, false, true),
            MemoryComputeKey.of(FOUND_PATH, Operator.or, true, true),
            MemoryComputeKey.of(PREDECESSORS_FROM_SOURCE, Operator.addAll, false, false),
            MemoryComputeKey.of(PREDECESSORS_FROM_DESTINATION, Operator.addAll, false, false),
            MemoryComputeKey.of(ITERATIONS_LEFT, Operator.assign, true, true),
            MemoryComputeKey.of(PATH_HAS_MIDDLE_POINT, Operator.and, false, true)
    );

    // Needed internally for OLAP tasks
    public ShortestPathVertexProgram() {
    }

    public ShortestPathVertexProgram(ConceptId sourceId, ConceptId destinationId) {
        this.persistentProperties.put(SOURCE, sourceId.getValue());
        this.persistentProperties.put(DESTINATION, destinationId.getValue());
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return Sets.newHashSet(
                VertexComputeKey.of(PREDECESSOR, true),
                VertexComputeKey.of(VISITED_IN_ITERATION, true));
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return messageScopeSetInAndOut;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("ShortestPathVertexProgram Started !!!!!!!!");

        memory.set(VOTE_TO_HALT_SOURCE, true);
        memory.set(VOTE_TO_HALT_DESTINATION, true);
        memory.set(FOUND_PATH, false);
        memory.set(PATH_HAS_MIDDLE_POINT, true);
        memory.set(ITERATIONS_LEFT, -1);
        memory.set(PREDECESSORS_FROM_SOURCE, new HashMap<String, Set<String>>());
        memory.set(PREDECESSORS_FROM_DESTINATION, new HashMap<String, Set<String>>());
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Tuple> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            // The type of id will likely have to change as we support more and more vendors
            String id = getVertexId(vertex);
            if (persistentProperties.get(SOURCE).equals(id)) {
                LOGGER.debug("Found source vertex");
                vertex.property(VISITED_IN_ITERATION, 1);
                sendMessage(messenger, Pair.with(FROM_SOURCE, id));
            } else if (persistentProperties.get(DESTINATION).equals(id)) {
                LOGGER.debug("Found destination vertex");
                vertex.property(VISITED_IN_ITERATION, -1);
                sendMessage(messenger, Pair.with(FROM_DESTINATION, id));
            }
        } else {
            if (memory.<Boolean>get(FOUND_PATH)) {
                if (messenger.receiveMessages().hasNext() && vertex.property(VISITED_IN_ITERATION).isPresent()) {
                    recordPredecessors(vertex, messenger, memory);
                }
            } else if (messenger.receiveMessages().hasNext()) {
                updateInstance(vertex, messenger, memory);
            }
        }
    }

    private static void sendMessage(Messenger<Tuple> messenger, Tuple message) {
        messenger.sendMessage(messageScopeIn, message);
        messenger.sendMessage(messageScopeOut, message);
    }

    private static void recordPredecessors(Vertex vertex, Messenger<Tuple> messenger, Memory memory) {
        int visitedInIteration = vertex.value(VISITED_IN_ITERATION);
        int iterationLeft = memory.<Integer>get(ITERATIONS_LEFT);
        if (visitedInIteration == iterationLeft) {
            Map<String, Set<String>> predecessorsMap = getPredecessors(vertex, messenger);
            if (!predecessorsMap.isEmpty()) {
                memory.add(PREDECESSORS_FROM_SOURCE, predecessorsMap);
                sendMessage(messenger, Pair.with(FROM_MIDDLE, getVertexId(vertex)));
            }
        } else if (-visitedInIteration == iterationLeft) {
            Map<String, Set<String>> predecessorsMap = getPredecessors(vertex, messenger);
            if (!predecessorsMap.isEmpty()) {
                memory.add(PREDECESSORS_FROM_DESTINATION, predecessorsMap);
                sendMessage(messenger, Pair.with(FROM_MIDDLE, getVertexId(vertex)));
            }
        }
    }

    private static Map<String, Set<String>> getPredecessors(Vertex vertex, Messenger<Tuple> messenger) {
        Set<String> predecessors = new HashSet<>();
        Iterator<Tuple> iterator = messenger.receiveMessages();
        while (iterator.hasNext()) {
            Tuple message = iterator.next();
            if ((int) message.getValue(DIRECTION) == FROM_MIDDLE) {
                predecessors.add((String) message.getValue(ID));
            }
        }
        if (predecessors.isEmpty()) return Collections.emptyMap();

        Map<String, Set<String>> predecessorMap = new HashMap<>();
        predecessorMap.put(getVertexId(vertex), predecessors);
        return predecessorMap;
    }

    private void updateInstance(Vertex vertex, Messenger<Tuple> messenger, Memory memory) {
        if (!vertex.property(VISITED_IN_ITERATION).isPresent()) {
            String id = getVertexId(vertex);
            LOGGER.trace("Checking instance " + id);

            boolean hasMessageSource = false;
            boolean hasMessageDestination = false;
            Iterator<Tuple> iterator = messenger.receiveMessages();
            while (iterator.hasNext()) {
                int messageDirection = (int) iterator.next().getValue(DIRECTION);
                if (messageDirection > 0) {
                    if (!hasMessageSource) { // make sure this is the first msg from source
                        LOGGER.trace("Received a message from source vertex");
                        hasMessageSource = true;
                        vertex.property(VISITED_IN_ITERATION, memory.getIteration() + 1);
                        memory.add(VOTE_TO_HALT_SOURCE, false);
                        if (hasMessageDestination) {
                            LOGGER.trace("Found path(s)");
                            memory.add(FOUND_PATH, true);
                        }
                    }
                } else {
                    if (!hasMessageDestination) { // make sure this is the first msg from destination
                        LOGGER.trace("Received a message from destination vertex");
                        hasMessageDestination = true;
                        vertex.property(VISITED_IN_ITERATION, -memory.getIteration() - 1);
                        memory.add(VOTE_TO_HALT_DESTINATION, false);
                        if (hasMessageSource) {
                            LOGGER.trace("Found path(s)");
                            memory.add(FOUND_PATH, true);
                        }
                    }
                }
            }

            Tuple message;
            if (hasMessageSource && hasMessageDestination) {
                message = Pair.with(FROM_MIDDLE, id);
            } else {
                message = Pair.with(hasMessageSource ? FROM_SOURCE : FROM_DESTINATION, id);
            }
            sendMessage(messenger, message);

        } else {
            if ((int) vertex.value(VISITED_IN_ITERATION) > 0) { // if received msg from source before
                Iterator<Tuple> iterator = messenger.receiveMessages();
                Set<String> middleLinkSet = new HashSet<>();
                while (iterator.hasNext()) {
                    Tuple message = iterator.next();
                    if ((Integer) message.getValue(DIRECTION) == -1) { // received msg from destination
                        middleLinkSet.add((String) message.getValue(ID));
                    }
                }
                if (!middleLinkSet.isEmpty()) {
                    LOGGER.trace("Found path");
                    memory.add(FOUND_PATH, true);
                    memory.add(PATH_HAS_MIDDLE_POINT, false);

                    String id = getVertexId(vertex);
                    Map<String, Set<String>> middleLinkMap = new HashMap<>();
                    middleLinkMap.put(id, middleLinkSet);
                    memory.add(PREDECESSORS_FROM_SOURCE, middleLinkMap);
                    sendMessage(messenger, Pair.with(FROM_MIDDLE, id));
                }
            } else { // if received msg from destination before
                Iterator<Tuple> iterator = messenger.receiveMessages();
                while (iterator.hasNext()) {
                    Tuple message = iterator.next();
                    if ((int) message.getValue(DIRECTION) == FROM_SOURCE) {
                        sendMessage(messenger, Pair.with(FROM_MIDDLE, getVertexId(vertex)));
                        break;
                    }
                }
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        //Be careful in Tinkergraph as things set here cannot be got!!!
        LOGGER.debug("Finished Iteration " + memory.getIteration());

        if (memory.getIteration() == 0) {
            return false;
        }

        if (memory.<Boolean>get(FOUND_PATH)) {
            if (memory.<Integer>get(ITERATIONS_LEFT) == -1) {
                if (!memory.<Boolean>get(PATH_HAS_MIDDLE_POINT)) {
                    if (memory.getIteration() == 1) return true;
                    else memory.set(ITERATIONS_LEFT, memory.getIteration() - 1);
                } else {
                    memory.set(ITERATIONS_LEFT, memory.getIteration());
                }
                return false;
            } else if (memory.<Integer>get(ITERATIONS_LEFT) == 1) {
                return true;
            } else {
                memory.set(ITERATIONS_LEFT, memory.<Integer>get(ITERATIONS_LEFT) - 1);
                return false;
            }
        }

        if (memory.<Boolean>get(VOTE_TO_HALT_SOURCE) || memory.<Boolean>get(VOTE_TO_HALT_DESTINATION)) {
            LOGGER.debug("There is no path between the given instances");
            throw new NoResultException();
        }

        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: " + MAX_ITERATION + " !!!!!!!!");
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        memory.set(VOTE_TO_HALT_SOURCE, true);
        memory.set(VOTE_TO_HALT_DESTINATION, true);
        return false;
    }
}
