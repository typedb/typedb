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

package ai.grakn.graql.internal.analytics;

import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.util.Schema;
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

    private static final int DIRECTION = 0;
    private static final int ID = 1;
//    private static final String DIVIDER = "\t";

    //    public static final String FOUND_IN_ITERATION = "shortestPathVertexProgram.foundInIteration";
    public static final String PATH_HAS_MIDDLE_POINT = "shortestPathVertexProgram.pathHasMiddlePoint";

    private static final String PREDECESSOR = "shortestPathVertexProgram.fromVertex";
    private static final String VISITED_IN_ITERATION = "shortestPathVertexProgram.visitedInIteration";
    private static final String VOTE_TO_HALT_SOURCE = "shortestPathVertexProgram.voteToHalt.source";
    private static final String VOTE_TO_HALT_DESTINATION = "shortestPathVertexProgram.voteToHalt.destination";
    private static final String FOUND_PATH = "shortestPathVertexProgram.foundDestination";

//    private static final String PREDECESSOR_FROM_SOURCE = "shortestPathVertexProgram.fromSource";
//    private static final String PREDECESSOR_FROM_DESTINATION = "shortestPathVertexProgram.fromDestination";
//    private static final String PATH_FROM_SOURCE = "shortestPathVertexProgram.pathFromSource";
//    private static final String PATH_FROM_DESTINATION = "shortestPathVertexProgram.pathFromDestination";

//    private static final String MIDDLE_POINTS = "shortestPathVertexProgram.middlePoints";
//    public static final String MIDDLE_LINKS = "shortestPathVertexProgram.middleLinks";

    //    public static final String PREDECESSORS = "shortestPathVertexProgram.predecessors";
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

//            MemoryComputeKey.of(PREDECESSORS, Operator.addAll, true, false),

//            MemoryComputeKey.of(MIDDLE_POINTS, Operator.addAll, true, false),
//            MemoryComputeKey.of(MIDDLE_LINKS, Operator.addAll, true, false),
//            MemoryComputeKey.of(PATH_FROM_SOURCE, MergeMap.INSTANCE, true, false),
//            MemoryComputeKey.of(PATH_FROM_DESTINATION, MergeMap.INSTANCE, true, false),

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
//                VertexComputeKey.of(FOUND_IN_ITERATION, false));
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
//        return memory.<Boolean>get(FOUND_PATH) ? Collections.emptySet() : messageScopeSetInAndOut;
        return messageScopeSetInAndOut;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("ShortestPathVertexProgram Started !!!!!!!!");
        memory.set(VOTE_TO_HALT_SOURCE, true);
        memory.set(VOTE_TO_HALT_DESTINATION, true);
        memory.set(FOUND_PATH, false);
        memory.set(PATH_HAS_MIDDLE_POINT, true);

//        memory.set(MIDDLE, "");

        memory.set(ITERATIONS_LEFT, -1);

//        memory.set(MIDDLE_POINTS, new HashMap<String, Integer>());
//        memory.set(MIDDLE_LINKS, new HashMap<String, Set<String>>());

//        memory.set(PREDECESSORS, new HashMap<String, Set<String>>());
        memory.set(PREDECESSORS_FROM_SOURCE, new HashMap<String, Set<String>>());
        memory.set(PREDECESSORS_FROM_DESTINATION, new HashMap<String, Set<String>>());

//        memory.set(PATH_FROM_SOURCE, new HashMap<String, Set<String>>());
//        memory.set(PATH_FROM_DESTINATION, new HashMap<String, Set<String>>());
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
            } else if (messenger.receiveMessages().hasNext()) {
                updateInstance(vertex, messenger, memory);
            }
        }

//        String id; //This will likely have to change as we support more and more vendors.
//        switch (memory.getIteration()) {
//            case 0:
//                // send message from both source(1) and destination(-1) vertex
//                id = vertex.value(Schema.VertexProperty.ID.name());
//                if (persistentProperties.get(SOURCE).equals(id)) {
//                    LOGGER.debug("Found source vertex");
//                    vertex.property(VISITED_IN_ITERATION, 1);
//                    sendMessage(messenger, Pair.with(FROM_SOURCE, id));
//                } else if (persistentProperties.get(DESTINATION).equals(id)) {
//                    LOGGER.debug("Found destination vertex");
//                    vertex.property(VISITED_IN_ITERATION, -1);
//                    sendMessage(messenger, Pair.with(FROM_DESTINATION, id));
//                }
//                break;
//            default:
//                if (memory.<Boolean>get(FOUND_PATH)) {
//                    if (messenger.receiveMessages().hasNext() && vertex.property(VISITED_IN_ITERATION).isPresent()) {
//                        int visitedInIteration = vertex.value(VISITED_IN_ITERATION);
//                        int iterationLeft = vertex.value(ITERATIONS_LEFT);
//                        if (visitedInIteration == iterationLeft) {
//                            memory.add(PREDECESSORS_FROM_SOURCE, getPredecessors(vertex, messenger));
//                            sendMessage(messenger, Pair.with(FROM_SOURCE, getVertexId(vertex)));
//                        } else if (-visitedInIteration == iterationLeft) {
//                            memory.add(PREDECESSORS_FROM_DESTINATION, getPredecessors(vertex, messenger));
//                            sendMessage(messenger, Pair.with(FROM_DESTINATION, getVertexId(vertex)));
//                        }
//                    }
//
////                    id = vertex.value(Schema.VertexProperty.ID.name());
////                    if (memory.get(PREDECESSOR_FROM_SOURCE).equals(id)) {
////                        LOGGER.debug("Traversing back to vertex " + id);
////                        memory.add(PREDECESSOR_FROM_SOURCE, vertex.value(PREDECESSOR));
////                        vertex.property(FOUND_IN_ITERATION, -memory.getIteration());
////                    } else if (memory.get(PREDECESSOR_FROM_DESTINATION).equals(id)) {
////                        LOGGER.debug("Traversing back to vertex " + id);
////                        memory.add(PREDECESSOR_FROM_DESTINATION, vertex.value(PREDECESSOR));
////                        vertex.property(FOUND_IN_ITERATION, memory.getIteration());
////                    }
//                } else if (messenger.receiveMessages().hasNext()) {
//                    updateInstance(vertex, messenger, memory);
//                }
//                break;
//        }
    }

    private static void sendMessage(Messenger<Tuple> messenger, Tuple message) {
        messenger.sendMessage(messageScopeIn, message);
        messenger.sendMessage(messageScopeOut, message);
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
            LOGGER.debug("Checking instance " + id);

//            Map<String, Set<String>> predecessorMap = new HashMap<>();
//            predecessorMap.put(id, new HashSet<>());

            Iterator<Tuple> iterator = messenger.receiveMessages();
            boolean hasMessageSource = false;
            boolean hasMessageDestination = false;
//            String predecessorFromSource = null;
//            String predecessorFromDestination = null;

//            Map<String, Integer> middlePoint = new HashMap<>();
            while (iterator.hasNext()) {
//                middlePoint.put(id, 0);

                int messageDirection = (int) iterator.next().getValue(DIRECTION);
                if (messageDirection > 0) {
                    if (!hasMessageSource) { // make sure this is the first msg from source
                        LOGGER.debug("Received a message from source vertex");
                        hasMessageSource = true;
                        vertex.property(VISITED_IN_ITERATION, memory.getIteration() + 1);
                        memory.add(VOTE_TO_HALT_SOURCE, false);
                        if (hasMessageDestination) {
                            LOGGER.debug("Found path(s)");
                            memory.add(FOUND_PATH, true);

//                            middlePoint.put(id, 0);
//                            memory.add(MIDDLE_POINTS, middlePoint);
//                            return;
                        }
                    }
                } else {
                    if (!hasMessageDestination) { // make sure this is the first msg from destination
                        LOGGER.debug("Received a message from destination vertex");
                        hasMessageDestination = true;
                        vertex.property(VISITED_IN_ITERATION, -memory.getIteration() - 1);
                        memory.add(VOTE_TO_HALT_DESTINATION, false);
                        if (hasMessageSource) {
                            LOGGER.debug("Found path(s)");
                            memory.add(FOUND_PATH, true);

//                            middlePoint.put(id, 0);
//                            memory.add(MIDDLE_POINTS, middlePoint);
//                            return;
                        }
                    }
                }
            }

            Tuple message;
            if (hasMessageSource && hasMessageDestination) { // path found, no need to send more messages
//                memory.add(MIDDLE_POINTS, middlePoint);
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
                    LOGGER.debug("Found path");
                    memory.add(FOUND_PATH, true);
                    memory.add(PATH_HAS_MIDDLE_POINT, false);
//                    memory.add(MIDDLE_POINTS, 1);

                    String id = vertex.value(Schema.VertexProperty.ID.name());
                    Map<String, Set<String>> middleLinkMap = new HashMap<>();
                    middleLinkMap.put(id, middleLinkSet);
                    memory.add(PREDECESSORS_FROM_SOURCE, middleLinkMap);

                    sendMessage(messenger, Pair.with(FROM_MIDDLE, id));
                }
            } else { // if received msg from destination before
                Iterator<Tuple> iterator = messenger.receiveMessages();
                boolean hasBothMessages = false;
                while (iterator.hasNext()) {
                    Tuple message = iterator.next();
                    if ((int) message.getValue(DIRECTION) == FROM_SOURCE) {
                        hasBothMessages = true;
//                        LOGGER.debug("Found path");
//                        memory.add(FOUND_PATH, true);
//                        memory.add(MIDDLE_POINTS, -1);
                        break;
                    }
                }
                if (hasBothMessages) {
                    sendMessage(messenger, Pair.with(FROM_MIDDLE, getVertexId(vertex)));
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
//                Map<String, Integer> middlePoints = memory.get(MIDDLE_POINTS);
//                System.out.println("middlePoints = " + middlePoints);

                if (!memory.<Boolean>get(PATH_HAS_MIDDLE_POINT)) {
                    if (memory.getIteration() == 1) return true;
                    else memory.set(ITERATIONS_LEFT, memory.getIteration() - 1);
                } else {
                    memory.set(ITERATIONS_LEFT, memory.getIteration());
                }

//                if (memory.getIteration() == 1 && !memory.<Boolean>get(PATH_HAS_MIDDLE_POINT)) {
//                    return true;
//                }
//
//                memory.set(ITERATIONS_LEFT, memory.getIteration());
//                if (middlePoints.isEmpty()) {
//                    memory.set(ITERATIONS_LEFT, memory.getIteration() - 1);
//                    Map<String, Set<String>> middleLinks = memory.get(MIDDLE_LINKS);
////                    memory.set(PREDECESSORS_FROM_SOURCE, new HashSet<>(middleLinks.keySet()));
////                    memory.set(PREDECESSORS_FROM_DESTINATION, new HashSet<>(middleLinks.values()));
//                    middleLinks.forEach((idSource, idSet) -> {
//                        middlePoints.put(idSource, FROM_SOURCE);
//                        idSet.forEach(idDestination -> middlePoints.put(idDestination, FROM_DESTINATION));
//                    });
//                    memory.set(PREDECESSOR, middlePoints);
//                }
//                else {
//                    memory.set(PREDECESSOR, middlePoints);
//                }
                return false;
            }
            if (memory.<Integer>get(ITERATIONS_LEFT) == 1) return true;
            else {
                memory.set(ITERATIONS_LEFT, memory.<Integer>get(ITERATIONS_LEFT) - 1);
                return false;
            }

//            if (!memory.get(PREDECESSORS).equals("")) {
//                String[] predecessors = ((String) memory.get(PREDECESSORS)).split(DIVIDER);
//                memory.set(PREDECESSORS, "");
//                memory.set(PREDECESSOR_FROM_SOURCE, predecessors[0]);
//                memory.set(PREDECESSOR_FROM_DESTINATION, predecessors[1]);
//                if (predecessors.length > 2) {
//                    memory.set(MIDDLE, predecessors[2]);
//                }
//                //Be careful in Tinkergraph as things set here cannot be got!!!
//                return predecessors[0].equals(persistentProperties.get(SOURCE));
//            }
//            return memory.get(PREDECESSOR_FROM_SOURCE).equals(persistentProperties.get(SOURCE));
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
//
//    public static final class MergeMap implements BinaryOperator<Map<String, Set>> {
//
//        private static final MergeMap INSTANCE = new MergeMap();
//
//        @Override
//        public Map<String, Set> apply(final Map<String, Set> a, final Map<String, Set> b) {
//            return a.size() < b.size() ? merge(a, b) : merge(b, a);
//        }
//
//        private Map<String, Set> merge(final Map<String, Set> a, final Map<String, Set> b) {
//            a.forEach((k, v) -> {
//                if (b.containsKey(k)) {
//                    b.get(k).addAll(v);
//                } else {
//                    b.put(k, v);
//                }
//            });
//            return b;
//        }
//    }
}
