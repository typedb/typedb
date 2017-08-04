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
import ai.grakn.concept.LabelId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.javatuples.Tuple;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.Utility.vertexHasSelectedTypeId;

/**
 * The vertex program for computing the shortest path between two instances.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class ShortestPathVertexProgram extends GraknVertexProgram<Tuple> {

    private static final int MAX_ITERATION = 50;

    private static final int ID = 0;
    private static final int DIRECTION = 1;
    private static final String DIVIDER = "\t";

    // element key
    private static final String PREDECESSOR = "shortestPathVertexProgram.fromVertex";
    private static final String VISITED_IN_ITERATION = "shortestPathVertexProgram.visitedInIteration";
    public static final String FOUND_IN_ITERATION = "shortestPathVertexProgram.foundInIteration";

    // memory key
    private static final String VOTE_TO_HALT_SOURCE = "shortestPathVertexProgram.voteToHalt.source";
    private static final String VOTE_TO_HALT_DESTINATION = "shortestPathVertexProgram.voteToHalt.destination";
    private static final String FOUND_PATH = "shortestPathVertexProgram.foundDestination";
    private static final String PREDECESSOR_FROM_SOURCE = "shortestPathVertexProgram.fromSource";
    private static final String PREDECESSOR_FROM_DESTINATION = "shortestPathVertexProgram.fromDestination";
    private static final String PREDECESSORS = "shortestPathVertexProgram.predecessors";
    public static final String MIDDLE = "shortestPathVertexProgram.middle";

    private static final Set<String> ELEMENT_COMPUTE_KEYS =
            Sets.newHashSet(PREDECESSOR, VISITED_IN_ITERATION, FOUND_IN_ITERATION);
    private static final Set<String> MEMORY_COMPUTE_KEYS =
            Sets.newHashSet(VOTE_TO_HALT_SOURCE, VOTE_TO_HALT_DESTINATION, FOUND_PATH,
                    PREDECESSOR_FROM_SOURCE, PREDECESSOR_FROM_DESTINATION, PREDECESSORS, MIDDLE);

    private static final String SOURCE = "shortestPathVertexProgram.startId";
    private static final String DESTINATION = "shortestPathVertexProgram.endId";

    // Needed internally for OLAP tasks
    public ShortestPathVertexProgram() {
    }

    public ShortestPathVertexProgram(Set<LabelId> selectedLabelIds, ConceptId sourceId, ConceptId destinationId) {
        this.selectedTypes = selectedLabelIds;
        this.persistentProperties.put(SOURCE, sourceId.getValue());
        this.persistentProperties.put(DESTINATION, destinationId.getValue());
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return ELEMENT_COMPUTE_KEYS;
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return memory.<Boolean>get(FOUND_PATH) ? Collections.emptySet() : messageScopeSetShortcut;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("ShortestPathVertexProgram Started !!!!!!!!");
        memory.set(VOTE_TO_HALT_SOURCE, true);
        memory.set(VOTE_TO_HALT_DESTINATION, true);
        memory.set(FOUND_PATH, false);
        memory.set(PREDECESSOR_FROM_SOURCE, "");
        memory.set(PREDECESSOR_FROM_DESTINATION, "");
        memory.set(PREDECESSORS, "");
        memory.set(MIDDLE, "");
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Tuple> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (vertexHasSelectedTypeId(vertex, selectedTypes)) {
                    // send message from both source(1) and destination(-1) vertex
                    String id = vertex.value(Schema.VertexProperty.ID.name());
                    if (persistentProperties.get(SOURCE).equals(id)) {
                        LOGGER.debug("Found source vertex");
                        vertex.property(PREDECESSOR, "");
                        vertex.property(VISITED_IN_ITERATION, 1);
                        messenger.sendMessage(messageScopeShortcutIn, Pair.with(id, 1));
                        messenger.sendMessage(messageScopeShortcutOut, Pair.with(id, 1));
                    } else if (persistentProperties.get(DESTINATION).equals(id)) {
                        LOGGER.debug("Found destination vertex");
                        vertex.property(PREDECESSOR, "");
                        vertex.property(VISITED_IN_ITERATION, -1);
                        messenger.sendMessage(messageScopeShortcutIn, Pair.with(id, -1));
                        messenger.sendMessage(messageScopeShortcutOut, Pair.with(id, -1));
                    }
                }
                break;
            default:
                if (memory.<Boolean>get(FOUND_PATH)) {
                    //This will likely have to change as we support more and more vendors.
                    String id = vertex.value(Schema.VertexProperty.ID.name());
                    if (memory.get(PREDECESSOR_FROM_SOURCE).equals(id)) {
                        LOGGER.debug("Traversing back to vertex " + id);
                        memory.set(PREDECESSOR_FROM_SOURCE, vertex.value(PREDECESSOR));
                        vertex.property(FOUND_IN_ITERATION, -1 * memory.getIteration());
                    } else if (memory.get(PREDECESSOR_FROM_DESTINATION).equals(id)) {
                        LOGGER.debug("Traversing back to vertex " + id);
                        memory.set(PREDECESSOR_FROM_DESTINATION, vertex.value(PREDECESSOR));
                        vertex.property(FOUND_IN_ITERATION, memory.getIteration());
                    }
                } else if (vertexHasSelectedTypeId(vertex, selectedTypes) && messenger.receiveMessages().hasNext()) {
                    updateInstance(vertex, messenger, memory);
                }
                break;
        }
    }

    private void updateInstance(Vertex vertex, Messenger<Tuple> messenger, Memory memory) {
        if (!vertex.property(PREDECESSOR).isPresent()) {
            String id = vertex.value(Schema.VertexProperty.ID.name());
            LOGGER.debug("Considering instance " + id);

            Iterator<Tuple> iterator = messenger.receiveMessages();
            boolean hasMessageSource = false;
            boolean hasMessageDestination = false;
            String predecessorFromSource = null;
            String predecessorFromDestination = null;

            while (iterator.hasNext()) {
                Tuple message = iterator.next();
                int messageDirection = (int) message.getValue(DIRECTION);

                if (messageDirection > 0) {
                    if (!hasMessageSource) {
                        LOGGER.debug("Received a message from source vertex");
                        hasMessageSource = true;
                        predecessorFromSource = (String) message.getValue(ID);
                        vertex.property(PREDECESSOR, predecessorFromSource);
                        vertex.property(VISITED_IN_ITERATION, memory.getIteration() + 1);
                        memory.and(VOTE_TO_HALT_SOURCE, false);
                        if (hasMessageDestination) {
                            LOGGER.debug("Found path");
                            memory.or(FOUND_PATH, true);
                            memory.set(PREDECESSORS, predecessorFromSource + DIVIDER + predecessorFromDestination +
                                    DIVIDER + id);
                            return;
                        }
                    }
                } else {
                    if (!hasMessageDestination) {
                        LOGGER.debug("Received a message from destination vertex");
                        hasMessageDestination = true;
                        predecessorFromDestination = (String) message.getValue(ID);
                        vertex.property(PREDECESSOR, predecessorFromDestination);
                        vertex.property(VISITED_IN_ITERATION, -1 * memory.getIteration() - 1);
                        memory.and(VOTE_TO_HALT_DESTINATION, false);
                        if (hasMessageSource) {
                            LOGGER.debug("Found path");
                            memory.or(FOUND_PATH, true);
                            memory.set(PREDECESSORS, predecessorFromSource + DIVIDER + predecessorFromDestination +
                                    DIVIDER + id);
                            return;
                        }
                    }
                }
            }

            int message = hasMessageSource ? 1 : -1;
            messenger.sendMessage(messageScopeShortcutIn, Pair.with(id, message));
            messenger.sendMessage(messageScopeShortcutOut, Pair.with(id, message));

        } else {
            int messageDirection = memory.getIteration() / (int) vertex.value(VISITED_IN_ITERATION);
            if (messageDirection == 1) {
                Iterator<Tuple> iterator = messenger.receiveMessages();
                while (iterator.hasNext()) {
                    Tuple message = iterator.next();
                    if ((int) message.getValue(DIRECTION) == -1) {
                        LOGGER.debug("Found path");
                        memory.or(FOUND_PATH, true);
                        memory.set(PREDECESSORS, vertex.value(Schema.VertexProperty.ID.name()) +
                                DIVIDER + message.getValue(ID));
                        return;
                    }
                }
            } else if (messageDirection == -1) {
                Iterator<Tuple> iterator = messenger.receiveMessages();
                while (iterator.hasNext()) {
                    Tuple message = iterator.next();
                    if ((int) message.getValue(DIRECTION) == 1) {
                        LOGGER.debug("Found path");
                        memory.or(FOUND_PATH, true);
                        memory.set(PREDECESSORS, message.getValue(ID) + DIVIDER +
                                vertex.value(Schema.VertexProperty.ID.name()));
                        return;
                    }
                }
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration " + memory.getIteration());
        if (memory.getIteration() == 0) return false;

        if (memory.<Boolean>get(FOUND_PATH)) {
            if (!memory.get(PREDECESSORS).equals("")) {
                String[] predecessors = ((String) memory.get(PREDECESSORS)).split(DIVIDER);
                memory.set(PREDECESSORS, "");
                memory.set(PREDECESSOR_FROM_SOURCE, predecessors[0]);
                memory.set(PREDECESSOR_FROM_DESTINATION, predecessors[1]);
                if (predecessors.length > 2) {
                    memory.set(MIDDLE, predecessors[2]);
                }
                //TODO: be careful in tinkergraph as things set here cannot be got
                return predecessors[0].equals(persistentProperties.get(SOURCE));
            }
            return memory.get(PREDECESSOR_FROM_SOURCE).equals(persistentProperties.get(SOURCE));
        }

        if (memory.<Boolean>get(VOTE_TO_HALT_SOURCE) || memory.<Boolean>get(VOTE_TO_HALT_DESTINATION)) {
            LOGGER.debug("There is no path between the given instances");
            throw new IllegalStateException(ErrorMessage.NO_PATH_EXIST.getMessage());
        }

        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: " + MAX_ITERATION + " !!!!!!!!");
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        memory.or(VOTE_TO_HALT_SOURCE, true);
        memory.or(VOTE_TO_HALT_DESTINATION, true);
        return false;
    }
}
