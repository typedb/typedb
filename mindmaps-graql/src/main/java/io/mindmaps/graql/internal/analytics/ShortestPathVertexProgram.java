/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.analytics;

import com.google.common.collect.Sets;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ShortestPathVertexProgram extends MindmapsVertexProgram<Integer> {

    private static final int MAX_ITERATION = 100;

    // element key
    private static final String IS_ACTIVE_CASTING = "shortestPathVertexProgram.isActiveCasting";
    protected static final String DISTANCE = "shortestPathVertexProgram.distance";

    // memory key
    private static final String VOTE_TO_HALT = "shortestPathVertexProgram.voteToHalt";
    private static final String FOUND_PATH = "shortestPathVertexProgram.foundPath";

    private static final Set<String> ELEMENT_COMPUTE_KEYS = Sets.newHashSet(IS_ACTIVE_CASTING, DISTANCE);
    private static final Set<String> MEMORY_COMPUTE_KEYS = Sets.newHashSet(VOTE_TO_HALT, FOUND_PATH);

    private static final int MESSAGE_FROM_LABELED_CLUSTER = 1;
    private static final int MESSAGE_FROM_ROLE_PLAYER = -1;
    private static final int MESSAGE_FROM_ASSERTION = -2;

    private static final String START_ID = "shortestPathVertexProgram.startId";
    private static final String END_ID = "shortestPathVertexProgram.endId";

    public ShortestPathVertexProgram() {
    }

    public ShortestPathVertexProgram(Set<String> selectedTypes, String startId, String endId) {
        this.selectedTypes = selectedTypes;
        this.persistentProperties.put(START_ID, startId);
        this.persistentProperties.put(END_ID, endId);
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
    public void setup(final Memory memory) {
        LOGGER.debug("ShortestPathVertexProgram Started !!!!!!!!");
        memory.set(VOTE_TO_HALT, true);
        memory.set(FOUND_PATH, false);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Integer> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    // TODO: try not to persist
                    vertex.property(DISTANCE, Integer.MAX_VALUE);

                    String type = vertex.label();
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        // each role-player sends 1 to castings following incoming edges
                        messenger.sendMessage(messageScopeIn, MESSAGE_FROM_ROLE_PLAYER);
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        // the assertion can also be a role-player, so sending 1 to castings following incoming edges
                        messenger.sendMessage(messageScopeIn, MESSAGE_FROM_ROLE_PLAYER);
                        // send -1 to castings following outgoing edges
                        messenger.sendMessage(messageScopeOut, MESSAGE_FROM_ASSERTION);
                    }
                    // send message from the starting vertex
                    if (persistentProperties.get(START_ID).equals(
                            vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name()))) {
                        vertex.property(DISTANCE, memory.getIteration());
                        LOGGER.debug("Found starting vertex");
                        messenger.sendMessage(messageScopeIn, MESSAGE_FROM_LABELED_CLUSTER);
                        messenger.sendMessage(messageScopeOut, MESSAGE_FROM_LABELED_CLUSTER);
                    }
                }
                break;
            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name())) {
                    // TODO: try not to persist
                    vertex.property(DISTANCE, Integer.MAX_VALUE);

                    Set<Integer> messageSet = new HashSet<>();
                    boolean hasBothMessages = false;
                    boolean isNeighbour = false;
                    Iterator<Integer> iterator = messenger.receiveMessages();
                    while (iterator.hasNext()) {
                        int message = iterator.next();
                        if (message < 0) messageSet.add(message);
                        else isNeighbour = true;
                        if (messageSet.size() == 2) {
                            hasBothMessages = true;
                        }
                        if (hasBothMessages && isNeighbour) {
                            vertex.property(DISTANCE, memory.getIteration());
                            messenger.sendMessage(messageScopeIn, MESSAGE_FROM_LABELED_CLUSTER);
                            messenger.sendMessage(messageScopeOut, MESSAGE_FROM_LABELED_CLUSTER);
                            break;
                        }
                    }
                    // casting is active if both its assertion and role-player is in the subgraph
                    vertex.property(IS_ACTIVE_CASTING, hasBothMessages);
                }
                break;
            default:
                // split the default case because shortcut edges cannot be filtered out
                if (memory.getIteration() % 2 == 0) {
                    if (selectedTypes.contains(Utility.getVertexType(vertex))
                            && messenger.receiveMessages().hasNext()) {
                        update(vertex, messenger, memory);
                    }
                } else {
                    if (vertex.label().equals(Schema.BaseType.CASTING.name()) &&
                            (boolean) vertex.value(IS_ACTIVE_CASTING) && messenger.receiveMessages().hasNext()) {
                        update(vertex, messenger, memory);
                    }
                }
                break;
        }
    }

    private void update(Vertex vertex, Messenger<Integer> messenger, Memory memory) {
        LOGGER.debug("Considering vertex " + vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name()));

        if ((Integer) vertex.value(DISTANCE) != Integer.MAX_VALUE) {
            LOGGER.debug("Distance label exists");
        } else {
            if (persistentProperties.get(END_ID).equals(
                    vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name()))) {
                memory.or(FOUND_PATH, true);
                LOGGER.debug("Found the path");
                return;
            }

            vertex.property(DISTANCE, memory.getIteration());
            messenger.sendMessage(messageScopeIn, MESSAGE_FROM_LABELED_CLUSTER);
            messenger.sendMessage(messageScopeOut, MESSAGE_FROM_LABELED_CLUSTER);
            memory.and(VOTE_TO_HALT, false);
            LOGGER.debug("Distance label added");
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration " + memory.getIteration());
        if (memory.getIteration() < 2) return false;
        if (memory.get(FOUND_PATH)) {
            return true;
        }

        if (memory.<Boolean>get(VOTE_TO_HALT)) {
            LOGGER.debug("There is no path between the given instances");
            return true;
        }

        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: " + MAX_ITERATION + " !!!!!!!!");
            throw new IllegalStateException(ErrorMessage.MAX_ITERATION_REACHED
                    .getMessage(this.getClass().toString()));
        }

        memory.or(VOTE_TO_HALT, true);
        return false;
    }
}
