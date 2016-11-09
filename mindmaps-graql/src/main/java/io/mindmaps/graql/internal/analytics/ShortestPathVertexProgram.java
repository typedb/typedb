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
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyVertexProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ShortestPathVertexProgram extends MindmapsVertexProgram<String> {

    private static final int MAX_ITERATION = 100;

    // element key
    private static final String IS_ACTIVE_CASTING = "shortestPathVertexProgram.isActiveCasting";
    private static final String FROM_VERTEX = "shortestPathVertexProgram.fromVertex";
    public static final String FOUND_IN_ITERATION = "shortestPathVertexProgram.foundInIteration";

    // memory key
    private static final String VOTE_TO_HALT = "shortestPathVertexProgram.voteToHalt";
    private static final String FOUND_DESTINATION = "shortestPathVertexProgram.foundDestination";
    private static final String NEXT_DESTINATION = "shortestPathVertexProgram.nextDestination";

    private static final Set<String> ELEMENT_COMPUTE_KEYS =
            Sets.newHashSet(IS_ACTIVE_CASTING, FROM_VERTEX, FOUND_IN_ITERATION);
    private static final Set<String> MEMORY_COMPUTE_KEYS =
            Sets.newHashSet(VOTE_TO_HALT, FOUND_DESTINATION, NEXT_DESTINATION);

    private static final String MESSAGE_FROM_ROLE_PLAYER = "R";
    private static final String MESSAGE_FROM_ASSERTION = "A";

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
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        if (memory.get(FOUND_DESTINATION)) return Collections.emptySet();
        return messageScopeSet;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("ShortestPathVertexProgram Started !!!!!!!!");
        memory.set(VOTE_TO_HALT, true);
        memory.set(FOUND_DESTINATION, false);
        memory.set(NEXT_DESTINATION, "");
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<String> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
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
                    String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
                    if (persistentProperties.get(START_ID).equals(id)) {
                        LOGGER.debug("Found starting vertex");
                        messenger.sendMessage(messageScopeIn, id);
                        messenger.sendMessage(messageScopeOut, id);
                    }
                }
                break;
            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name())) {
//                    String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
                    Set<String> messageSet = new HashSet<>();
                    boolean hasBothMessages = false;
                    String fromVertex = null;
                    Iterator<String> iterator = messenger.receiveMessages();
                    while (iterator.hasNext()) {
                        String message = iterator.next();
                        if (message.length() == 1) messageSet.add(message);
                        else fromVertex = message;
                        if (messageSet.size() == 2) {
                            hasBothMessages = true;
                        }
                        if (hasBothMessages && fromVertex != null) {
//                            vertex.property(FROM_VERTEX, fromVertex);
                            messenger.sendMessage(messageScopeIn, fromVertex);
                            messenger.sendMessage(messageScopeOut, fromVertex);
                            break;
                        }
                    }
                    // casting is active if both its assertion and role-player is in the subgraph
                    vertex.property(IS_ACTIVE_CASTING, hasBothMessages);
                }
                break;
            default:
                if (memory.get(FOUND_DESTINATION)) {
                    String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
                    if (memory.get(NEXT_DESTINATION).equals(id)) {
                        LOGGER.debug("Traversing back to vertex " + id);
                        memory.set(NEXT_DESTINATION, vertex.value(FROM_VERTEX));
                        vertex.property(FOUND_IN_ITERATION, memory.getIteration());
                    }
                } else {
                    // split the default case because shortcut edges cannot be filtered out
                    if (memory.getIteration() % 2 == 0) {
                        if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                            update(vertex, messenger, memory, false);
                        }
                    } else {
                        if (vertex.label().equals(Schema.BaseType.CASTING.name()) &&
                                (boolean) vertex.value(IS_ACTIVE_CASTING)) {
                            update(vertex, messenger, memory, true);
                        }
                    }
                }
                break;
        }
    }

    private void update(Vertex vertex, Messenger<String> messenger, Memory memory, boolean isCasting) {
//        if (memory.get(FOUND_DESTINATION)) {
//            String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
//            if (memory.get(NEXT_DESTINATION).equals(id)) {
//                LOGGER.debug("Traversing back to vertex " + id);
//                memory.set(NEXT_DESTINATION, vertex.value(FROM_VERTEX));
//                vertex.property(FOUND_IN_ITERATION, memory.getIteration());
//            }
//        } else
        if (messenger.receiveMessages().hasNext()) {
            String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
            LOGGER.debug("Considering vertex " + id);

            if (vertex.property(FROM_VERTEX).isPresent()) {
                LOGGER.debug("FromVertex exists");
            } else {
                String fromVertex = messenger.receiveMessages().next();
                if (persistentProperties.get(END_ID).equals(id)) {
                    memory.or(FOUND_DESTINATION, true);
                    memory.set(NEXT_DESTINATION, fromVertex);
                    LOGGER.debug("Found the destination vertex");
                    return;
                }

                if (isCasting) {
                    String message = messenger.receiveMessages().next();
                    messenger.sendMessage(messageScopeIn, message);
                    messenger.sendMessage(messageScopeOut, message);
                    LOGGER.debug("This is a casting vertex");
                } else {
                    vertex.property(FROM_VERTEX, fromVertex);
                    messenger.sendMessage(messageScopeIn, id);
                    messenger.sendMessage(messageScopeOut, id);
                    LOGGER.debug("FromVertex added");
                }
                memory.and(VOTE_TO_HALT, false);
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration " + memory.getIteration());
        if (memory.getIteration() < 3) return false;
        if (memory.get(FOUND_DESTINATION)) {
            return memory.get(NEXT_DESTINATION).equals(persistentProperties.get(START_ID));
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
