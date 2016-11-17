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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ShortestPathVertexProgram extends GraknVertexProgram<Tuple> {

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
        if ((Boolean)memory.get(FOUND_DESTINATION)) return Collections.emptySet();
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
    public void safeExecute(final Vertex vertex, Messenger<Tuple> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    String type = vertex.label();
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        messenger.sendMessage(messageScopeIn, Pair.with(MESSAGE_FROM_ROLE_PLAYER, 1));
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        messenger.sendMessage(messageScopeIn, Pair.with(MESSAGE_FROM_ROLE_PLAYER, 1));
                        messenger.sendMessage(messageScopeOut, Pair.with(MESSAGE_FROM_ASSERTION, 2));
                    }
                    // send message from the source vertex
                    String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
                    if (persistentProperties.get(START_ID).equals(id)) {
                        LOGGER.debug("Found starting vertex");
                        vertex.property(FROM_VERTEX, "");
                        messenger.sendMessage(messageScopeIn, Pair.with(id, 1));
                        messenger.sendMessage(messageScopeOut, Pair.with(id, 2));
                    }
                }
                break;
            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name())) {
                    Set<String> messageSet = new HashSet<>();
                    boolean hasBothMessages = false;
                    String fromVertex = null;
                    int messageDirection = 0;
                    Iterator<Tuple> iterator = messenger.receiveMessages();
                    int i = 0;
                    while (iterator.hasNext()) {
                        Tuple message = iterator.next();
                        String messageContent = (String) message.getValue(0);
                        LOGGER.debug("Message " + i++ + ": " + messageContent);

                        if (messageContent.length() == 1) messageSet.add(messageContent);
                        else {
                            fromVertex = messageContent;
                            messageDirection = (int) message.getValue(1);
                        }

                        if (messageSet.size() == 2) {
                            hasBothMessages = true;
                        }
                        if (hasBothMessages && fromVertex != null) {
                            if (messageDirection == 1)
                                messenger.sendMessage(messageScopeIn, Pair.with(fromVertex, 1));
                            else
                                messenger.sendMessage(messageScopeOut, Pair.with(fromVertex, 2));
                            LOGGER.debug("This is a casting node connected to source vertex");
                            break;
                        }
                    }
                    // casting is active if both its assertion and role-player is in the subgraph
                    vertex.property(IS_ACTIVE_CASTING, hasBothMessages);
                }
                break;
            default:
                if ((Boolean)memory.get(FOUND_DESTINATION)) {
                    String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
                    if (memory.get(NEXT_DESTINATION).equals(id)) {
                        LOGGER.debug("Traversing back to vertex " + id);
                        memory.set(NEXT_DESTINATION, vertex.value(FROM_VERTEX));
                        vertex.property(FOUND_IN_ITERATION, memory.getIteration());
                    }
                } else {
                    if (messenger.receiveMessages().hasNext()) {

                        // split because shortcut edges cannot be filtered out
                        if (memory.getIteration() % 2 == 0) {
                            if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                                updateInstance(vertex, messenger, memory);
                            }
                        } else {
                            if (vertex.label().equals(Schema.BaseType.CASTING.name()) &&
                                    (boolean) vertex.value(IS_ACTIVE_CASTING)) {
                                updateCasting(vertex, messenger, memory);
                            }
                        }
                    }
                }
                break;
        }
    }

    private void updateInstance(Vertex vertex, Messenger<Tuple> messenger, Memory memory) {
        if (!vertex.property(FROM_VERTEX).isPresent()) {
            String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
            LOGGER.debug("Considering vertex " + id);

            String fromVertex = (String) messenger.receiveMessages().next().getValue(0);

            if (persistentProperties.get(END_ID).equals(id)) {
                memory.or(FOUND_DESTINATION, true);
                memory.set(NEXT_DESTINATION, fromVertex);
                LOGGER.debug("Found the destination vertex");
                return;
            }

            vertex.property(FROM_VERTEX, fromVertex);
            messenger.sendMessage(messageScopeIn, Pair.with(id, 1));
            messenger.sendMessage(messageScopeOut, Pair.with(id, 2));
            LOGGER.debug("FromVertex added, messages sent");

            memory.and(VOTE_TO_HALT, false);
        }
    }

    private void updateCasting(Vertex vertex, Messenger<Tuple> messenger, Memory memory) {
        LOGGER.debug("Considering vertex " + vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name()));
        boolean messageSentIn = false;
        boolean messageSentOut = false;
        Iterator<Tuple> receiveMessages = messenger.receiveMessages();
        while (receiveMessages.hasNext()) {
            Tuple message = receiveMessages.next();
            if ((int) message.getValue(1) == 1) {
                if (!messageSentIn) {
                    messenger.sendMessage(messageScopeIn, message);
                    messageSentIn = true;
                }
            } else {
                if (!messageSentOut) {
                    messenger.sendMessage(messageScopeOut, message);
                    messageSentOut = true;
                }
            }
            if (messageSentIn && messageSentOut) break;
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration " + memory.getIteration());
        if (memory.getIteration() < 2) return false;
        if ((Boolean)memory.get(FOUND_DESTINATION)) {
            return memory.get(NEXT_DESTINATION).equals(persistentProperties.get(START_ID));
        }

        if (memory.<Boolean>get(VOTE_TO_HALT) && memory.getIteration() % 2 == 0) {
            LOGGER.debug("There is no path between the given instances");
            throw new IllegalStateException(ErrorMessage.NO_PATH_EXIST.getMessage());
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
