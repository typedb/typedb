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

import java.util.*;

public class ShortestPathVertexProgram extends GraknVertexProgram<Tuple> {

    private static final int MAX_ITERATION = 50;

    // element key
    private static final String IS_ACTIVE_CASTING = "shortestPathVertexProgram.isActiveCasting";
    private static final String PREDECESSOR = "shortestPathVertexProgram.fromVertex";
    public static final String FOUND_IN_ITERATION = "shortestPathVertexProgram.foundInIteration";

    // memory key
    private static final String VOTE_TO_HALT_SOURCE = "shortestPathVertexProgram.voteToHalt.source";
    private static final String VOTE_TO_HALT_DESTINATION = "shortestPathVertexProgram.voteToHalt.destination";
    private static final String FOUND_PATH = "shortestPathVertexProgram.foundDestination";
    private static final String PREDECESSOR_FROM_SOURCE = "shortestPathVertexProgram.fromSource";
    private static final String PREDECESSOR_FROM_DESTINATION = "shortestPathVertexProgram.fromDestination";

    private static final Set<String> ELEMENT_COMPUTE_KEYS =
            Sets.newHashSet(IS_ACTIVE_CASTING, PREDECESSOR, FOUND_IN_ITERATION);
    private static final Set<String> MEMORY_COMPUTE_KEYS =
            Sets.newHashSet(VOTE_TO_HALT_SOURCE, VOTE_TO_HALT_DESTINATION, FOUND_PATH,
                    PREDECESSOR_FROM_SOURCE, PREDECESSOR_FROM_DESTINATION);

    private static final String MESSAGE_FROM_ROLE_PLAYER = "R";
    private static final String MESSAGE_FROM_ASSERTION = "A";

    private static final String SOURCE = "shortestPathVertexProgram.startId";
    private static final String DESTINATION = "shortestPathVertexProgram.endId";

    public ShortestPathVertexProgram() {
    }

    public ShortestPathVertexProgram(Set<String> selectedTypes, String sourceId, String destinationId) {
        this.selectedTypes = selectedTypes;
        this.persistentProperties.put(SOURCE, sourceId);
        this.persistentProperties.put(DESTINATION, destinationId);
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
        if ((Boolean)memory.get(FOUND_PATH)) return Collections.emptySet();
        return messageScopeSet;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.info("ShortestPathVertexProgram Started !!!!!!!!");
        memory.set(VOTE_TO_HALT_SOURCE, true);
        memory.set(VOTE_TO_HALT_DESTINATION, true);
        memory.set(FOUND_PATH, false);
        memory.set(PREDECESSOR_FROM_SOURCE, "");
        memory.set(PREDECESSOR_FROM_DESTINATION, "");
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Tuple> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    String type = vertex.label();
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        messenger.sendMessage(messageScopeIn, Pair.with(MESSAGE_FROM_ROLE_PLAYER, 0));
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        messenger.sendMessage(messageScopeIn, Pair.with(MESSAGE_FROM_ROLE_PLAYER, 0));
                        messenger.sendMessage(messageScopeOut, Pair.with(MESSAGE_FROM_ASSERTION, 0));
                    }
                    // send message from both source and destination vertex
                    String id = vertex.id().toString();
                    if (persistentProperties.get(SOURCE).equals(id)) {
                        LOGGER.debug("Found source vertex");
                        vertex.property(PREDECESSOR, "");
                        messenger.sendMessage(messageScopeIn, Pair.with(id, 1));
                        messenger.sendMessage(messageScopeOut, Pair.with(id, 2));
                    } else if (persistentProperties.get(DESTINATION).equals(id)) {
                        LOGGER.debug("Found destination vertex");
                        vertex.property(PREDECESSOR, "");
                        messenger.sendMessage(messageScopeIn, Pair.with(id, -1));
                        messenger.sendMessage(messageScopeOut, Pair.with(id, -2));
                    }
                }
                break;
            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name())) {
                    Set<String> messageSet = new HashSet<>();
                    Map<Integer, Tuple> messageMap = new HashMap<>();
                    Iterator<Tuple> iterator = messenger.receiveMessages();
                    int i = 0;
                    while (iterator.hasNext()) {
                        Tuple message = iterator.next();
                        String messageContent = (String) message.getValue(0);
                        int messageDirection = (int) message.getValue(1);
                        LOGGER.debug("Message " + i++ + ": " + messageContent);

                        if (messageDirection == 0) messageSet.add(messageContent);
                        else messageMap.put(messageDirection, message);
                    }
                    // casting is active if both its assertion and role-player is in the subgraph
                    if (messageSet.size() == 2) {
                        LOGGER.debug("Considering casting " + vertex.id().toString());
                        vertex.property(IS_ACTIVE_CASTING, true);
                        sendMessagesFromCasting(messenger, memory, messageMap);
                    }
                }
                break;
            default:
                if (memory.get(FOUND_PATH)) {
                    String id = vertex.id().toString(); //This will likely have to change as we support more and more vendors.
                    if (memory.get(PREDECESSOR_FROM_SOURCE).equals(id)) {
                        LOGGER.debug("Traversing back to vertex " + id);
                        memory.set(PREDECESSOR_FROM_SOURCE, vertex.value(PREDECESSOR));
                        vertex.property(FOUND_IN_ITERATION, -1 * memory.getIteration());
                    } else if (memory.get(PREDECESSOR_FROM_DESTINATION).equals(id)) {
                        LOGGER.debug("Traversing back to vertex " + id);
                        memory.set(PREDECESSOR_FROM_DESTINATION, vertex.value(PREDECESSOR));
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
                                    vertex.property(IS_ACTIVE_CASTING).isPresent()) {
                                updateCasting(vertex, messenger, memory);
                            }
                        }
                    }
                }
                break;
        }
    }

    private void updateInstance(Vertex vertex, Messenger<Tuple> messenger, Memory memory) {
        if (!vertex.property(PREDECESSOR).isPresent()) {
            String id = vertex.id().toString();
            LOGGER.debug("Considering instance " + id);

            Iterator<Tuple> iterator = messenger.receiveMessages();
            boolean hasMessageSource = false;
            boolean hasMessageDestination = false;
            String predecessorFromSource = null;
            String predecessorFromDestination = null;
            while (iterator.hasNext()) {
                Tuple message = iterator.next();
                int messageDirection = (int) message.getValue(1);
                if (messageDirection > 0) {
                    if (!hasMessageSource) {
                        LOGGER.debug("Received a message from source vertex");
                        hasMessageSource = true;
                        predecessorFromSource = (String) message.getValue(0);
                        messenger.sendMessage(messageScopeIn, Pair.with(id, 1));
                        messenger.sendMessage(messageScopeOut, Pair.with(id, 2));
                        vertex.property(PREDECESSOR, predecessorFromSource);
                        memory.and(VOTE_TO_HALT_SOURCE, false);
                    }
                } else {
                    if (!hasMessageDestination) {
                        LOGGER.debug("Received a message from destination vertex");
                        hasMessageDestination = true;
                        predecessorFromDestination = (String) message.getValue(0);
                        messenger.sendMessage(messageScopeIn, Pair.with(id, -1));
                        messenger.sendMessage(messageScopeOut, Pair.with(id, -2));
                        vertex.property(PREDECESSOR, predecessorFromDestination);
                        memory.and(VOTE_TO_HALT_DESTINATION, false);
                    }
                }
                if (hasMessageSource && hasMessageDestination) {
                    LOGGER.debug("Found path");
                    memory.or(FOUND_PATH, true);
                    memory.set(PREDECESSOR_FROM_SOURCE, predecessorFromSource);
                    memory.set(PREDECESSOR_FROM_DESTINATION, predecessorFromDestination);

                    vertex.property(FOUND_IN_ITERATION, memory.getIteration());
                    return;
                }
            }
        }
    }

    private void updateCasting(Vertex vertex, Messenger<Tuple> messenger, Memory memory) {
        LOGGER.debug("Considering casting " + vertex.id().toString());
        Map<Integer, Tuple> messageMap = new HashMap<>();
        Iterator<Tuple> iterator = messenger.receiveMessages();
        int i = 0;
        while (iterator.hasNext()) {
            Tuple message = iterator.next();
            LOGGER.debug("Message " + i++ + ": " + message.getValue(0));
            messageMap.put((int) message.getValue(1), message);
        }
        sendMessagesFromCasting(messenger, memory, messageMap);
    }

    private void sendMessagesFromCasting(Messenger<Tuple> messenger, Memory memory, Map<Integer, Tuple> messageMap) {
        int sum = messageMap.keySet().stream().mapToInt(Integer::intValue).sum();
        if (messageMap.size() == 3) {
            LOGGER.debug("3 messages received, message sum = " + sum);
            LOGGER.debug("Found path");
            memory.or(FOUND_PATH, true);
            if (sum == 1) {
                memory.set(PREDECESSOR_FROM_SOURCE, messageMap.get(1).getValue(0));
                memory.set(PREDECESSOR_FROM_DESTINATION, messageMap.get(-2).getValue(0));
            } else {
                memory.set(PREDECESSOR_FROM_SOURCE, messageMap.get(2).getValue(0));
                memory.set(PREDECESSOR_FROM_DESTINATION, messageMap.get(-1).getValue(0));
            }
            return;
        }
        if (messageMap.size() == 2) {
            LOGGER.debug("2 messages received, message sum = " + sum);
            switch (sum) {
                case 0:
                    messenger.sendMessage(messageScopeOut, messageMap.get(2));
                    messenger.sendMessage(messageScopeOut, messageMap.get(-2));
                    break;
                case 3:
                    messenger.sendMessage(messageScopeIn, messageMap.get(1));
                    messenger.sendMessage(messageScopeOut, messageMap.get(2));
                    break;
                case -3:
                    messenger.sendMessage(messageScopeIn, messageMap.get(-1));
                    messenger.sendMessage(messageScopeOut, messageMap.get(-2));
                    break;
                case 1:
                    LOGGER.debug("Found path");
                    memory.or(FOUND_PATH, true);
                    memory.set(PREDECESSOR_FROM_SOURCE, messageMap.get(2).getValue(0));
                    memory.set(PREDECESSOR_FROM_DESTINATION, messageMap.get(-1).getValue(0));
                    break;
                case -1:
                    LOGGER.debug("Found path");
                    memory.or(FOUND_PATH, true);
                    memory.set(PREDECESSOR_FROM_SOURCE, messageMap.get(1).getValue(0));
                    memory.set(PREDECESSOR_FROM_DESTINATION, messageMap.get(-2).getValue(0));
                    break;
            }
        } else if (messageMap.size() == 1) {
            LOGGER.debug("1 message received, message sum = " + sum);
            switch (sum) {
                case 1:
                    messenger.sendMessage(messageScopeIn, messageMap.get(1));
                    break;
                case -1:
                    messenger.sendMessage(messageScopeIn, messageMap.get(-1));
                    break;
                case 2:
                    messenger.sendMessage(messageScopeOut, messageMap.get(2));
                    break;
                case -2:
                    messenger.sendMessage(messageScopeOut, messageMap.get(-2));
                    break;
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.info("Finished Iteration " + memory.getIteration());
        if (memory.getIteration() == 0) return false;

        if ((Boolean)memory.get(FOUND_PATH)) {
            return memory.get(PREDECESSOR_FROM_SOURCE).equals(persistentProperties.get(SOURCE));
        }

        if (memory.getIteration() % 2 == 0 &&
                (memory.<Boolean>get(VOTE_TO_HALT_SOURCE) || memory.<Boolean>get(VOTE_TO_HALT_DESTINATION))) {
            LOGGER.debug("There is no path between the given instances");
            throw new IllegalStateException(ErrorMessage.NO_PATH_EXIST.getMessage());
        }

        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: " + MAX_ITERATION + " !!!!!!!!");
            throw new IllegalStateException(ErrorMessage.MAX_ITERATION_REACHED
                    .getMessage(this.getClass().toString()));
        }

        memory.or(VOTE_TO_HALT_SOURCE, true);
        memory.or(VOTE_TO_HALT_DESTINATION, true);
        return false;
    }
}
