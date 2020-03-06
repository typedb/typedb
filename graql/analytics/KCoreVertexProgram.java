/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.analytics;

import com.google.common.collect.Iterators;
import grakn.core.core.Schema;
import grakn.core.kb.graql.exception.GraqlQueryException;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

/**
 * The vertex program for computing k-core.
 *
 */

public class KCoreVertexProgram extends GraknVertexProgram<String> {

    private static final int MAX_ITERATION = 200;
    private static final String EMPTY_MESSAGE = "";

    public static final String K_CORE_LABEL = "kCoreVertexProgram.kCoreLabel";

    static final String IMPLICIT_MESSAGE_COUNT = "kCoreVertexProgram.implicitMessageCount";
    static final String MESSAGE_COUNT = "corenessVertexProgram.messageCount";
    static final String K_CORE_STABLE = "kCoreVertexProgram.stable";
    static final String K_CORE_EXIST = "kCoreVertexProgram.exist";
    static final String K = "kCoreVertexProgram.k";

    private static final String CONNECTED_COMPONENT_STARTED = "kCoreVertexProgram.ccStarted";
    private static final String VOTE_TO_HALT = "kCoreVertexProgram.voteToHalt";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = newHashSet(
            MemoryComputeKey.of(K_CORE_STABLE, Operator.and, false, true),
            MemoryComputeKey.of(K_CORE_EXIST, Operator.or, false, true),
            MemoryComputeKey.of(CONNECTED_COMPONENT_STARTED, Operator.assign, true, true),
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true),
            MemoryComputeKey.of(K, Operator.assign, true, true));

    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = newHashSet(
            VertexComputeKey.of(K_CORE_LABEL, false),
            VertexComputeKey.of(IMPLICIT_MESSAGE_COUNT, true));

    @SuppressWarnings("unused") //Needed internally for OLAP tasks
    public KCoreVertexProgram() {
    }

    public KCoreVertexProgram(long kValue) {
        this.persistentProperties.put(K, kValue);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return VERTEX_COMPUTE_KEYS;
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("KCoreVertexProgram Started !!!!!!!!");

        // K_CORE_STABLE is true by default, and we reset it after each odd iteration.
        memory.set(K_CORE_STABLE, false);
        memory.set(K_CORE_EXIST, false);
        memory.set(K, persistentProperties.get(K));
        memory.set(VOTE_TO_HALT, true);
        memory.set(CONNECTED_COMPONENT_STARTED, false);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<String> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                sendMessage(messenger, EMPTY_MESSAGE);
                break;

            case 1: // get degree first, as degree must >= k
                filterByDegree(vertex, messenger, memory, true);
                break;

            default:
                if (memory.<Boolean>get(CONNECTED_COMPONENT_STARTED)) {
                    if (messenger.receiveMessages().hasNext()) {
                        if (vertex.property(K_CORE_LABEL).isPresent()) {
                            updateClusterLabel(vertex, messenger, memory);
                        } else if (vertex.label().equals(Schema.BaseType.RELATION.name())) {
                            relayClusterLabel(messenger, memory);
                        }
                    }
                } else {
                    // relay message through relation vertices in even iterations
                    // send message from regular entities in odd iterations
                    if (atRelations(memory)) {
                        relayOrSaveMessages(vertex, messenger);
                    } else {
                        updateEntityAndAttribute(vertex, messenger, memory, false);
                    }
                }
                break;
        }
    }

    static void filterByDegree(Vertex vertex, Messenger<String> messenger, Memory memory, boolean persistId) {
        if ((vertex.label().equals(Schema.BaseType.ENTITY.name()) ||
                vertex.label().equals(Schema.BaseType.ATTRIBUTE.name())) &&
                Iterators.size(messenger.receiveMessages()) >= memory.<Long>get(K)) {
            String id = vertex.id().toString();

            // coreness query doesn't require id
            if (persistId) {
                vertex.property(K_CORE_LABEL, id);
            } else {
                vertex.property(K_CORE_LABEL, true);
            }
            memory.add(K_CORE_EXIST, true);

            // send ids from now on, as we want to count connected entities, not relations
            sendMessage(messenger, id);
        }
    }

    static void relayOrSaveMessages(Vertex vertex, Messenger<String> messenger) {
        if (messenger.receiveMessages().hasNext()) {
            if (vertex.label().equals(Schema.BaseType.RELATION.name())) {
                // relay the messages
                messenger.receiveMessages().forEachRemaining(msg -> sendMessage(messenger, msg));
            } else if ((vertex.label().equals(Schema.BaseType.ENTITY.name()) ||
                    vertex.label().equals(Schema.BaseType.ATTRIBUTE.name())) &&
                    vertex.property(K_CORE_LABEL).isPresent()) {
                // messages received via implicit edge, save the count for next iteration
                vertex.property(IMPLICIT_MESSAGE_COUNT, (long) newHashSet(messenger.receiveMessages()).size());
            }
        }
    }

    static void updateEntityAndAttribute(Vertex vertex, Messenger<String> messenger,
                                         Memory memory, boolean persistMessageCount) {
        if (vertex.property(K_CORE_LABEL).isPresent()) {
            String id = vertex.id().toString();
            long messageCount = (long) getMessageCountExcludeSelf(messenger, id);
            if (vertex.property(IMPLICIT_MESSAGE_COUNT).isPresent()) {
                messageCount += vertex.<Long>value(IMPLICIT_MESSAGE_COUNT);
                // need to remove implicit count as the vertex may not receive msg via implicit edge
                vertex.property(IMPLICIT_MESSAGE_COUNT).remove();
            }

            if (messageCount >= memory.<Long>get(K)) {
                LOGGER.trace("Sending msg from {}", id);
                sendMessage(messenger, id);
                memory.add(K_CORE_EXIST, true);

                if (persistMessageCount) {
                    // message count may help eliminate unqualified vertex in earlier iterations
                    vertex.property(MESSAGE_COUNT, messageCount);
                }
            } else {
                LOGGER.trace("Removing label of {}", id);
                vertex.property(K_CORE_LABEL).remove();
                memory.add(K_CORE_STABLE, false);
            }
        }
    }

    private static void updateClusterLabel(Vertex vertex, Messenger<String> messenger, Memory memory) {
        String currentMax = vertex.value(K_CORE_LABEL);
        String max = IteratorUtils.reduce(messenger.receiveMessages(), currentMax,
                (a, b) -> a.compareTo(b) > 0 ? a : b);
        if (!max.equals(currentMax)) {
            LOGGER.trace("Cluster label of {} changed from {} to {}", vertex, currentMax, max);
            vertex.property(K_CORE_LABEL, max);
            sendMessage(messenger, max);
            memory.add(VOTE_TO_HALT, false);
        } else {
            LOGGER.trace("Cluster label of {} is still {}", vertex, currentMax);
        }
    }

    private static void relayClusterLabel(Messenger<String> messenger, Memory memory) {
        String firstMessage = messenger.receiveMessages().next();
        String max = IteratorUtils.reduce(messenger.receiveMessages(), firstMessage,
                (a, b) -> a.compareTo(b) > 0 ? a : b);
        sendMessage(messenger, max);
        memory.add(VOTE_TO_HALT, false);
    }

    // count the messages from relations, so need to filter its own msg
    private static int getMessageCountExcludeSelf(Messenger<String> messenger, String id) {
        Set<String> messageSet = newHashSet(messenger.receiveMessages());
        messageSet.remove(id);
        return messageSet.size();
    }

    static void sendMessage(Messenger<String> messenger, String message) {
        messenger.sendMessage(messageScopeIn, message);
        messenger.sendMessage(messageScopeOut, message);
    }

    static boolean atRelations(Memory memory) {
        return memory.getIteration() % 2 == 0;
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration {}", memory.getIteration());
        if (memory.isInitialIteration()) return false;

        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: {}", MAX_ITERATION);
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        if (memory.<Boolean>get(CONNECTED_COMPONENT_STARTED)) {
            if (memory.<Boolean>get(VOTE_TO_HALT)) {
                LOGGER.debug("KCoreVertexProgram Finished");
                return true; // connected component is done
            } else {
                memory.set(VOTE_TO_HALT, true);
                return false;
            }
        } else {
            if (!atRelations(memory)) {
                if (!memory.<Boolean>get(K_CORE_EXIST)) {
                    LOGGER.debug("KCoreVertexProgram Finished");
                    LOGGER.debug("No Such Core Areas Found");
                    throw new NoResultException();
                } else {
                    if (memory.<Boolean>get(K_CORE_STABLE)) {
                        memory.set(CONNECTED_COMPONENT_STARTED, true);
                        LOGGER.debug("Found Core Areas");
                        LOGGER.debug("Starting Connected Components");
                    } else {
                        memory.set(K_CORE_EXIST, false);
                        memory.set(K_CORE_STABLE, true);
                    }
                    return false;
                }
            } else {
                return false; // can not end after relayOrSaveMessages
            }
        }
    }
}