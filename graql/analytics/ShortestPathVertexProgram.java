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
import grakn.core.kb.concept.api.ConceptId;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The vertex program for computing the shortest path between two instances.
 */
public class ShortestPathVertexProgram extends GraknVertexProgram<ShortestPathVertexProgram.VertexMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(ShortestPathVertexProgram.class);

    // persistent properties
    private final String sourceId = "source-id";
    private final String destinationId = "destination-id";

    // vertex property names
    private final String srcMsgFromPrevIterations = "message-from-source";
    private final String destMsgFromPrevIterations = "message-from-destination";
    private final String shortestPathRecordedAndBroadcasted = "shortest-path-found-and-relayed";
    private final String pathFoundButIsNotTheShortest = "not-the-shortest";

    // memory key names
    public static final String SHORTEST_PATH = "result";
    private final String atLeastOneVertexActive = "at-least-one-vertex-active";
    private final String shortestPathLength = "length";
    private final String allShortestPathsFound_TerminateAtTheEndOfThisIteration = "terminate";

    private final MessageScope inEdge = MessageScope.Local.of(__::inE);
    private final MessageScope outEdge = MessageScope.Local.of(__::outE);
    private final long SHORTEST_PATH_LENGTH_NOT_YET_SET = -1L;

    // needed for OLAP
    public ShortestPathVertexProgram() {
    }

    public ShortestPathVertexProgram(ConceptId source, ConceptId destination) {
        persistentProperties.put(sourceId, Schema.elementId(source));
        persistentProperties.put(destinationId, Schema.elementId(destination));
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return new HashSet<>(Arrays.asList(inEdge, outEdge));
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return new HashSet<>(Arrays.asList(
                VertexComputeKey.of(shortestPathRecordedAndBroadcasted, true),
                VertexComputeKey.of(pathFoundButIsNotTheShortest, true),
                VertexComputeKey.of(srcMsgFromPrevIterations, true),
                VertexComputeKey.of(destMsgFromPrevIterations, true)
        ));
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return new HashSet<>(Arrays.asList(
                MemoryComputeKey.of(atLeastOneVertexActive, Operator.or, false, true),
                MemoryComputeKey.of(shortestPathLength, Operator.assign, true, true),
                MemoryComputeKey.of(allShortestPathsFound_TerminateAtTheEndOfThisIteration, Operator.assign, false, true),
                MemoryComputeKey.of(SHORTEST_PATH, Operator.addAll, false, false)
        ));
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(atLeastOneVertexActive, false);
        memory.set(shortestPathLength, SHORTEST_PATH_LENGTH_NOT_YET_SET);
        memory.set(allShortestPathsFound_TerminateAtTheEndOfThisIteration, false);
        memory.set(SHORTEST_PATH, new HashMap<String, Set<String>>());
    }

    @Override
    public void safeExecute(Vertex vertex, Messenger<VertexMessage> messenger, final Memory memory) {
        String vertexId = vertex.id().toString();

        if (source(vertex)) {
            if (memory.isInitialIteration()) {
                broadcastInitialSourceMessage(messenger, memory, vertexId);
                memory.add(atLeastOneVertexActive, true);
            }
            else {
                List<VertexMessage> messages = messages(messenger);
                List<MessageFromSource> incomingSourceMsg = messageFromSource(messages);
                List<MessageFromDestination> incomingDestMsg = messageFromDestination(messages);
                LOG.debug("Iteration {} , Vertex {}: received the following messages: {}, {}", memory.getIteration(), vertexId, incomingSourceMsg, incomingDestMsg);
                if (!incomingSourceMsg.isEmpty() || !incomingDestMsg.isEmpty()) {
                    if (!incomingDestMsg.isEmpty()) {
                        long pathLength = incomingDestMsg.get(0).pathLength();
                        if (memory.<Long>get(shortestPathLength) == SHORTEST_PATH_LENGTH_NOT_YET_SET || pathLength == memory.<Long>get(shortestPathLength)) {
                            recordShortestPath_AndMarkBroadcasted(vertex, memory, vertexId, incomingDestMsg, pathLength);
                        }
                        else {
                            LOG.debug("Iteration {}, Vertex {}: received {} of length {}. This isn't the shortest path, which is of length {}. Do nothing.",
                                    memory.getIteration(), vertexId, incomingDestMsg, pathLength, memory.<Long>get(shortestPathLength));
                        }
                    }
                    else {
                        LOG.debug("Iteration {}, Vertex {}: no message from destination yet. Do nothing", memory.getIteration(), vertexId);
                    }
                }
            }
        }
        else if (destination(vertex)) {
            if (memory.isInitialIteration()) {
                broadcastInitialDestinationMessage(messenger, memory, vertexId);
                memory.add(atLeastOneVertexActive, true);
            }
            else {
                List<VertexMessage> messages = messages(messenger);
                List<MessageFromSource> incomingSourceMsg = messageFromSource(messages);
                List<MessageFromDestination> incomingDestMsg = messageFromDestination(messages);
                LOG.debug("Iteration {} , Vertex {}: received the following messages: {}, {}", memory.getIteration(), vertexId, incomingSourceMsg, incomingDestMsg);
                if (!incomingSourceMsg.isEmpty() || !incomingDestMsg.isEmpty()) {
                    if (!incomingSourceMsg.isEmpty()) {
                        long pathLength = incomingSourceMsg.get(0).pathLength();
                        if (memory.<Long>get(shortestPathLength) == SHORTEST_PATH_LENGTH_NOT_YET_SET || pathLength == memory.<Long>get(shortestPathLength)) {
                            markBroadcasted_TerminateAtTheEndOfThisIeration(vertex, memory, vertexId, incomingSourceMsg, pathLength);
                        }
                        else {
                            LOG.debug("Iteration {}, Vertex {}: received {} of length {}. This isn't the shortest path, which is of length {}. Do nothing.",
                                    memory.getIteration(), vertexId, incomingDestMsg, pathLength, memory.<Long>get(shortestPathLength));
                        }
                    }
                    else {
                        LOG.debug("Iteration {}, Vertex {}: no message from source yet. Do nothing", memory.getIteration(), vertexId);
                    }
                }
            }
        }
        else { // if neither source nor destination vertex
            if (memory.isInitialIteration()) {
                LOG.debug("Iteration {}, Vertex {}: neither a source nor destination vertex. Do nothing", memory.getIteration(), vertexId);
            }
            else {
                boolean shortestPathProcessed = this.<Boolean>get(vertex, shortestPathRecordedAndBroadcasted).orElse(false);

                if (shortestPathProcessed) {
                    LOG.debug("Iteration {}, Vertex {}: shortest path have been relayed. Do nothing", memory.getIteration(), vertexId);
                    return;
                }

                List<VertexMessage> messages = messages(messenger);
                List<MessageFromSource> incomingSourceMsg = messageFromSource(messages);
                List<MessageFromDestination> incomingDestMsg = messageFromDestination(messages);
                LOG.debug("Iteration {} , Vertex {}: received the following messages: {}, {}", memory.getIteration(), vertexId, incomingSourceMsg, incomingDestMsg);

                if (!incomingSourceMsg.isEmpty() || !incomingDestMsg.isEmpty()) {
                    boolean hasNewMessageToProcess = false;
                    if (!get(vertex, srcMsgFromPrevIterations).isPresent() && !incomingSourceMsg.isEmpty()) {
                        set(vertex, srcMsgFromPrevIterations, incomingSourceMsg);
                        broadcastSourceMessages(messenger, memory, vertexId, incomingSourceMsg);
                        hasNewMessageToProcess = true;
                    }
                    if (!get(vertex, destMsgFromPrevIterations).isPresent() && !incomingDestMsg.isEmpty()) {
                        set(vertex, destMsgFromPrevIterations, incomingDestMsg);
                        broadcastDestinationMessages(messenger, memory, vertexId, incomingDestMsg);
                        hasNewMessageToProcess = true;
                    }
                    if (get(vertex, srcMsgFromPrevIterations).isPresent() && get(vertex, destMsgFromPrevIterations).isPresent()) {
                        List<MessageFromSource> srcMsgs = this.<List<MessageFromSource>>get(vertex, srcMsgFromPrevIterations).get();
                        List<MessageFromDestination> destMsgs = this.<List<MessageFromDestination>>get(vertex, destMsgFromPrevIterations).get();
                        long pathLength = srcMsgs.get(0).pathLength() + destMsgs.get(0).pathLength();
                        LOG.debug("Iteration {}, Vertex {}: Path between source and destination with length {} found here.", memory.getIteration(), vertexId, pathLength );

                        if (memory.<Long>get(shortestPathLength) == SHORTEST_PATH_LENGTH_NOT_YET_SET || pathLength == memory.<Long>get(shortestPathLength)) {
                            recordShortestPath_AndMarkBroadcasted(vertex, memory, vertexId, destMsgs, pathLength);
                        }
                        else {
                            LOG.debug("Iteration {}, Vertex {}: is not the shortest path. Do nothing", memory.getIteration(), vertexId);
                            set(vertex, pathFoundButIsNotTheShortest, true);
                        }
                    }
                    memory.add(atLeastOneVertexActive, hasNewMessageToProcess);
                }
                else {
                    LOG.debug("Iteration {}, Vertex {}: receives no message. Do nothing", memory.getIteration(), vertexId);
                }
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        boolean terminate = !memory.<Boolean>get(atLeastOneVertexActive) || memory.<Boolean>get(allShortestPathsFound_TerminateAtTheEndOfThisIteration);
        if (!memory.<Boolean>get(atLeastOneVertexActive)) {
            LOG.debug("No vertex is active. Terminating compute path.");
        }
        if (memory.<Boolean>get(allShortestPathsFound_TerminateAtTheEndOfThisIteration)) {
            LOG.debug("All shortest paths have been found. Terminating compute path.");
        }
        memory.set(atLeastOneVertexActive, false); // set for next iteration
        return terminate;
    }

    private Map<String, Set<String>> recordShortestPath_AndMarkBroadcasted(Vertex vertex, Memory memory, String vertexId, List<MessageFromDestination> destMsgs, long pathLength) {
        Map<String, Set<String>> msg = new HashMap<>(Collections.singletonMap(vertexId,
                destMsgs.stream().map(MessageFromDestination::vertexId).collect(Collectors.toSet())));
        memory.add(SHORTEST_PATH, msg);
        memory.add(shortestPathLength, pathLength);
        set(vertex, shortestPathRecordedAndBroadcasted, true);
        LOG.debug("Iteration {}, Vertex {}: is the shortest path. Record({})", memory.getIteration(), vertexId, msg);
        return msg;
    }

    private void markBroadcasted_TerminateAtTheEndOfThisIeration(Vertex vertex, Memory memory, String vertexId, List<MessageFromSource> incomingSourceMsg, long pathLength) {
        Map<String, Set<String>> msg = new HashMap<>(Collections.singletonMap(vertexId,
                incomingSourceMsg.stream().map(MessageFromSource::vertexId).collect(Collectors.toSet())));
        // memory.add(SHORTEST_PATH, msg); do not record
        memory.add(shortestPathLength, pathLength);
        set(vertex, shortestPathRecordedAndBroadcasted, true);
        memory.add(allShortestPathsFound_TerminateAtTheEndOfThisIteration, true);
        LOG.debug("Iteration {}, Vertex {}: received {}. 'compute new-path' finished. Terminating...", memory.getIteration(), vertexId, msg);
    }

    private void broadcastInitialDestinationMessage(Messenger<VertexMessage> messenger, Memory memory, String vertexId) {
        MessageFromDestination initialOutgoingDestMsg = new MessageFromDestination(vertexId,1L);
        LOG.debug("Iteration {}, Vertex {}: I am the destination vertex [{}]. Sending message {} to neighbors", memory.getIteration(), vertexId, vertexId, initialOutgoingDestMsg);
        broadcastToNeighbors(messenger, initialOutgoingDestMsg);
    }

    private void broadcastInitialSourceMessage(Messenger<VertexMessage> messenger, Memory memory, String vertexId) {
        MessageFromSource initialOutgoingSrcMsg = new MessageFromSource(vertexId,1L);
        broadcastToNeighbors(messenger, initialOutgoingSrcMsg);
        LOG.debug("Iteration {}, Vertex {}: I am the source vertex [{}]. Sending message {} to neighbors", memory.getIteration(), vertexId, vertexId, initialOutgoingSrcMsg);
    }

    private void broadcastDestinationMessages(Messenger<VertexMessage> messenger, Memory memory, String vertexId, List<MessageFromDestination> incomingDestMsg) {
        if (!incomingDestMsg.isEmpty()) {
            MessageFromDestination msg = incomingDestMsg.get(0);
            MessageFromDestination outgoingDstMsg = new MessageFromDestination(vertexId, msg.pathLength() + 1);
            broadcastToNeighbors(messenger, outgoingDstMsg);
            LOG.debug("Iteration {}, Vertex {}: Relaying message {}", memory.getIteration(), vertexId, outgoingDstMsg);
        }
    }

    private void broadcastSourceMessages(Messenger<VertexMessage> messenger, Memory memory, String vertexId, List<MessageFromSource> incomingSourceMsg) {
        if (!incomingSourceMsg.isEmpty()) {
            MessageFromSource msg = incomingSourceMsg.get(0);
            MessageFromSource outgoingSrcMsg = new MessageFromSource(vertexId, msg.pathLength() + 1);
            broadcastToNeighbors(messenger, outgoingSrcMsg);
            LOG.debug("Iteration {}, Vertex {}: Relaying message {}.", memory.getIteration(), vertexId, outgoingSrcMsg);
        }
    }

    private boolean source(Vertex vertex) {
        Object source = persistentProperties.get(sourceId);
        Object vertexId = vertex.id();
        return source.toString().equals(vertexId.toString());
    }

    private boolean destination(Vertex vertex) {
        Object source = persistentProperties.get(destinationId);
        Object vertexId = vertex.id();
        return source.toString().equals(vertexId.toString());
    }

    private List<VertexMessage> messages(Messenger<VertexMessage> messenger) {
        return IteratorUtils.asList(messenger.receiveMessages());
    }

    private List<MessageFromSource> messageFromSource(List<VertexMessage> messages) {
        return IteratorUtils.asList(Iterators.filter(messages.iterator(), e -> e instanceof MessageFromSource));
    }

    private List<MessageFromDestination> messageFromDestination(List<VertexMessage> messages) {
        return IteratorUtils.asList(Iterators.filter(messages.iterator(), e -> e instanceof MessageFromDestination));
    }

    private <T> Optional<T> get(Vertex vertex, String key) {
        return Optional.ofNullable(vertex.property(key).orElse(null)).map(e -> (T) e);
    }

    private void set(Vertex vertex, String key, Object value) {
        vertex.property(key, value);
    }

    private void broadcastToNeighbors(Messenger<VertexMessage> messenger, VertexMessage message) {
        messenger.sendMessage(inEdge, message);
        messenger.sendMessage(outEdge, message);
    }

    interface VertexMessage {
        String vertexId();
        long pathLength();
    }

    static class MessageFromSource implements VertexMessage {
        private final String vertexId;
        private final long pathLength;

        MessageFromSource(String vertexId, long pathLength) {
            this.vertexId = vertexId;
            this.pathLength = pathLength;
        }

        @Override
        public String vertexId() {
            return vertexId;
        }

        @Override
        public long pathLength() {
            return pathLength;
        }

        @Override
        public String toString() {
            return "FromSourceMessage(vertexId=" + vertexId + ", pathLength=" + pathLength + ")";
        }
    }

    static class MessageFromDestination implements VertexMessage {
        private final String vertexId;
        private final long pathLength;

        MessageFromDestination(String vertexId, long pathLength) {
            this.vertexId = vertexId;
            this.pathLength = pathLength;
        }

        @Override
        public String vertexId() {
            return vertexId;
        }

        @Override
        public long pathLength() {
            return pathLength;
        }

        @Override
        public String toString() {
            return "FromDestinationMessage(vertexId=" + vertexId + ", pathLength=" + pathLength + ")";
        }
    }
}