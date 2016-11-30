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

import com.google.common.collect.Sets;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ConnectedComponentVertexProgram extends GraknVertexProgram<String> {

    private static final int MAX_ITERATION = 100;
    private static final String MIN_STRING = "0";
    // element key
    private static final String IS_ACTIVE_CASTING = "connectedComponentVertexProgram.isActiveCasting";
    protected static final String CLUSTER_LABEL = "connectedComponentVertexProgram.clusterLabel";

    // memory key
    private static final String VOTE_TO_HALT = "connectedComponentVertexProgram.voteToHalt";
    private static final String IS_LAST_ITERATION = "connectedComponentVertexProgram.isLastIteration";

    private static final Set<String> ELEMENT_COMPUTE_KEYS = Sets.newHashSet(IS_ACTIVE_CASTING, CLUSTER_LABEL);
    private static final Set<String> MEMORY_COMPUTE_KEYS = Sets.newHashSet(VOTE_TO_HALT, IS_LAST_ITERATION);

    private static final String MESSAGE_FROM_ROLE_PLAYER = "R";
    private static final String MESSAGE_FROM_ASSERTION = "A";

    private static final String PERSIST = "connectedComponentVertexProgram.persist";
    private static final String KEYSPACE = "connectedComponentVertexProgram.keyspace";

    private BulkResourceMutate bulkResourceMutate;

    public ConnectedComponentVertexProgram() {
    }

    public ConnectedComponentVertexProgram(Set<String> selectedTypes) {
        this.selectedTypes = selectedTypes;
        this.persistentProperties.put(PERSIST, false);
    }

    public ConnectedComponentVertexProgram(Set<String> selectedTypes, String keyspace) {
        this.selectedTypes = selectedTypes;
        this.persistentProperties.put(PERSIST, true);
        this.persistentProperties.put(KEYSPACE, keyspace);
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
        LOGGER.debug("ConnectedComponentVertexProgram Started !!!!!!!!");
        memory.set(VOTE_TO_HALT, true);
        memory.set(IS_LAST_ITERATION, false);
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
                }
                break;
            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name())) {
                    Set<String> messageSet = new HashSet<>();
                    boolean hasBothMessages = false;
                    Iterator<String> iterator = messenger.receiveMessages();
                    while (iterator.hasNext()) {
                        messageSet.add(iterator.next());
                        if (messageSet.size() == 2) {
                            hasBothMessages = true;
                            break;
                        }
                    }
                    // casting is active if both its assertion and role-player is in the subgraph
                    vertex.property(IS_ACTIVE_CASTING, hasBothMessages);
                } else if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    String id = vertex.id().toString();
                    vertex.property(CLUSTER_LABEL, id);
                    messenger.sendMessage(messageScopeIn, id);
                    messenger.sendMessage(messageScopeOut, id);
                }
                break;
            case 2:
                //similar to default case, except that casting has no cluster label before this iteration
                if (vertex.label().equals(Schema.BaseType.CASTING.name()) &&
                        (boolean) vertex.value(IS_ACTIVE_CASTING)) {
                    String max = IteratorUtils.reduce(messenger.receiveMessages(), MIN_STRING,
                            (a, b) -> a.compareTo(b) > 0 ? a : b);
                    vertex.property(CLUSTER_LABEL, max);
                    messenger.sendMessage(messageScopeIn, max);
                    messenger.sendMessage(messageScopeOut, max);
                }
                break;
            default:
                if ((Boolean)memory.get(IS_LAST_ITERATION)) {
                    if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                        bulkResourceMutate.putValue(vertex, vertex.value(CLUSTER_LABEL));
                    }
                } else {
                    // split the default case because shortcut edges cannot be filtered out
                    if (memory.getIteration() % 2 == 1) {
                        if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                            update(vertex, messenger, memory);
                        }
                    } else {
                        if (vertex.label().equals(Schema.BaseType.CASTING.name()) &&
                                (boolean) vertex.value(IS_ACTIVE_CASTING)) {
                            update(vertex, messenger, memory);
                        }
                    }
                }
                break;
        }
    }

    private void update(Vertex vertex, Messenger<String> messenger, Memory memory) {
        String currentMax = vertex.value(CLUSTER_LABEL);
        String max = IteratorUtils.reduce(messenger.receiveMessages(), currentMax,
                (a, b) -> a.compareTo(b) > 0 ? a : b);
        if (max.compareTo(currentMax) > 0) {
            vertex.property(CLUSTER_LABEL, max);
            messenger.sendMessage(messageScopeIn, max);
            messenger.sendMessage(messageScopeOut, max);
            memory.and(VOTE_TO_HALT, false);
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.info("Finished Iteration " + memory.getIteration());
        if (memory.getIteration() < 3) return false;
        if ((Boolean)memory.get(IS_LAST_ITERATION)) return true;

        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        if (voteToHalt) {
            if (!(boolean) this.persistentProperties.get(PERSIST)) return true;
            LOGGER.debug("Persisting Resources ...");
            memory.set(IS_LAST_ITERATION, true);
            return false;
        }

        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: " + MAX_ITERATION + " !!!!!!!!");
            throw new IllegalStateException(ErrorMessage.MAX_ITERATION_REACHED
                    .getMessage(this.getClass().toString()));
        }

        memory.or(VOTE_TO_HALT, true);
        return false;
    }

    @Override
    public void workerIterationStart(Memory memory) {
        if ((boolean) this.persistentProperties.get(PERSIST) && (boolean) memory.get(IS_LAST_ITERATION)) {
            LOGGER.debug("Iteration " + memory.getIteration() + ", workerIterationStart");
            bulkResourceMutate = new BulkResourceMutate<Long>((String) persistentProperties.get(KEYSPACE),
                    Analytics.connectedComponent);
        }
    }

    @Override
    public void workerIterationEnd(Memory memory) {
        if ((boolean) this.persistentProperties.get(PERSIST) && (boolean) memory.get(IS_LAST_ITERATION)) {
            LOGGER.debug("Iteration " + memory.getIteration() + ", workerIterationEnd");
            bulkResourceMutate.flush();
        }
    }
}
