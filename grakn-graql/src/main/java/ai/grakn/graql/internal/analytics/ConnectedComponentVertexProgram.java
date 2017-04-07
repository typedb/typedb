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

import ai.grakn.concept.TypeLabel;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * The vertex program for connected components in a graph.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class ConnectedComponentVertexProgram extends GraknVertexProgram<String> {

    private static final int MAX_ITERATION = 100;
    // element key
    private static final String IS_ACTIVE_CASTING = "connectedComponentVertexProgram.isActiveCasting";
    public static final String CLUSTER_LABEL = "connectedComponentVertexProgram.clusterLabel";

    // memory key
    private static final String VOTE_TO_HALT = "connectedComponentVertexProgram.voteToHalt";

    private static final Set<String> MEMORY_COMPUTE_KEYS = Sets.newHashSet(VOTE_TO_HALT);

    private static final String MESSAGE_FROM_ROLE_PLAYER = "R";
    private static final String MESSAGE_FROM_ASSERTION = "A";

    private String isActiveCasting;
    private String clusterLabel;

    public ConnectedComponentVertexProgram() {
    }

    public ConnectedComponentVertexProgram(Set<TypeLabel> selectedTypes, String randomId) {
        this.selectedTypes = selectedTypes;

        isActiveCasting = IS_ACTIVE_CASTING + randomId;
        clusterLabel = CLUSTER_LABEL + randomId;
        this.persistentProperties.put(IS_ACTIVE_CASTING, isActiveCasting);
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        this.isActiveCasting = (String) this.persistentProperties.get(IS_ACTIVE_CASTING);
        this.clusterLabel = (String) this.persistentProperties.get(CLUSTER_LABEL);
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return Sets.newHashSet(isActiveCasting, clusterLabel);
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("ConnectedComponentVertexProgram Started !!!!!!!!");
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<String> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    String type = vertex.label();
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        // each role-player sends 1 to castings following incoming edges
                        messenger.sendMessage(messageScopeInRolePlayer, MESSAGE_FROM_ROLE_PLAYER);
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        // the assertion can also be a role-player, so sending 1 to castings following incoming edges
                        messenger.sendMessage(messageScopeInRolePlayer, MESSAGE_FROM_ROLE_PLAYER);
                        // send -1 to castings following outgoing edges
                        messenger.sendMessage(messageScopeOutCasting, MESSAGE_FROM_ASSERTION);
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
                    vertex.property(isActiveCasting, hasBothMessages);
                    if (hasBothMessages) {
                        String id = vertex.id().toString();
                        vertex.property(clusterLabel, id);
                        messenger.sendMessage(messageScopeOutRolePlayer, id);
                        messenger.sendMessage(messageScopeInCasting, id);
                    }
                } else if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    String id = vertex.id().toString();
                    vertex.property(clusterLabel, id);
                    messenger.sendMessage(messageScopeInRolePlayer, id);
                    messenger.sendMessage(messageScopeOutCasting, id);
                }
                break;
            default:
                if (selectedTypes.contains(Utility.getVertexType(vertex)) ||
                        (vertex.label().equals(Schema.BaseType.CASTING.name()) &&
                                (boolean) vertex.value(isActiveCasting))) {
                    update(vertex, messenger, memory);
                }
                break;
        }
    }

    private void update(Vertex vertex, Messenger<String> messenger, Memory memory) {
        String currentMax = vertex.value(clusterLabel);
        String max = IteratorUtils.reduce(messenger.receiveMessages(), currentMax,
                (a, b) -> a.compareTo(b) > 0 ? a : b);
        if (max.compareTo(currentMax) > 0) {
            vertex.property(clusterLabel, max);
            messenger.sendMessage(messageScopeIn, max);
            messenger.sendMessage(messageScopeOut, max);
            memory.and(VOTE_TO_HALT, false);
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration " + memory.getIteration());
        if (memory.getIteration() < 2) return false;
        if (memory.<Boolean>get(VOTE_TO_HALT)) {
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