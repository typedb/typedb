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

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.util.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
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

    public static final String CLUSTER_LABEL = "connectedComponentVertexProgram.clusterLabel";
    private static final String VOTE_TO_HALT = "connectedComponentVertexProgram.voteToHalt";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS =
            Collections.singleton(MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));

    private String clusterLabel;

    public ConnectedComponentVertexProgram() {
    }

    public ConnectedComponentVertexProgram(String randomId) {
        clusterLabel = CLUSTER_LABEL + randomId;
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        this.clusterLabel = (String) this.persistentProperties.get(CLUSTER_LABEL);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return Collections.singleton(VertexComputeKey.of(clusterLabel, false));
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
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
                String id = vertex.value(Schema.VertexProperty.ID.name());
                vertex.property(clusterLabel, id);
                messenger.sendMessage(messageScopeIn, id);
                messenger.sendMessage(messageScopeOut, id);
                break;
            default:
                update(vertex, messenger, memory);
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
            memory.add(VOTE_TO_HALT, false);
            memory.getIteration();
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
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        memory.set(VOTE_TO_HALT, true);
        return false;
    }

}