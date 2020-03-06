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

import grakn.core.kb.graql.exception.GraqlQueryException;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Set;

/**
 * The vertex program for computing connected components.
 *
 */

public class ConnectedComponentsVertexProgram extends GraknVertexProgram<String> {

    private static final int MAX_ITERATION = 100;

    public static final String CLUSTER_LABEL = "connectedComponentVertexProgram.clusterLabel";
    private static final String VOTE_TO_HALT = "connectedComponentVertexProgram.voteToHalt";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS =
            Collections.singleton(MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));

    public ConnectedComponentsVertexProgram() {
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return Collections.singleton(VertexComputeKey.of(CLUSTER_LABEL, false));
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("ConnectedComponentsVertexProgram Started !!!!!!!!");
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<String> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            String id = vertex.id().toString();
            vertex.property(CLUSTER_LABEL, id);
            messenger.sendMessage(messageScopeIn, id);
            messenger.sendMessage(messageScopeOut, id);
        } else {
            updateClusterLabel(vertex, messenger, memory);
        }
    }

    private static void updateClusterLabel(Vertex vertex, Messenger<String> messenger, Memory memory) {
        String currentMax = vertex.value(CLUSTER_LABEL);
        String max = IteratorUtils.reduce(messenger.receiveMessages(), currentMax,
                (a, b) -> a.compareTo(b) > 0 ? a : b);
        if (max.compareTo(currentMax) > 0) {
            vertex.property(CLUSTER_LABEL, max);
            messenger.sendMessage(messageScopeIn, max);
            messenger.sendMessage(messageScopeOut, max);
            memory.add(VOTE_TO_HALT, false);
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration {}", memory.getIteration());
        if (memory.getIteration() < 2) return false;
        if (memory.<Boolean>get(VOTE_TO_HALT)) {
            return true;
        }
        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: {}", MAX_ITERATION);
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        memory.set(VOTE_TO_HALT, true);
        return false;
    }

}