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

import ai.grakn.concept.LabelId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.util.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
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
    // element key
    public static final String CLUSTER_LABEL = "connectedComponentVertexProgram.clusterLabel";

    // memory key
    private static final String VOTE_TO_HALT = "connectedComponentVertexProgram.voteToHalt";

    private static final Set<String> MEMORY_COMPUTE_KEYS = Collections.singleton(VOTE_TO_HALT);

    private String clusterLabel;

    public ConnectedComponentVertexProgram() {
    }

    public ConnectedComponentVertexProgram(Set<LabelId> selectedTypes, String randomId) {
        this.selectedTypes = selectedTypes;

        clusterLabel = CLUSTER_LABEL + randomId;
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        this.clusterLabel = (String) this.persistentProperties.get(CLUSTER_LABEL);
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return Collections.singleton(clusterLabel);
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
                if (selectedTypes.contains(Utility.getVertexTypeId(vertex))) {
                    String id = vertex.value(Schema.VertexProperty.ID.name());
                    vertex.property(clusterLabel, id);
                    messenger.sendMessage(messageScopeShortcutIn, id);
                    messenger.sendMessage(messageScopeShortcutOut, id);
                }
                break;
            default:
                if (selectedTypes.contains(Utility.getVertexTypeId(vertex))) {
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
            messenger.sendMessage(messageScopeShortcutIn, max);
            messenger.sendMessage(messageScopeShortcutOut, max);
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
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        memory.or(VOTE_TO_HALT, true);
        return false;
    }

}