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

import ai.grakn.util.CommonUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Set;

/**
 * The vertex program for counting concepts.
 * <p>
 *
 * @author Jason Liu
 */

public class CountVertexProgram extends GraknVertexProgram<Long> {

    // element key
    public static final String EDGE_COUNT = "countVertexProgram.edgeCount";

    private String edgeCountPropertyKey;

    // Needed internally for OLAP tasks
    public CountVertexProgram() {
    }

    public CountVertexProgram(String randomId) {
        this.edgeCountPropertyKey = EDGE_COUNT + randomId;
        this.persistentProperties.put(EDGE_COUNT, edgeCountPropertyKey);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        edgeCountPropertyKey = (String) this.persistentProperties.get(EDGE_COUNT);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return Collections.singleton(VertexComputeKey.of(edgeCountPropertyKey, false));
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return memory.isInitialIteration() ? messageScopeSetShortcut : Collections.emptySet();
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                messenger.sendMessage(messageScopeOut, 1L);
                break;
            case 1:
                if (messenger.receiveMessages().hasNext()) {
                    vertex.property(edgeCountPropertyKey, getMessageCount(messenger));
                }
                break;
            default:
                throw CommonUtil.unreachableStatement("Exceeded expected maximum number of iterations");
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Count Iteration " + memory.getIteration());
        return memory.getIteration() == 1;
    }
}
