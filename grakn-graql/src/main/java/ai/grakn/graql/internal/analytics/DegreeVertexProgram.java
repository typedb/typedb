/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.analytics;

import ai.grakn.concept.LabelId;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.Utility.vertexHasSelectedTypeId;

/**
 * The vertex program for computing the degree.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class DegreeVertexProgram extends GraknVertexProgram<Long> {

    public static final String DEGREE = "degreeVertexProgram.degree";
    private static final String OF_LABELS = "degreeVertexProgram.ofLabelIds";

    Set<LabelId> ofLabelIds = new HashSet<>();

    // Needed internally for OLAP tasks
    public DegreeVertexProgram() {
    }

    public DegreeVertexProgram(Set<LabelId> ofLabelIds) {
        this.ofLabelIds = ofLabelIds;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        ofLabelIds.forEach(type -> configuration.addProperty(OF_LABELS + "." + type, type));
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        configuration.subset(OF_LABELS).getKeys().forEachRemaining(key ->
                ofLabelIds.add((LabelId) configuration.getProperty(OF_LABELS + "." + key)));
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return Collections.singleton(VertexComputeKey.of(DEGREE, false));
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return memory.isInitialIteration() ?
                Sets.newHashSet(messageScopeResourceIn, messageScopeOut) : Collections.emptySet();
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                degreeMessagePassing(messenger);
                break;
            case 1:
                degreeMessageCounting(messenger, vertex);
                break;
            default:
                throw CommonUtil.unreachableStatement("Exceeded expected maximum number of iterations");
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Degree Iteration " + memory.getIteration());
        return memory.getIteration() == 1;
    }

    private void degreeMessagePassing(Messenger<Long> messenger) {
        messenger.sendMessage(messageScopeResourceIn, 1L);
        messenger.sendMessage(messageScopeOut, 1L);
    }

    private void degreeMessageCounting(Messenger<Long> messenger, Vertex vertex) {
        if (messenger.receiveMessages().hasNext() &&
                (ofLabelIds.isEmpty() || vertexHasSelectedTypeId(vertex, ofLabelIds))) {
            vertex.property(DEGREE, getMessageCount(messenger));
        }
    }
}
