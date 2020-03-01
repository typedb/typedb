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

import com.google.common.collect.Sets;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.LabelId;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Set;

import static grakn.core.graql.analytics.Utility.vertexHasSelectedTypeId;

/**
 * The vertex program for computing the degree in statistics.
 * <p>
 *
 */

public class DegreeStatisticsVertexProgram extends DegreeVertexProgram {

    @SuppressWarnings("unused")// Needed internally for OLAP tasks
    public DegreeStatisticsVertexProgram() {
    }

    public DegreeStatisticsVertexProgram(Set<LabelId> ofLabelIDs) {
        super(ofLabelIDs);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                degreeStatisticsStepResourceOwner(vertex, messenger, ofLabelIds);
                break;
            case 1:
                degreeStatisticsStepResourceRelation(vertex, messenger, ofLabelIds);
                break;
            case 2:
                degreeStatisticsStepResource(vertex, messenger, ofLabelIds);
                break;
            default:
                throw GraknAnalyticsException.unreachableStatement("Exceeded expected maximum number of iterations");
        }
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                return Sets.newHashSet(messageScopeShortcutIn, messageScopeResourceOut);
            case 1:
                return Collections.singleton(messageScopeShortcutOut);
            default:
                return Collections.emptySet();
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Degree Iteration {}", memory.getIteration());
        return memory.getIteration() == 2;
    }

    static void degreeStatisticsStepResourceOwner(Vertex vertex, Messenger<Long> messenger, Set<LabelId> ofLabelIds) {
        LabelId labelId = Utility.getVertexTypeId(vertex);
        if (labelId.isValid() && !ofLabelIds.contains(labelId)) {
            messenger.sendMessage(messageScopeShortcutIn, 1L);
            messenger.sendMessage(messageScopeResourceOut, 1L);
        }
    }

    static void degreeStatisticsStepResourceRelation(Vertex vertex, Messenger<Long> messenger, Set<LabelId> ofLabelIds) {
        if (messenger.receiveMessages().hasNext()) {
            if (vertex.label().equals(Schema.BaseType.RELATION.name())) {
                messenger.sendMessage(messageScopeOut, 1L);
            } else if (ofLabelIds.contains(Utility.getVertexTypeId(vertex))) {
                vertex.property(DEGREE, getMessageCount(messenger));
            }
        }
    }

    static void degreeStatisticsStepResource(Vertex vertex, Messenger<Long> messenger,
                                             Set<LabelId> ofLabelIds) {
        if (vertexHasSelectedTypeId(vertex, ofLabelIds)) {
            vertex.property(DEGREE, vertex.property(DEGREE).isPresent() ?
                    getMessageCount(messenger) + (Long) vertex.value(DEGREE) : getMessageCount(messenger));
        }
    }
}
