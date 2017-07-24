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
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.Utility.vertexHasSelectedTypeId;

/**
 * The vertex program for computing the degree in statistics.
 * <p>
 *
 * @author Jason Liu
 */

public class DegreeStatisticsVertexProgram extends DegreeVertexProgram {

    // Needed internally for OLAP tasks
    public DegreeStatisticsVertexProgram() {
    }

    public DegreeStatisticsVertexProgram(Set<LabelId> types, Set<LabelId> ofLabelIDs, String randomId) {
        super(types, ofLabelIDs, randomId);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                degreeStatisticsStepResourceOwner(vertex, messenger, selectedTypes, ofLabelIds);
                break;
            case 1:
                degreeStatisticsStepResourceRelation(vertex, messenger);
                break;
            case 2:
                degreeStatisticsStepResource(vertex, messenger, ofLabelIds, degreePropertyKey);
                break;
            default:
                throw CommonUtil.unreachableStatement("Exceeded expected maximum number of iterations");
        }
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                return Collections.singleton(messageScopeShortcutIn);
            case 1:
                return Collections.singleton(messageScopeShortcutOut);
            default:
                return Collections.emptySet();
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Degree Iteration " + memory.getIteration());
        return memory.getIteration() == 2;
    }

    static void degreeStatisticsStepResourceOwner(Vertex vertex, Messenger<Long> messenger,
                                                  Set<LabelId> selectedLabelIds, Set<LabelId> ofLabelIds) {
        LabelId labelId = Utility.getVertexTypeId(vertex);
        if (labelId.isValid() && selectedLabelIds.contains(labelId) && !ofLabelIds.contains(labelId)) {
            messenger.sendMessage(messageScopeShortcutIn, 1L);
        }
    }

    static void degreeStatisticsStepResourceRelation(Vertex vertex, Messenger<Long> messenger) {
        if (vertex.label().equals(Schema.BaseType.RELATION.name()) && messenger.receiveMessages().hasNext()) {
            messenger.sendMessage(messageScopeShortcutOut, 1L);
        }
    }

    static void degreeStatisticsStepResource(Vertex vertex, Messenger<Long> messenger,
                                             Set<LabelId> ofLabelIds, String degree) {
        if (vertexHasSelectedTypeId(vertex, ofLabelIds)) {
            vertex.property(degree, getMessageCount(messenger));
        }
    }
}
