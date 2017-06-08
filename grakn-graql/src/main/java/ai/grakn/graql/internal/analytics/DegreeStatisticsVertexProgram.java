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

import ai.grakn.concept.TypeId;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Set;

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

    public DegreeStatisticsVertexProgram(Set<TypeId> types, Set<TypeId> ofTypeIDs, String randomId) {
        super(types, ofTypeIDs, randomId);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                degreeStatisticsStepResourceOwner(vertex, messenger, selectedTypes, ofTypeIds);
                break;
            case 1:
                degreeStatisticsStepResourceRelation(vertex, messenger);
                break;
            case 2:
                degreeStatisticsStepResource(vertex, messenger, ofTypeIds, degreePropertyKey);
                break;
            default:
                throw new RuntimeException("unreachable");
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
                                                  Set<TypeId> selectedTypeIds, Set<TypeId> ofTypeIds) {
        TypeId typeId = Utility.getVertexTypeId(vertex);
        if (selectedTypeIds.contains(typeId) && !ofTypeIds.contains(typeId)) {
            messenger.sendMessage(messageScopeShortcutIn, 1L);
        }
    }

    static void degreeStatisticsStepResourceRelation(Vertex vertex, Messenger<Long> messenger) {
        if (vertex.label().equals(Schema.BaseType.RELATION.name()) && messenger.receiveMessages().hasNext()) {
            messenger.sendMessage(messageScopeShortcutOut, 1L);
        }
    }

    static void degreeStatisticsStepResource(Vertex vertex, Messenger<Long> messenger,
                                             Set<TypeId> ofTypeIds, String degree) {
        if (ofTypeIds.contains(Utility.getVertexTypeId(vertex))) {
            vertex.property(degree, getMessageCount(messenger));
        }
    }
}
