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
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
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

    // element key
    static final String VISITED = "degreeStatisticsVertexProgram.visited";
    private static final Set<String> ELEMENT_COMPUTE_KEYS = Sets.newHashSet(VISITED, DEGREE);

    // Needed internally for OLAP tasks
    public DegreeStatisticsVertexProgram() {
    }

    public DegreeStatisticsVertexProgram(Set<TypeLabel> types, Set<TypeLabel> ofTypeLabels) {
        super(types, ofTypeLabels);
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return ELEMENT_COMPUTE_KEYS;
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                degreeStatisticsStepInstance(vertex, messenger, selectedTypes, ofTypeLabels);
                break;
            case 1:
                degreeStatisticsStepCastingIn(vertex, messenger);
                break;
            case 2:
                degreeStatisticsStepRelation(vertex, messenger);
                break;
            case 3:
                degreeStatisticsStepCastingOut(vertex, messenger);
                break;
            case 4:
                degreeStatisticsStepResource(vertex, messenger, ofTypeLabels);
                break;
            default:
                throw new RuntimeException("unreachable");
        }
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                return Collections.singleton(messageScopeInRolePlayer);
            case 1:
                return Collections.singleton(messageScopeInCasting);
            case 2:
                return Collections.singleton(messageScopeOutCasting);
            case 3:
                return Collections.singleton(messageScopeOutRolePlayer);
            default:
                return Collections.emptySet();
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Degree Iteration " + memory.getIteration());
        return memory.getIteration() == 4;
    }

    static void degreeStatisticsStepInstance(Vertex vertex, Messenger<Long> messenger,
                                             Set<TypeLabel> selectedTypes, Set<TypeLabel> ofTypeLabels) {
        TypeLabel type = Utility.getVertexType(vertex);
        if (selectedTypes.contains(type) && !ofTypeLabels.contains(type)) {
            messenger.sendMessage(messageScopeInRolePlayer, 1L);
        }
    }

    static void degreeStatisticsStepCastingIn(Vertex vertex, Messenger<Long> messenger) {
        if (vertex.label().equals(Schema.BaseType.CASTING.name()) && messenger.receiveMessages().hasNext()) {
            vertex.property(VISITED, true);
            messenger.sendMessage(messageScopeInCasting, 1L);
        }
    }

    static void degreeStatisticsStepRelation(Vertex vertex, Messenger<Long> messenger) {
        if (vertex.label().equals(Schema.BaseType.RELATION.name()) && messenger.receiveMessages().hasNext()) {
            messenger.sendMessage(messageScopeOutCasting, 1L);
        }
    }

    static void degreeStatisticsStepCastingOut(Vertex vertex, Messenger<Long> messenger) {
        if (vertex.label().equals(Schema.BaseType.CASTING.name()) && !vertex.property(VISITED).isPresent()
                && messenger.receiveMessages().hasNext()) {
            messenger.sendMessage(messageScopeOutRolePlayer, getMessageCount(messenger));
        }
    }

    static void degreeStatisticsStepResource(Vertex vertex, Messenger<Long> messenger, Set<TypeLabel> ofTypeLabels) {
        if (ofTypeLabels.contains(Utility.getVertexType(vertex))) {
            vertex.property(DEGREE, getMessageCount(messenger));
        }
    }
}
