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
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Graph;
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

    private String visited;

    // Needed internally for OLAP tasks
    public DegreeStatisticsVertexProgram() {
    }

    public DegreeStatisticsVertexProgram(Set<TypeId> types, Set<TypeId> ofTypeIDs, String randomId) {
        super(types, ofTypeIDs, randomId);
        visited = VISITED + randomId;
        this.persistentProperties.put(VISITED, visited);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        visited = (String) this.persistentProperties.get(VISITED);
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return Sets.newHashSet(visited, degree);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                degreeStatisticsStepInstance(vertex, messenger, selectedTypes, ofTypeIds);
                break;
            case 1:
                degreeStatisticsStepCastingIn(vertex, messenger, visited);
                break;
            case 2:
                degreeStatisticsStepRelation(vertex, messenger);
                break;
            case 3:
                degreeStatisticsStepCastingOut(vertex, messenger, visited);
                break;
            case 4:
                degreeStatisticsStepResource(vertex, messenger, ofTypeIds, degree);
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
                                             Set<TypeId> selectedTypeIds, Set<TypeId> ofTypeIds) {
        TypeId typeId = Utility.getVertexTypeId(vertex);
        if (selectedTypeIds.contains(typeId) && !ofTypeIds.contains(typeId)) {
            messenger.sendMessage(messageScopeInRolePlayer, 1L);
        }
    }

    static void degreeStatisticsStepCastingIn(Vertex vertex, Messenger<Long> messenger, String visited) {
        if (vertex.label().equals(Schema.BaseType.CASTING.name()) && messenger.receiveMessages().hasNext()) {
            vertex.property(visited, true);
            messenger.sendMessage(messageScopeInCasting, 1L);
        }
    }

    static void degreeStatisticsStepRelation(Vertex vertex, Messenger<Long> messenger) {
        if (vertex.label().equals(Schema.BaseType.RELATION.name()) && messenger.receiveMessages().hasNext()) {
            messenger.sendMessage(messageScopeOutCasting, 1L);
        }
    }

    static void degreeStatisticsStepCastingOut(Vertex vertex, Messenger<Long> messenger, String visited) {
        if (vertex.label().equals(Schema.BaseType.CASTING.name()) && !vertex.property(visited).isPresent()
                && messenger.receiveMessages().hasNext()) {
            messenger.sendMessage(messageScopeOutRolePlayer, getMessageCount(messenger));
        }
    }

    static void degreeStatisticsStepResource(Vertex vertex, Messenger<Long> messenger,
                                             Set<TypeId> ofTypeIds, String degree) {
        if (ofTypeIds.contains(Utility.getVertexTypeId(vertex))) {
            vertex.property(degree, getMessageCount(messenger));
        }
    }
}
