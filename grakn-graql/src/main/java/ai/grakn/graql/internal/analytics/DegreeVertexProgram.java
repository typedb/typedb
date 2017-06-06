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
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The vertex program for computing the degree.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class DegreeVertexProgram extends GraknVertexProgram<Long> {

    // element key
    public static final String DEGREE = "degreeVertexProgram.degree";
    private static final String OF_TYPE_LABELS = "degreeVertexProgram.ofTypeIds";

    Set<TypeId> ofTypeIds = new HashSet<>();

    String degreePropertyKey;

    // Needed internally for OLAP tasks
    public DegreeVertexProgram() {
    }

    public DegreeVertexProgram(Set<TypeId> types, Set<TypeId> ofTypeIds, String randomId) {
        selectedTypes = types;
        degreePropertyKey = DEGREE + randomId;
        this.ofTypeIds = ofTypeIds;
        this.persistentProperties.put(DEGREE, degreePropertyKey);
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        ofTypeIds.forEach(type -> configuration.addProperty(OF_TYPE_LABELS + "." + type, type));
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        configuration.subset(OF_TYPE_LABELS).getKeys().forEachRemaining(key ->
                ofTypeIds.add(TypeId.of(configuration.getInt(OF_TYPE_LABELS + "." + key))));
        degreePropertyKey = (String) this.persistentProperties.get(DEGREE);
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return Collections.singleton(degreePropertyKey);
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return memory.isInitialIteration() ? messageScopeSetShortCut : Collections.emptySet();
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                degreeMessagePassing(vertex, messenger);
                break;
            case 1:
                degreeMessageCounting(vertex, messenger);
                break;
            default:
                throw new RuntimeException("unreachable");
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Degree Iteration " + memory.getIteration());
        return memory.getIteration() == 1;
    }

    void degreeMessagePassing(Vertex vertex, Messenger<Long> messenger) {
        if (selectedTypes.contains(Utility.getVertexTypeId(vertex))) {
            messenger.sendMessage(messageScopeShortCutIn, 1L);
            messenger.sendMessage(messageScopeShortCutOut, 1L);
        }
    }

    void degreeMessageCounting(Vertex vertex, Messenger<Long> messenger) {
        TypeId typeId = Utility.getVertexTypeId(vertex);
        if (selectedTypes.contains(typeId) && ofTypeIds.contains(typeId)) {
            vertex.property(degreePropertyKey, getMessageCount(messenger));
        }
    }
}
