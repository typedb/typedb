/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.analytics;

import com.google.common.collect.Sets;
import io.mindmaps.constants.DataType;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.ConfigurationTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */

public class DegreeVertexProgram implements VertexProgram<Long> {

    private MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
    private MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);

    public static final String DEGREE_VALUE_TYPE = DataType.ConceptProperty.VALUE_LONG.name();
    private HashSet<String> baseTypes = Sets.newHashSet(
            DataType.BaseType.ENTITY.name(),
            DataType.BaseType.RELATION.name(),
            DataType.BaseType.RESOURCE.name());

    private static final String TRAVERSAL_SUPPLIER = "analytics.degreeVertexProgram.traversalSupplier";

    private ConfigurationTraversal<Vertex, Edge> configurationTraversal;

    private static final Set<String> COMPUTE_KEYS = new HashSet<>(Arrays.asList(DEGREE_VALUE_TYPE));

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, DegreeVertexProgram.class.getName());
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> set = new HashSet<>();
        set.add(this.countMessageScopeOut);
        set.add(this.countMessageScopeIn);
        return set;
    }

    @Override
    public DegreeVertexProgram clone() {
        try {
            final DegreeVertexProgram clone = (DegreeVertexProgram) super.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void execute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            if (vertex.label().equals(DataType.BaseType.RELATION.name())) {
                messenger.sendMessage(this.countMessageScopeOut, 1L);
            }
        } else if (memory.getIteration() == 1) {
            if (vertex.label().equals(DataType.BaseType.CASTING.name())) {
                long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                messenger.sendMessage(this.countMessageScopeOut, edgeCount);
                messenger.sendMessage(this.countMessageScopeIn, 1L);
            }
        } else if (memory.getIteration() == 2) {
            if (baseTypes.contains(vertex.label())) {
                long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                vertex.property(DEGREE_VALUE_TYPE, edgeCount);
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() == 2;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this);
    }

}
