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
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.factory.MindmapsClient;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;


/**
 *
 */

public class DegreeAndPersistVertexProgram implements VertexProgram<Long> {

    private final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
    private final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);
    private MindmapsGraph mindmapsGraph;

//    public static final String DEGREE = "analytics.degreeAndPersistVertexProgram.degree";

    private final HashSet<String> baseTypes = Sets.newHashSet(
            DataType.BaseType.ENTITY.name(),
            DataType.BaseType.RELATION.name(),
            DataType.BaseType.RESOURCE.name());

//    private static final String TRAVERSAL_SUPPLIER = "analytics.degreeAndPersistVertexProgram.traversalSupplier";

//    private ConfigurationTraversal<Vertex, Edge> configurationTraversal;

//    private static final Set<String> COMPUTE_KEYS = new HashSet<>(Arrays.asList(DEGREE));

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, DegreeAndPersistVertexProgram.class.getName());
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return Collections.emptySet();
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> set = new HashSet<>();
        set.add(this.countMessageScopeOut);
        set.add(this.countMessageScopeIn);
        return set;
    }

    @Override
    public DegreeAndPersistVertexProgram clone() {
        try {
            final DegreeAndPersistVertexProgram clone = (DegreeAndPersistVertexProgram) super.clone();
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
            if (vertex.label().equals(DataType.BaseType.RELATION.name()) &&
                    !Analytics.isAnalyticsElement(vertex)) {
                messenger.sendMessage(this.countMessageScopeOut, 1L);
            }
        } else if (memory.getIteration() == 1) {
            if (vertex.label().equals(DataType.BaseType.CASTING.name())) {
                long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                messenger.sendMessage(this.countMessageScopeOut, edgeCount);
                messenger.sendMessage(this.countMessageScopeIn, 1L);
            }
        } else if (memory.getIteration() == 2) {
            if (baseTypes.contains(vertex.label()) &&
                    !Analytics.isAnalyticsElement(vertex)) {
                long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                Analytics.persistResource(mindmapsGraph, vertex, Analytics.degree, edgeCount);
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

    @Override
    public void workerIterationStart(Memory memory) {
        //TODO: wait for Filipe to provide a method in core so we don't specify keySpace
        mindmapsGraph = MindmapsClient.getGraph(Analytics.keySpace);
    }
}
