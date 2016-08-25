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
import io.mindmaps.core.model.Type;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.ConfigurationTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.relaxng.datatype.Datatype;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.analytics.Analytics.*;

/**
 *
 */

public class DegreeVertexProgram implements VertexProgram<Long> {

    private final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
    private final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);

    public static final String DEGREE = "analytics.degreeVertexProgram.degree";

    private static final String TRAVERSAL_SUPPLIER = "analytics.degreeVertexProgram.traversalSupplier";

    private ConfigurationTraversal<Vertex, Edge> configurationTraversal;

    private static final Set<String> COMPUTE_KEYS = new HashSet<>(Arrays.asList(DEGREE));

    private HashSet<String> baseTypes = Sets.newHashSet(
            DataType.BaseType.ENTITY.name(),
//            DataType.BaseType.RELATION.name(),
            DataType.BaseType.RESOURCE.name());

    private Set<String> selectedTypes = null;

    public DegreeVertexProgram() {
    }

    public DegreeVertexProgram(Set<Type> types) {
        selectedTypes = types.stream().map(Type::getId).collect(Collectors.toSet());
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.selectedTypes = new HashSet<>();
        configuration.getKeys(TYPE).forEachRemaining(key -> selectedTypes.add(configuration.getString(key)));
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, DegreeVertexProgram.class.getName());
        Iterator iterator = selectedTypes.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            configuration.addProperty(TYPE + "." + count, iterator.next());
            count++;
        }
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
        switch (memory.getIteration()) {
            case 0:
                if (baseTypes.contains(vertex.label())) {
                    if (selectedTypes.contains(getVertextType(vertex)) && !isAnalyticsElement(vertex)) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                        System.out.println("base type, step 1");
                    }
//                } else if (vertex.label().equals(DataType.BaseType.ROLE_TYPE.name())) {
//                    System.out.println("role type, step 1");
//                    messenger.sendMessage(this.countMessageScopeIn, 1L);
                }
                break;
            case 1:
                if (vertex.label().equals(DataType.BaseType.CASTING.name())) {
                    long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                    if (edgeCount > 0) {
                        messenger.sendMessage(this.countMessageScopeIn, -1L);
                        System.out.println("casting type step 2");
                    }
//                } else if (vertex.label().equals(DataType.BaseType.RELATION_TYPE.name())) {
//                    long roleCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
//                    messenger.sendMessage(this.countMessageScopeIn, roleCount);
//                    System.out.println("relation type step 2, role count = " + roleCount);
                }
                break;
            case 2:
                if (vertex.label().equals(DataType.BaseType.RELATION.name()) &&
                        selectedTypes.contains(getVertextType(vertex)) &&
                        !isAnalyticsElement(vertex)) {
                    messenger.sendMessage(this.countMessageScopeOut, 1L);
                    long roleCount = 0;
                    long rolePlayerCount = 0;
                    Iterator<Long> iterator = messenger.receiveMessages();
                    while (iterator.hasNext()) {
                        long message = iterator.next();
                        if (message > 0) roleCount = message;
                        else rolePlayerCount++;
                    }
//                    vertex.property(DEGREE, Math.min(roleCount, rolePlayerCount));
                    vertex.property(DEGREE, rolePlayerCount);
                }
                break;
            case 3:
                if (vertex.label().equals(DataType.BaseType.CASTING.name())) {
                    long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                    messenger.sendMessage(this.countMessageScopeOut, edgeCount);
                }
                break;
            case 4:
                if (baseTypes.contains(vertex.label()) &&
                        !isAnalyticsElement(vertex)) {
                    long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                    vertex.property(DEGREE, edgeCount);
                }
                break;
        }

//
//        if (memory.getIteration() == 0) {
//            if (vertex.label().equals(DataType.BaseType.RELATION.name()) &&
//                    selectedTypes.contains(getVertextType(vertex)) &&
//                    !isAnalyticsElement(vertex)) {
//                messenger.sendMessage(this.countMessageScopeOut, 1L);
//            }
//        } else if (memory.getIteration() == 1) {
//            if (vertex.label().equals(DataType.BaseType.CASTING.name())) {
//                long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
//                messenger.sendMessage(this.countMessageScopeOut, edgeCount);
//                messenger.sendMessage(this.countMessageScopeIn, 1L);
//            }
//        } else if (memory.getIteration() == 2) {
//            if (baseTypes.contains(vertex.label()) &&
//                    !isAnalyticsElement(vertex)) {
//                long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
//                vertex.property(DEGREE, edgeCount);
//            }
//        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        System.out.println("memory.getIteration() = " + memory.getIteration());
        return memory.getIteration() == 4;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this);
    }

}
