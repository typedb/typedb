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

import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public class MedianVertexProgram extends MindmapsVertexProgram<Long> {

    private final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
    private final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);

    public static final String DEGREE = "degree";
    public static final int MAX_ITERATION = 100;

    private static final Set<String> ELEMENT_COMPUTE_KEYS = Collections.singleton(DEGREE);

    public MedianVertexProgram() {
    }

    public MedianVertexProgram(Set<String> types) {
        selectedTypes = types;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return ELEMENT_COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> set = new HashSet<>();
        set.add(this.countMessageScopeOut);
        set.add(this.countMessageScopeIn);
        return set;
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(getVertexType(vertex))) {
                    String type = vertex.value(Schema.ConceptProperty.BASE_TYPE.name());
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                        messenger.sendMessage(this.countMessageScopeOut, -1L);
                    }
                }
                break;
            case 1:
                String type = vertex.value(Schema.ConceptProperty.BASE_TYPE.name());
                if (type.equals(Schema.BaseType.CASTING.name())) {
                    boolean hasRolePlayer = false;
                    long assertionCount = 0;
                    Iterator<Long> iterator = messenger.receiveMessages();
                    while (iterator.hasNext()) {
                        long message = iterator.next();
                        if (message < 0) assertionCount++;
                        else hasRolePlayer = true;
                    }
                    if (hasRolePlayer) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                        messenger.sendMessage(this.countMessageScopeOut, assertionCount);
                    }
                }
                break;
            case 2:
                if (selectedTypes.contains(getVertexType(vertex))) {
                    if (baseTypes.contains(vertex.value(Schema.ConceptProperty.BASE_TYPE.name()).toString())) {
                        long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                        vertex.property(DEGREE, edgeCount);
                    }
                }
                break;
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() == 2;
    }

}
