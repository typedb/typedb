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
import io.mindmaps.MindmapsGraph;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.util.Schema;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.concept.Type;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.ConfigurationTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.analytics.Analytics.TYPE;
import static io.mindmaps.graql.internal.analytics.Analytics.getVertexType;
import static io.mindmaps.graql.internal.analytics.Analytics.isAnalyticsElement;

public class DegreeAndPersistVertexProgram extends MindmapsVertexProgram<Long> {


    public static final String MEMORY_KEY = "oldAssertionId";

    private static final String KEYSPACE_KEY = "keyspace";

    private static final Set<String> COMPUTE_KEYS = Collections.singleton(MEMORY_KEY);

    private String keySpace;

    public DegreeAndPersistVertexProgram() {
    }

    public DegreeAndPersistVertexProgram(String keySpace, Set<String> types) {
        persistentProperties.put(KEYSPACE_KEY,keySpace);
        selectedTypes = types;
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        keySpace = (String) persistentProperties.get(KEYSPACE_KEY);
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return COMPUTE_KEYS;
    }

    @Override
    public void execute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(getVertexType(vertex)) && !isAnalyticsElement(vertex)) {
                    String type = vertex.label();
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        messenger.sendMessage(countMessageScopeIn, 1L);
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        messenger.sendMessage(countMessageScopeOut, -1L);
                        messenger.sendMessage(countMessageScopeIn, 1L);
                    }
                }
                break;
            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name())) {
                    boolean hasRolePlayer = false;
                    long assertionCount = 0;
                    Iterator<Long> iterator = messenger.receiveMessages();
                    while (iterator.hasNext()) {
                        long message = iterator.next();
                        if (message < 0) assertionCount++;
                        else hasRolePlayer = true;
                    }
                    if (hasRolePlayer) {
                        messenger.sendMessage(countMessageScopeIn, 1L);
                        messenger.sendMessage(countMessageScopeOut, assertionCount);
                    }
                }
                break;
            case 2:
                if (!isAnalyticsElement(vertex) && selectedTypes.contains(getVertexType(vertex))) {
                    if (baseTypes.contains(vertex.label())) {
                        long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                        String oldAssertionId = Analytics.persistResource(keySpace, vertex, Analytics.degree, edgeCount);
                        if (oldAssertionId != null) {
                            vertex.property(MEMORY_KEY, oldAssertionId);
                        }
                    }
                }
                break;
            case 3:
                if(vertex.property(DegreeAndPersistVertexProgram.MEMORY_KEY).isPresent()) {
                    mindmapsGraph.getRelation(vertex.value(DegreeAndPersistVertexProgram.MEMORY_KEY)).delete();
                    try {
                        mindmapsGraph.commit();
                    } catch (MindmapsValidationException e) {
                        throw new RuntimeException("Failed to delete relation during bulk resource mutation.", e);
                    }
                }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() == 3;
    }

}
