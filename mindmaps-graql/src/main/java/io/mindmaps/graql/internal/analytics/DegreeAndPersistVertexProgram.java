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

public class DegreeAndPersistVertexProgram implements VertexProgram<Long> {

    private final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
    private final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);
    private MindmapsGraph mindmapsGraph;

    public static final String DEGREE = "analytics.degreeAndPersistVertexProgram.degree";

    public static final String OLD_ASSERTION_ID = "analytics.degreeAndPersistVertexProgram.oldAssertionId";

    private static final String TRAVERSAL_SUPPLIER = "analytics.degreeAndPersistVertexProgram.traversalSupplier";

    private static final String KEYSPACE = "analytics.degreeAndPersistVertexProgram.keySpace";

    private ConfigurationTraversal<Vertex, Edge> configurationTraversal;

    private static final Set<String> COMPUTE_KEYS = Collections.singleton(OLD_ASSERTION_ID);

    private final HashSet<String> baseTypes = Sets.newHashSet(
            Schema.BaseType.ENTITY.name(),
            Schema.BaseType.RESOURCE.name());

    private Set<String> selectedTypes = null;

    private String keySpace;

    public DegreeAndPersistVertexProgram() {
    }

    public DegreeAndPersistVertexProgram(String keySpace, Set<String> types) {
        this.keySpace = keySpace;
        selectedTypes = types;
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.selectedTypes = new HashSet<>();
        configuration.getKeys(TYPE).forEachRemaining(key -> selectedTypes.add(configuration.getString(key)));
        keySpace = configuration.getString(KEYSPACE);
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, DegreeAndPersistVertexProgram.class.getName());
        Iterator iterator = selectedTypes.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            configuration.addProperty(TYPE + "." + count, iterator.next());
            count++;
        }
        configuration.setProperty(KEYSPACE,keySpace);
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
    public DegreeAndPersistVertexProgram clone() {
        try {
            final DegreeAndPersistVertexProgram clone = (DegreeAndPersistVertexProgram) super.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(ErrorMessage.CLONE_FAILED.getMessage(this.getClass().toString(),e.getMessage()),e);
        }
    }

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void execute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        MindmapsGraph mindmapsGraph = MindmapsClient.getGraphBatchLoading(keySpace);
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(getVertexType(vertex)) && !isAnalyticsElement(vertex)) {
                    if (baseTypes.contains(vertex.label())) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                    } else if (vertex.label().equals(Schema.BaseType.RELATION.name())) {
                        messenger.sendMessage(this.countMessageScopeOut, -1L);
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
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
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                        messenger.sendMessage(this.countMessageScopeOut, assertionCount);
                    }
                }
                break;
            case 2:
                if (!isAnalyticsElement(vertex) && selectedTypes.contains(getVertexType(vertex))) {
                    if (baseTypes.contains(vertex.label()) ||
                            vertex.label().equals(Schema.BaseType.RELATION.name())) {
                        long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                        String oldAssertionId = Analytics.persistResource(keySpace, vertex, Analytics.degree, edgeCount);
                        if (oldAssertionId != null) {
                            vertex.property(OLD_ASSERTION_ID, oldAssertionId);
                        }
                    }
                }
                break;
            case 3:
                if(vertex.property(DegreeAndPersistVertexProgram.OLD_ASSERTION_ID).isPresent()) {
                    mindmapsGraph.getRelation(vertex.value(DegreeAndPersistVertexProgram.OLD_ASSERTION_ID)).delete();
                    try {
                        mindmapsGraph.commit();
                    } catch (MindmapsValidationException e) {
                        throw new RuntimeException("Failed to delete relation during bulk resource mutation.",e);
                    }
                }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() == 3;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this);
    }

}
