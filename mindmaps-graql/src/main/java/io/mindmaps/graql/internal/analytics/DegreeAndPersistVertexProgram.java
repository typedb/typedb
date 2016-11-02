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
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Set;

class DegreeAndPersistVertexProgram extends MindmapsVertexProgram<Long> {

    private static final String KEYSPACE_KEY = "keyspace";

    private BulkResourceMutate bulkResourceMutate;

    public DegreeAndPersistVertexProgram() {
    }

    public DegreeAndPersistVertexProgram(Set<String> types, String keySpace) {
        persistentProperties.put(KEYSPACE_KEY, keySpace);
        selectedTypes = types;
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(Utility.getVertexType(vertex)) && !Utility.isAnalyticsElement(vertex)) {
                    String type = vertex.label();
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        messenger.sendMessage(messageScopeIn, 1L);
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        messenger.sendMessage(messageScopeOut, -1L);
                        messenger.sendMessage(messageScopeIn, 1L);
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
                        messenger.sendMessage(messageScopeIn, 1L);
                        messenger.sendMessage(messageScopeOut, assertionCount);
                    }
                }
                break;
            case 2:
                if (!Utility.isAnalyticsElement(vertex) && selectedTypes.contains(Utility.getVertexType(vertex))) {
                    long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                    bulkResourceMutate.putValue(vertex, edgeCount);
                }
                break;
        }
    }

    @Override
    public void workerIterationStart(Memory memory) {
        bulkResourceMutate =
                new BulkResourceMutate<Long>((String) persistentProperties.get(KEYSPACE_KEY), Analytics.degree);
    }

    @Override
    public void workerIterationEnd(Memory memory) {
        bulkResourceMutate.flush();
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() == 2;
    }

}
