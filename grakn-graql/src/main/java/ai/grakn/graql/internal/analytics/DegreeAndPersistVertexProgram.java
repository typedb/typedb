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

import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

class DegreeAndPersistVertexProgram extends GraknVertexProgram<Long> {

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
                if (selectedTypes.contains(Utility.getVertexType(vertex)))
                    degreeStep0(vertex, messenger);
                break;

            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name()))
                    degreeStep1(messenger);
                break;

            case 2:
                if (selectedTypes.contains(Utility.getVertexType(vertex)))
                    bulkResourceMutate.putValue(vertex, getEdgeCount(messenger));
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
        LOGGER.info("Finished Iteration " + memory.getIteration());
        return memory.getIteration() == 2;
    }

}
