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

import ai.grakn.concept.TypeName;
import ai.grakn.util.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Set;

public class DegreeAndPersistVertexProgram extends GraknVertexProgram<Long> {

    private static final String KEYSPACE_KEY = "degreeAndPersistVertexProgram.keyspace";
    private static final String OF_TYPE_NAMES = "degreeAndPersistVertexProgram.ofTypeNames";
    private static final String DEGREE_NAME = "degreeAndPersistVertexProgram.degreeName";

    private BulkResourceMutate<Long> bulkResourceMutate;
    private Set<TypeName> ofTypeNames = new HashSet<>();

    public DegreeAndPersistVertexProgram() {
    }

    public DegreeAndPersistVertexProgram(Set<TypeName> types, Set<TypeName> ofTypeNames,
                                         String keySpace, TypeName degreeName) {
        this.persistentProperties.put(KEYSPACE_KEY, keySpace);
        this.persistentProperties.put(DEGREE_NAME, degreeName);
        this.selectedTypes = types;
        this.ofTypeNames = ofTypeNames;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        ofTypeNames.forEach(type -> configuration.addProperty(OF_TYPE_NAMES + "." + type, type));
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        configuration.subset(OF_TYPE_NAMES).getKeys().forEachRemaining(key ->
                ofTypeNames.add(TypeName.of((String) configuration.getProperty(OF_TYPE_NAMES + "." + key))));
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    degreeStep0(vertex, messenger);
                }
                break;

            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name())) {
                    degreeStep1(messenger);
                }
                break;

            case 2:
                String type = Utility.getVertexType(vertex);
                if (selectedTypes.contains(type) && ofTypeNames.contains(type)) {
                    bulkResourceMutate.putValue(vertex, getEdgeCount(messenger));
                }
                break;
        }
    }

    @Override
    public void workerIterationStart(Memory memory) {
        if (memory.getIteration() == 2) {
            bulkResourceMutate = new BulkResourceMutate<>((String) persistentProperties.get(KEYSPACE_KEY),
                    TypeName.of((String) persistentProperties.get(DEGREE_NAME)));
        }
    }

    @Override
    public void workerIterationEnd(Memory memory) {
        if (memory.getIteration() == 2) {
            bulkResourceMutate.flush();
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration " + memory.getIteration());
        return memory.getIteration() == 2;
    }

}
