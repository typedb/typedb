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

import java.util.Collections;
import java.util.Set;

public class DegreeVertexProgram extends GraknVertexProgram<Long> {

    // element key
    public static final String DEGREE = "medianVertexProgram.degree";

    private static final Set<String> ELEMENT_COMPUTE_KEYS = Collections.singleton(DEGREE);

    public DegreeVertexProgram() {
    }

    public DegreeVertexProgram(Set<String> types) {
        selectedTypes = types;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return ELEMENT_COMPUTE_KEYS;
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
                    vertex.property(DEGREE, getEdgeCount(messenger));
                break;
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.info("Finished Iteration " + memory.getIteration());
        return memory.getIteration() == 2;
    }

}
