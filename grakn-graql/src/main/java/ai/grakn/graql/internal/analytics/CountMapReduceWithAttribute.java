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
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;

/**
 * The MapReduce program for counting the number of instances
 *
 * @author Jason Liu
 */

public class CountMapReduceWithAttribute extends CountMapReduce {

    // Needed internally for OLAP tasks
    public CountMapReduceWithAttribute() {
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (vertex.property(CountVertexProgram.EDGE_COUNT).isPresent()) {
            emitter.emit(RESERVED_TYPE_LABEL_KEY,
                    vertex.value(CountVertexProgram.EDGE_COUNT));
        }
        emitter.emit(vertex.value(Schema.VertexProperty.THING_TYPE_LABEL_ID.name()), 1L);
    }
}
