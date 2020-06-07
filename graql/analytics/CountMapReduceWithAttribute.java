/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.analytics;

import grakn.core.core.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;

/**
 * The MapReduce program for counting the number of instances
 *
 */

public class CountMapReduceWithAttribute extends CountMapReduce {

    @SuppressWarnings("unused")// Needed internally for OLAP tasks
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
