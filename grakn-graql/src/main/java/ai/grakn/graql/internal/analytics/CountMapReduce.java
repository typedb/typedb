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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.Iterator;

/**
 * The MapReduce program for counting the number of instances in a graph
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class CountMapReduce extends GraknMapReduce<Long> {

    // Needed internally for OLAP tasks
    public CountMapReduce() {
    }

    public CountMapReduce(String edgeCountPropertyKey) {
        this.persistentProperties.put(CountVertexProgram.EDGE_COUNT, edgeCountPropertyKey);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (vertex.property((String) persistentProperties.get(CountVertexProgram.EDGE_COUNT)).isPresent()) {
            emitter.emit(RESERVED_TYPE_LABEL_KEY,
                    vertex.value((String) persistentProperties.get(CountVertexProgram.EDGE_COUNT)));
        }
        emitter.emit(vertex.value(Schema.VertexProperty.THING_TYPE_LABEL_ID.name()), 1L);
    }

    @Override
    Long reduceValues(Iterator<Long> values) {
        return IteratorUtils.reduce(values, 0L, (a, b) -> a + b);
    }
}
