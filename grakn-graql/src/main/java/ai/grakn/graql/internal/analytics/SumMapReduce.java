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

import ai.grakn.concept.TypeLabel;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

/**
 * The MapReduce program for computing the sum of the given resource.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class SumMapReduce extends GraknMapReduce<Number> {

    // Needed internally for OLAP tasks
    public SumMapReduce() {
    }

    public SumMapReduce(Set<TypeLabel> selectedTypes, String resourceDataType) {
        super(selectedTypes, resourceDataType);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Number> emitter) {
        if (usingLong()) {
            if (resourceIsValid(vertex)) {
                emitter.emit(NullObject.instance(),
                        ((Long) vertex.value(Schema.ConceptProperty.VALUE_LONG.name())) *
                                ((Long) vertex.value(DegreeVertexProgram.DEGREE)));
                return;
            }
            emitter.emit(NullObject.instance(), 0L);
        } else {
            if (resourceIsValid(vertex)) {
                emitter.emit(NullObject.instance(),
                        ((Double) vertex.value(Schema.ConceptProperty.VALUE_DOUBLE.name())) *
                                ((Long) vertex.value(DegreeVertexProgram.DEGREE)));
                return;
            }
            emitter.emit(NullObject.instance(), 0D);
        }
    }

    @Override
    Number reduceValues(Iterator<Number> values) {
        if (usingLong()) {
            return IteratorUtils.reduce(values, 0L, (a, b) -> a.longValue() + b.longValue());
        } else {
            return IteratorUtils.reduce(values, 0D, (a, b) -> a.doubleValue() + b.doubleValue());
        }
    }
}
