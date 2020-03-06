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
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.LabelId;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

/**
 * The MapReduce program for computing the sum of the given resource.
 * <p>
 *
 */

public class SumMapReduce extends StatisticsMapReduce<Number> {

    // Needed internally for OLAP tasks
    public SumMapReduce() {
    }

    public SumMapReduce(Set<LabelId> selectedLabelIds, AttributeType.DataType resourceDataType, String degreePropertyKey) {
        super(selectedLabelIds, resourceDataType, degreePropertyKey);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Number> emitter) {
        if (usingLong()) {
            if (resourceIsValid(vertex)) {
                emitter.emit(NullObject.instance(),
                        ((Long) vertex.value(Schema.VertexProperty.VALUE_LONG.name())) *
                                ((Long) vertex.value(degreePropertyKey)));
                return;
            }
            emitter.emit(NullObject.instance(), 0L);
        } else {
            if (resourceIsValid(vertex)) {
                emitter.emit(NullObject.instance(),
                        ((Double) vertex.value(Schema.VertexProperty.VALUE_DOUBLE.name())) *
                                ((Long) vertex.value(degreePropertyKey)));
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
