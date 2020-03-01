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

import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.LabelId;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * The MapReduce program for computing the standard deviation of the given resource.
 * <p>
 *
 */

public class StdMapReduce extends StatisticsMapReduce<Map<String, Double>> {

    public static final String COUNT = "C";
    public static final String SUM = "S";
    public static final String SQUARE_SUM = "SM";

    // Needed internally for OLAP tasks
    public StdMapReduce() {
    }

    public StdMapReduce(Set<LabelId> selectedLabelIds, AttributeType.DataType resourceDataType, String degreePropertyKey) {
        super(selectedLabelIds, resourceDataType, degreePropertyKey);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Map<String, Double>> emitter) {
        if (resourceIsValid(vertex)) {
            Map<String, Double> tuple = new HashMap<>(3);
            Double degree =
                    ((Long) vertex.value(degreePropertyKey)).doubleValue();
            double value = resourceValue(vertex).doubleValue();
            tuple.put(SUM, value * degree);
            tuple.put(SQUARE_SUM, value * value * degree);
            tuple.put(COUNT, degree);
            emitter.emit(NullObject.instance(), tuple);
            return;
        }
        Map<String, Double> emptyTuple = new HashMap<>(3);
        emptyTuple.put(SUM, 0D);
        emptyTuple.put(SQUARE_SUM, 0D);
        emptyTuple.put(COUNT, 0D);
        emitter.emit(NullObject.instance(), emptyTuple);
    }

    @Override
    Map<String, Double> reduceValues(Iterator<Map<String, Double>> values) {
        Map<String, Double> emptyTuple = new HashMap<>(3);
        emptyTuple.put(SUM, 0D);
        emptyTuple.put(SQUARE_SUM, 0D);
        emptyTuple.put(COUNT, 0D);
        return IteratorUtils.reduce(values, emptyTuple,
                (a, b) -> {
                    a.put(COUNT, a.get(COUNT) + b.get(COUNT));
                    a.put(SUM, a.get(SUM) + b.get(SUM));
                    a.put(SQUARE_SUM, a.get(SQUARE_SUM) + b.get(SQUARE_SUM));
                    return a;
                });
    }
}
