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
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.Utility.reduceSet;

/**
 * The MapReduce program for collecting the result of a degree query.
 * <p>
 * It returns a map, the key being the degree, the value being a vertex id set containing all the vertices
 * with the given degree.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class DegreeDistributionMapReduce extends GraknMapReduce<Set<String>> {

    // Needed internally for OLAP tasks
    public DegreeDistributionMapReduce() {
    }

    public DegreeDistributionMapReduce(Set<TypeLabel> selectedTypes) {
        super(selectedTypes);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Set<String>> emitter) {
        if (selectedTypes.contains(Utility.getVertexType(vertex))) {
            emitter.emit(vertex.value(DegreeVertexProgram.DEGREE), Collections.singleton(vertex.id().toString()));
        } else {
            emitter.emit(NullObject.instance(), Collections.emptySet());
        }
    }

    @Override
    Set<String> reduceValues(Iterator<Set<String>> values) {
        return reduceSet(values);
    }

    @Override
    public Map<Serializable, Set<String>> generateFinalResult(Iterator<KeyValue<Serializable, Set<String>>> keyValues) {
        final Map<Serializable, Set<String>> clusterPopulation = Utility.keyValuesToMap(keyValues);
        clusterPopulation.remove(NullObject.instance());
        return clusterPopulation;
    }
}
