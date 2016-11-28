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
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.*;

class ClusterMemberMapReduce extends GraknMapReduce<Set<String>> {

    private static final String CLUSTER_LABEL = "clusterMemberMapReduce.clusterLabel";

    public ClusterMemberMapReduce() {
    }

    public ClusterMemberMapReduce(Set<String> selectedTypes, String clusterLabel) {
        this.selectedTypes = selectedTypes;
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Set<String>> emitter) {
        if (selectedTypes.contains(Utility.getVertexType(vertex)) &&
                vertex.property((String) persistentProperties.get(CLUSTER_LABEL)).isPresent()) {
            emitter.emit(vertex.value((String) persistentProperties.get(CLUSTER_LABEL)),
                    Collections.singleton(vertex.id().toString()));
            return;
        }
        emitter.emit(NullObject.instance(), Collections.emptySet());
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Set<String>> values,
                       final ReduceEmitter<Serializable, Set<String>> emitter) {
        Set<String> set = new HashSet<>();
        while (values.hasNext()) {
            set.addAll(values.next());
        }
        emitter.emit(key, set);
    }

    @Override
    public void combine(final Serializable key, final Iterator<Set<String>> values,
                        final ReduceEmitter<Serializable, Set<String>> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public boolean doStage(Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Set<String>> generateFinalResult(Iterator<KeyValue<Serializable, Set<String>>> keyValues) {
        final Map<Serializable, Set<String>> clusterPopulation = new HashMap<>();
        keyValues.forEachRemaining(pair -> clusterPopulation.put(pair.getKey(), pair.getValue()));
        clusterPopulation.remove(NullObject.instance());
        return clusterPopulation;
    }
}
