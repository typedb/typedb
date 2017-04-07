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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.Utility.reduceSet;

/**
 * The MapReduce program for collecting the result of a clustering query.
 * <p>
 * It returns a map, the key being the cluster id, the value being a vertex id set containing all the vertices
 * in the given cluster
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class ClusterMemberMapReduce extends GraknMapReduce<Set<String>> {

    private static final String CLUSTER_LABEL = "clusterMemberMapReduce.clusterLabel";
    private static final String CLUSTER_SIZE = "clusterMemberMapReduce.clusterSize";

    // Needed internally for OLAP tasks
    public ClusterMemberMapReduce() {
    }

    public ClusterMemberMapReduce(Set<TypeLabel> selectedTypes, String clusterLabel) {
        super(selectedTypes);
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
    }

    public ClusterMemberMapReduce(Set<TypeLabel> selectedTypes, String clusterLabel, Long clusterSize) {
        this(selectedTypes, clusterLabel);
        this.persistentProperties.put(CLUSTER_SIZE, clusterSize);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Set<String>> emitter) {
        if (vertex.property((String) persistentProperties.get(CLUSTER_LABEL)).isPresent()) {
            if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                emitter.emit(vertex.value((String) persistentProperties.get(CLUSTER_LABEL)),
                        Collections.singleton(vertex.id().toString()));
            }
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
        if (this.persistentProperties.containsKey(CLUSTER_SIZE)) {
            long clusterSize = (long) persistentProperties.get(CLUSTER_SIZE);
            keyValues = IteratorUtils.filter(keyValues, pair -> Long.valueOf(pair.getValue().size()).equals(clusterSize));
        }
        final Map<Serializable, Set<String>> clusterPopulation = Utility.keyValuesToMap(keyValues);
        clusterPopulation.remove(NullObject.instance());
        return clusterPopulation;
    }
}
