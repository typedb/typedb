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

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

/**
 * The MapReduce program for collecting the result of a clustering query.
 * <p>
 * It returns a map, the key being the cluster id, the value being the number of vertices the given cluster has.
 * <p>
 *
 */

public class ClusterSizeMapReduce extends GraknMapReduce<Long> {

    private static final String CLUSTER_LABEL = "clusterSizeMapReduce.clusterLabel";
    private static final String CLUSTER_SIZE = "clusterSizeMapReduce.size";

    // Needed internally for OLAP tasks
    public ClusterSizeMapReduce() {
    }

    public ClusterSizeMapReduce(String clusterLabel) {
        this(clusterLabel, null);
    }

    public ClusterSizeMapReduce(String clusterLabel, @Nullable Long clusterSize) {
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
        if (clusterSize != null) this.persistentProperties.put(CLUSTER_SIZE, clusterSize);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (vertex.property((String) persistentProperties.get(CLUSTER_LABEL)).isPresent()) {
            emitter.emit(vertex.value((String) persistentProperties.get(CLUSTER_LABEL)), 1L);
        } else {
            emitter.emit(NullObject.instance(), 0L);
        }
    }

    @Override
    Long reduceValues(Iterator<Long> values) {
        return IteratorUtils.reduce(values, 0L, (a, b) -> a + b);
    }

    @Override
    public Map<Serializable, Long> generateFinalResult(Iterator<KeyValue<Serializable, Long>> keyValues) {
        if (this.persistentProperties.containsKey(CLUSTER_SIZE)) {
            long clusterSize = (long) persistentProperties.get(CLUSTER_SIZE);
            keyValues = IteratorUtils.filter(keyValues, pair -> pair.getValue().equals(clusterSize));
        }
        final Map<Serializable, Long> clusterPopulation = Utility.keyValuesToMap(keyValues);
        clusterPopulation.remove(NullObject.instance());
        return clusterPopulation;
    }
}