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
import grakn.core.kb.concept.api.ConceptId;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.analytics.Utility.reduceSet;

/**
 * The MapReduce program for collecting the result of a clustering query.
 * <p>
 * It returns a map, the key being the cluster id, the value being a vertex id set containing all the vertices
 * in the given cluster
 * <p>
 *
 */

public class ClusterMemberMapReduce extends GraknMapReduce<Set<ConceptId>> {

    private static final String CLUSTER_LABEL = "clusterMemberMapReduce.clusterLabel";
    private static final String CLUSTER_SIZE = "clusterMemberMapReduce.size";

    // Needed internally for OLAP tasks
    public ClusterMemberMapReduce() {
    }

    public ClusterMemberMapReduce(String clusterLabel) {
        this(clusterLabel, null);
    }

    public ClusterMemberMapReduce(String clusterLabel, @Nullable Long clusterSize) {
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
        if (clusterSize != null) this.persistentProperties.put(CLUSTER_SIZE, clusterSize);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Set<ConceptId>> emitter) {
        if (vertex.property((String) persistentProperties.get(CLUSTER_LABEL)).isPresent()) {
            String clusterPropertyKey = (String) persistentProperties.get(CLUSTER_LABEL);
            String clusterId = vertex.value(clusterPropertyKey);
            ConceptId conceptId = Schema.conceptId(vertex);
            Set<ConceptId> cluster = Collections.singleton(conceptId);

            emitter.emit(clusterId, cluster);
        } else {
            emitter.emit(NullObject.instance(), Collections.emptySet());
        }
    }

    @Override
    Set<ConceptId> reduceValues(Iterator<Set<ConceptId>> values) {
        return reduceSet(values);
    }

    @Override
    public Map<Serializable, Set<ConceptId>> generateFinalResult(Iterator<KeyValue<Serializable, Set<ConceptId>>> keyValues) {
        if (this.persistentProperties.containsKey(CLUSTER_SIZE)) {
            long clusterSize = (long) persistentProperties.get(CLUSTER_SIZE);
            keyValues = IteratorUtils.filter(keyValues, pair -> Long.valueOf(pair.getValue().size()).equals(clusterSize));
        }
        final Map<Serializable, Set<ConceptId>> clusterPopulation = Utility.keyValuesToMap(keyValues);
        clusterPopulation.remove(NullObject.instance());
        return clusterPopulation;
    }
}
