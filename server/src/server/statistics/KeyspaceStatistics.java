/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.statistics;

import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.ConceptVertex;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyspaceStatistics {

    private ConcurrentHashMap<String, Long> instanceCountsCache;

    public KeyspaceStatistics() {
        instanceCountsCache = new ConcurrentHashMap<>();
    }

    public long count(TransactionOLTP tx, String label) {
        // return count if cached, else cache miss and retrieve from Janus
        instanceCountsCache.computeIfAbsent(label, l -> retrieveCountFromVertex(tx, l));
        return instanceCountsCache.get(label);
    }

    public void commit(TransactionOLTP tx, UncomittedStatisticsDelta statisticsDelta) {
        HashMap<String, Long> deltaMap = statisticsDelta.instanceDeltas();

        // merge each delta into the cache, then flush the cache to Janus
        for (Map.Entry<String, Long> entry : deltaMap.entrySet()) {
            String label = entry.getKey();
            Long delta = entry.getValue();
            instanceCountsCache.compute(label, (k, prior) -> {
                long existingCount;
                if (instanceCountsCache.containsKey(label)) {
                    existingCount = prior;
                } else {
                    existingCount = retrieveCountFromVertex(tx, label) ;
                }
                return existingCount + delta;
            });
        }

        persist(tx);
    }

    private void persist(TransactionOLTP tx) {
        for (String label : instanceCountsCache.keySet()) {
            Long count = instanceCountsCache.get(label);
            Concept schemaConcept = tx.getSchemaConcept(Label.of(label));
            Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
            janusVertex.property(Schema.VertexProperty.INSTANCE_COUNT.name(), count);
        }
    }

    private long retrieveCountFromVertex(TransactionOLTP tx, String label) {
        Concept schemaConcept = tx.getSchemaConcept(Label.of(label));
        Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
        VertexProperty<Object> property = janusVertex.property(Schema.VertexProperty.INSTANCE_COUNT.name());
        // VertexProperty is similar to a Java Optional
        return (Long) property.orElse(0L);
    }
}
