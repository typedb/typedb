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


/**
 * This class is bound per-keyspace, and shared between sessions on the same keyspace just like the JanusGraph object.
 * The general method of operation is as a cache, into which the statistics delta is merged on commit.
 * At this point we also write the statistics to JanusGraph, writing recorded values as vertex properties on the schema
 * concepts.
 *
 * On cache miss, we read from JanusGraph schema vertices, which only have the INSTANCE_COUNT property if the
 * count is non-zero or has been non-zero in the past. No such property means instance count is 0.
 */
public class KeyspaceStatistics {

    private ConcurrentHashMap<Label, Long> instanceCountsCache;

    public KeyspaceStatistics() {
        instanceCountsCache = new ConcurrentHashMap<>();
    }

    public long count(TransactionOLTP tx, String label) {
        Label lab = Label.of(label);
        // return count if cached, else cache miss and retrieve from Janus
        instanceCountsCache.computeIfAbsent(lab, l -> retrieveCountFromVertex(tx, l));
        return instanceCountsCache.get(lab);
    }

    public void commit(TransactionOLTP tx, UncomittedStatisticsDelta statisticsDelta) {
        HashMap<Label, Long> deltaMap = statisticsDelta.instanceDeltas();

        // merge each delta into the cache, then flush the cache to Janus
        for (Map.Entry<Label, Long> entry : deltaMap.entrySet()) {
            Label label = entry.getKey();
            Long delta = entry.getValue();
            // atomic update
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
        // TODO - there's an possible removal from instanceCountsCache here
        // when the schemaConcept is null - ie it's been removed. However making this
        // thread safe with competing action of creating a schema concept of the same name again
        // So for now just wait until sessions are closed and rebuild the instance counts cache from scratch

        for (Label label : instanceCountsCache.keySet()) {
            // don't change the value, just use `.compute()` for atomic and locking vertex write
            instanceCountsCache.compute(label, (lab, count) -> {
                Concept schemaConcept = tx.getSchemaConcept(lab);
                if (schemaConcept != null) {
                    Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
                    janusVertex.property(Schema.VertexProperty.INSTANCE_COUNT.name(), count);
                }
                return count;
            });
        }
    }

    /**
     * Effectively a cache miss - retrieves the value from the janus vertex
     * Note that the count property doesn't exist on a label until a commit places a non-zero count on the vertex
     */
    private long retrieveCountFromVertex(TransactionOLTP tx, Label label) {
        Concept schemaConcept = tx.getSchemaConcept(label);
        if (schemaConcept != null) {
            Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
            VertexProperty<Object> property = janusVertex.property(Schema.VertexProperty.INSTANCE_COUNT.name());
            // VertexProperty is similar to a Java Optional
            return (Long) property.orElse(0L);
        } else {
            // if the schema concept is NULL, it doesn't exist! While it shouldn't pass validation, if we can use it to
            // short circuit then it's a good starting point
            return -1L;
        }
    }
}
