/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;


/**
 * This class is bound per-keyspace, and shared between sessions on the same keyspace just like the JanusGraph object.
 * The general method of operation is as a cache, into which the statistics delta is merged on commit.
 * At this point we also write the statistics to JanusGraph, writing recorded values as vertex properties on the schema
 * concepts.
 *
 * On cache miss, we read from JanusGraph schema vertices, which only have the INSTANCE_COUNT property if the
 * count is non-zero or has been non-zero in the past. No such property means instance count is 0.
 *
 * We also store the total count of all concepts the same was as any other schema concept, but on the `Thing` meta
 * concept. Note that this is different from the other instance counts as it DOES include counts of all subtypes. The
 * other counts on user-defined schema concepts are for for that concrete type only
 */
public class KeyspaceStatistics {

    private ConcurrentHashMap<Label, Long> instanceCountsCache;

    public KeyspaceStatistics() {
        instanceCountsCache = new ConcurrentHashMap<>();
    }

    public long count(TransactionOLTP tx, Label label) {
        // return count if cached, else cache miss and retrieve from Janus
        instanceCountsCache.computeIfAbsent(label, l -> retrieveCountFromVertex(tx, l));
        return instanceCountsCache.get(label);
    }

    public void commit(TransactionOLTP tx, UncomittedStatisticsDelta statisticsDelta) {
        HashMap<Label, Long> deltaMap = statisticsDelta.instanceDeltas();

        statisticsDelta.updateThingCount();

        // merge each delta into the cache, then flush the cache to Janus
        Set<Label> labelsToPersist = new HashSet<>();
        deltaMap.entrySet().stream()
                .filter(e -> e.getValue() != 0)
                .forEach(entry -> {
                    Label label = entry.getKey();
                    Long delta = entry.getValue();
                    labelsToPersist.add(label);
                    // atomic update
                    instanceCountsCache.compute(label, (k, prior) ->
                        prior == null?
                                retrieveCountFromVertex(tx, label) + delta:
                                prior + delta
            );
        });

        persist(tx, labelsToPersist);
    }

    private void persist(TransactionOLTP tx, Set<Label> labelsToPersist) {
        // TODO - there's an possible removal from instanceCountsCache here
        // when the schemaConcept is null - ie it's been removed. However making this
        // thread safe with competing action of creating a schema concept of the same name again
        // So for now just wait until sessions are closed and rebuild the instance counts cache from scratch

        for (Label label : labelsToPersist) {
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
