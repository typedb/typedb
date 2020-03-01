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
 *
 */

package grakn.core.keyspace;

import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.keyspace.StatisticsDelta;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class is bound per-keyspace, and shared between sessions on the same keyspace just like the JanusGraph object.
 * The general method of operation is as a cache, into which the statistics delta is merged on commit.
 * At this point we also write the statistics to JanusGraph, writing recorded values as vertex properties on the schema
 * concepts.
 *
 * On cache miss, we read from JanusGraph schema vertices, which only have the INSTANCE_COUNT property if the
 * count is non-zero or has been non-zero in the past. No such property means instance count is 0.
 *
 * We also store the total count of all concepts the same was as any other schema concept, but on the meta
 * concept types. Note that this is different from the other instance counts as it DOES include counts of all subtypes. The
 * other counts on user-defined schema concepts are for for that concrete type only
 */
public class KeyspaceStatisticsImpl implements KeyspaceStatistics {

    private ConcurrentHashMap<Label, Long> instanceCountsCache;

    public KeyspaceStatisticsImpl() {
        instanceCountsCache = new ConcurrentHashMap<>();
    }

    @Override
    public long count(ConceptManager conceptManager, Label label) {
        // return count if cached, else cache miss and retrieve from Janus
        instanceCountsCache.computeIfAbsent(label, l -> retrieveCountFromVertex(conceptManager, l));
        return instanceCountsCache.get(label);
    }

    @Override
    public void commit(ConceptManager conceptManager, StatisticsDelta statisticsDelta) {
        HashMap<Label, Long> deltaMap = statisticsDelta.instanceDeltas();

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
                                retrieveCountFromVertex(conceptManager, label) + delta:
                                prior + delta
            );
        });

        persist(conceptManager, labelsToPersist);
    }

    private void persist(ConceptManager conceptManager, Set<Label> labelsToPersist) {
        // TODO - there's an possible removal from instanceCountsCache here
        // when the schemaConcept is null - ie it's been removed. However making this
        // thread safe with competing action of creating a schema concept of the same name again
        // So for now just wait until sessions are closed and rebuild the instance counts cache from scratch

        for (Label label : labelsToPersist) {
            // don't change the value, just use `.compute()` for atomic and locking vertex write
            instanceCountsCache.compute(label, (lab, count) -> {
                Concept schemaConcept = conceptManager.getSchemaConcept(lab);
                if (schemaConcept != null && schemaConcept.isType()) {
                    Type conceptAsType = schemaConcept.asType();
                    conceptAsType.writeCount(count);
                }
                return count;
            });
        }
    }

    /**
     * Effectively a cache miss - retrieves the value from the janus vertex
     * Note that the count property doesn't exist on a label until a commit places a non-zero count on the vertex
     */
    private long retrieveCountFromVertex(ConceptManager conceptManager, Label label) {
        Concept schemaConcept = conceptManager.getSchemaConcept(label);
        if (schemaConcept != null && schemaConcept.isType()) {
            Type conceptAsType = schemaConcept.asType();
            return conceptAsType.getCount();
        } else {
            // if the schema concept is NULL, it doesn't exist! While it shouldn't pass validation, if we can use it to
            // short circuit then it's a good starting point
            return -1L;
        }
    }
}
