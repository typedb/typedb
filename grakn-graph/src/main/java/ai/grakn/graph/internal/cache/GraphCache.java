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

package ai.grakn.graph.internal.cache;

import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.concept.OntologyConceptImpl;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Tracks Graph Specific Variables
 * </p>
 *
 * <p>
 *     Caches Graph or Session specific data which is shared across transactions:
 *     <ol>
 *         <li>Ontology Cache - All the types which make up the ontology. This cache expires</li>
 *         <li>
 *             Label Cache - All the labels which make up the ontology. This can never expire and is needed in order
 *             to perform fast lookups. Essentially it is used for mapping labels to ids.
 *         </li>
 *     <ol/>
 * </p>
 *
 * @author fppt
 *
 */
public class GraphCache {
    //Caches
    private final Cache<Label, OntologyConcept> cachedTypes;
    private final Map<Label, LabelId> cachedLabels;

    public GraphCache(Properties properties){
        cachedLabels = new ConcurrentHashMap<>();

        int cacheTimeout = Integer.parseInt(properties.get(AbstractGraknGraph.NORMAL_CACHE_TIMEOUT_MS).toString());
        cachedTypes = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(cacheTimeout, TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * Caches a type so that we can retrieve ontological concepts without making a DB read.
     *
     * @param label The label of the type to cache
     * @param type The type to cache
     */
    public void cacheType(Label label, OntologyConcept type){
        cachedTypes.put(label, type);
    }

    /**
     * Caches a label so we can map type labels to type ids. This is necesssary so we can make fast indexed lookups.
     *
     * @param label The label of the type to cache
     * @param id The id of the type to cache
     */
    public void cacheLabel(Label label, LabelId id){
        cachedLabels.put(label, id);
    }

    /**
     * Reads the types and their labels currently in the transaction cache into the graph cache.
     * This usually happens when a commit occurs and allows us to track Ontology mutations without having to read
     * the graph.
     *
     * @param txCache The transaction cache
     */
    void readTxCache(TxCache txCache){
        //TODO: The difference between the caches need to be taken into account. For example if a type is delete then it should be removed from the cachedLabels
        cachedLabels.putAll(txCache.getLabelCache());
        cachedTypes.putAll(txCache.getOntologyConceptCache());

        //Flush All The Internal Transaction Caches
        txCache.getOntologyConceptCache().values().forEach(ontologyConcept
                -> OntologyConceptImpl.from(ontologyConcept).txCacheFlush());
    }

    /**
     * A copy of the cached labels. This is used when creating a new transaction.
     *
     * @return an immutable copy of the cached labels.
     */
    Map<Label, LabelId> getCachedLabels(){
        return ImmutableMap.copyOf(cachedLabels);
    }

    /**
     * A copy of the cached ontology. This is used when creating a new transaction.
     *
     * @return an immutable copy of the cached ontology.
     */
    public Map<Label, OntologyConcept> getCachedTypes(){
        return ImmutableMap.copyOf(cachedTypes.asMap());
    }
}
