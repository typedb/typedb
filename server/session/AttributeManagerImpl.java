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
 *
 */


package grakn.core.server.session;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.server.AttributeManager;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class AttributeManagerImpl implements AttributeManager {
    private final static int TIMEOUT_MINUTES_ATTRIBUTES_CACHE = 2;
    private final static int ATTRIBUTES_CACHE_MAX_SIZE = 10000;

    private final Cache<String, ConceptId> attributesCache;
    //we track txs that insert an attribute with given index
    private final ConcurrentHashMap<String, Set<String>> ephemeralAttributeCache;
    private final Set<String> lockCandidates;

    public AttributeManagerImpl(){
        this.attributesCache = CacheBuilder.newBuilder()
                .expireAfterAccess(TIMEOUT_MINUTES_ATTRIBUTES_CACHE, TimeUnit.MINUTES)
                .maximumSize(ATTRIBUTES_CACHE_MAX_SIZE)
                .build();

        this.ephemeralAttributeCache = new ConcurrentHashMap<>();
        this.lockCandidates = ConcurrentHashMap.newKeySet();
    }

    @Override
    public Cache<String, ConceptId> attributesCache() {
        return attributesCache;
    }

    @Override
    public void ackAttributeInsert(String index, String txId) {
        ephemeralAttributeCache.compute(index, (ind, entry) -> {
            if (entry == null) {
                Set<String> txSet = ConcurrentHashMap.newKeySet();
                txSet.add(txId);
                return txSet;
            }
            else{
                entry.add(txId);
                if (entry.size() > 1) lockCandidates.addAll(entry);
                return entry;
            }
        });
    }

    @Override
    public void ackAttributeDelete(String index, String txId) {
        ephemeralAttributeCache.merge(index, ConcurrentHashMap.newKeySet(), (existingValue, zero) -> {
            if (existingValue.isEmpty()) return null;
            existingValue.remove(txId);
            return existingValue;
        });
    }

    @Override
    public void ackAttributeCommit(String index, String txId) {
        ackAttributeDelete(index, txId);
    }

    @Override
    public void ackCommit(String txId) {
        lockCandidates.remove(txId);
    }

    @Override
    public boolean requiresLock(String txId) {
        return lockCandidates.contains(txId);
    }

    @Override
    public void printEphemeralCache() {
        ephemeralAttributeCache.entrySet().forEach(System.out::println);
    }

}
