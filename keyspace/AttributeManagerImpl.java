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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.keyspace.AttributeManager;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class AttributeManagerImpl implements AttributeManager {
    private final static int TIMEOUT_MINUTES_ATTRIBUTES_CACHE = 2;
    private final static int ATTRIBUTES_CACHE_MAX_SIZE = 10000;

    private final Cache<String, ConceptId> attributesCommitted;
    //we track txs that insert an attribute with given index
    private final ConcurrentHashMap<String, Set<String>> attributesEphemeral;
    private final Set<String> lockCandidates;

    public AttributeManagerImpl(){
        this.attributesCommitted = CacheBuilder.newBuilder()
                .expireAfterAccess(TIMEOUT_MINUTES_ATTRIBUTES_CACHE, TimeUnit.MINUTES)
                .maximumSize(ATTRIBUTES_CACHE_MAX_SIZE)
                .build();

        this.attributesEphemeral = new ConcurrentHashMap<>();
        this.lockCandidates = ConcurrentHashMap.newKeySet();
    }

    @Override
    public boolean isAttributeEphemeral(String index) {
        return attributesEphemeral.containsKey(index);
    }

    @Override
    public Cache<String, ConceptId> attributesCommitted() {
        return attributesCommitted;
    }

    @Override
    public void ackAttributeInsert(String index, String txId) {
        //transaction of txId signals that it inserted an attribute with specific index:
        // - if we don't have the index in the cache, we create an appropriate entry
        // - if index is present in the cache, we update the entry with txId and recommend all txs in the entry to lock
        attributesEphemeral.compute(index, (ind, entry) -> {
            if (entry == null) {
                Set<String> txSet = ConcurrentHashMap.newKeySet();
                txSet.add(txId);
                return txSet;
            } else {
                entry.add(txId);
                if (entry.size() > 1) lockCandidates.addAll(entry);
                return entry;
            }
        });
    }

    @Override
    public void ackAttributeDelete(String index, String txId) {
        //transaction of txId signals that it deleted an attribute with specific index:
        // - we remove this txId from ephemeral attributes
        // - if the removal leads to emptying the ephemeral attribute entry, we remove the entry
        attributesEphemeral.merge(index, ConcurrentHashMap.newKeySet(), (existingValue, zero) -> {
            existingValue.remove(txId);
            if (existingValue.size() == 0) return null;
            return existingValue;
        });
    }
    
    private void ackAttributeCommit(String index, String txId) {
        ackAttributeDelete(index, txId);
    }

    @Override
    public void ackCommit(Set<String> indices, String txId) {
        indices.forEach(index -> ackAttributeCommit(index, txId));
        lockCandidates.remove(txId);
    }

    @Override
    public boolean requiresLock(String txId) {
        return lockCandidates.contains(txId);
    }

    @Override
    public boolean lockCandidatesPresent() {
        return !lockCandidates.isEmpty();
    }

    @Override
    public boolean ephemeralAttributesPresent() {
        return !attributesEphemeral.values().isEmpty();
    }
}
