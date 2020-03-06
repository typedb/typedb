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
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.keyspace.ShardManager;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ShardManagerImpl implements ShardManager {
    private final static int TIMEOUT_MINUTES_ATTRIBUTES_CACHE = 2;
    private final static int ATTRIBUTES_CACHE_MAX_SIZE = 10000;

    private final Cache<Label, Long> shardsEphemeral;
    private final ConcurrentHashMap<Label, Set<String>> shardRequests;
    private final Set<String> lockCandidates;

    public ShardManagerImpl(){
        this.shardsEphemeral = CacheBuilder.newBuilder()
                .expireAfterAccess(TIMEOUT_MINUTES_ATTRIBUTES_CACHE, TimeUnit.MINUTES)
                .maximumSize(ATTRIBUTES_CACHE_MAX_SIZE)
                .build();
        this.shardRequests = new ConcurrentHashMap<>();
        this.lockCandidates = ConcurrentHashMap.newKeySet();
    }


    public Long getEphemeralShardCount(Label type){ return shardsEphemeral.getIfPresent(type);}
    public void updateEphemeralShardCount(Label type, Long count){ shardsEphemeral.put(type, count);}

    @Override
    public void ackShardRequest(Label type, String txId) {
        //transaction of txId signals that it needs to create a shard for a specific label:
        // - if we don't have the label in the cache, we create an appropriate entry
        // - if label is present in the cache, we update the entry with this txId and recommend all txs in the entry to lock
        shardRequests.compute(type, (ind, entry) -> {
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

    private void ackShardCommit(Label type, String txId) {
        //transaction of txId signals that it commited a shard for a specific label:
        // - we remove this txId from shard requests
        // - if the removal leads to emptying the entry of the shard requests, we remove the entry
        shardRequests.merge(type, ConcurrentHashMap.newKeySet(), (existingValue, newValue) -> {
            existingValue.remove(txId);
            if (existingValue.size() == 0) return null;
            return existingValue;
        });
    }

    @Override
    public void ackCommit(Set<Label> labels, String txId) {
        labels.forEach(label -> ackShardCommit(label, txId));
        lockCandidates.remove(txId);
    }

    @Override
    public boolean requiresLock(String txId) {
        return lockCandidates.contains(txId);
    }

    @Override
    public boolean lockCandidatesPresent(){
        return !lockCandidates.isEmpty();
    }

    @Override
    public boolean shardRequestsPresent() {
        return !shardRequests.values().isEmpty();
    }

}
