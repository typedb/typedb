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

import grakn.core.kb.concept.api.Label;
import grakn.core.kb.server.ShardManager;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ShardManagerImpl implements ShardManager {

    private final ConcurrentHashMap<Label, Set<String>> shardRequests;
    private final Set<String> lockCandidates;

    public ShardManagerImpl(){
        this.shardRequests = new ConcurrentHashMap<>();
        this.lockCandidates = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void ackShardRequest(Label type, String txId) {
        shardRequests.compute(type, (ind, entry) -> {
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
    public void ackShardCommit(Label type, String txId) {
        shardRequests.merge(type, ConcurrentHashMap.newKeySet(), (existingValue, zero) -> {
            if (existingValue.isEmpty()) return null;
            existingValue.remove(txId);
            return existingValue;
        });
    }

    @Override
    public void ackCommit(String txId) {
        lockCandidates.remove(txId);
    }

    @Override
    public boolean requiresLock(String txId) {
        return lockCandidates.contains(txId);
    }
}
