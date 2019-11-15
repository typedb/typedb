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

package grakn.core.kb.server;

import com.google.common.cache.Cache;
import grakn.core.kb.concept.api.Label;

/**
 * When loading concurrently, we want to minimise the amount of locking needed for correctness and consistency.
 * To be able to lock more effectively, we need to lock selectively based on the creation of shards of the same type.
 *
 * The idea is that if two transactions are about to insert a shard for the same type, we require them to acquire graph locks when committing.
 * To do so we introduce the ShardManager. The ShardManager is a session-wide object used to resolve shard creation contention.
 * Just before committing, all transactions are required to signal their need to create a new shard vertex to the ShardManager.
 * The ShardManager then tracks the transaction shard requirements for specific types. This way it can find and resolve possible contention.
 *
 * When a transaction is about to commit, it polls the ShardManager if it needs to use a lock during commit.
 * If the ShardManager finds at least two transactions with a shared shard vertex request, it will advise
 * the competing transaction to use a lock when committing.
 */
public interface ShardManager {

    void ackShardRequest(Label type, String txId);
    void ackShardCommit(Label type, String txId);
    void ackCommit(String txId);
    boolean requiresLock(String txId);
    Cache<Label, Long> shardCache();
    boolean lockCandidatesPresent();
    boolean shardRequestsPresent();
}
