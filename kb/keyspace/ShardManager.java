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

package grakn.core.kb.keyspace;

import com.google.common.annotations.VisibleForTesting;
import grakn.core.kb.concept.api.Label;

import java.util.Set;

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
    void ackCommit(Set<Label> labels, String txId);
    boolean requiresLock(String txId);

    Long getEphemeralShardCount(Label type);
    void updateEphemeralShardCount(Label type, Long count);

    @VisibleForTesting
    boolean lockCandidatesPresent();

    @VisibleForTesting
    boolean shardRequestsPresent();
}
