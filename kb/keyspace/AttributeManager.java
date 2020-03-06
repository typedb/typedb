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
import com.google.common.cache.Cache;
import grakn.core.kb.concept.api.ConceptId;

import java.util.Set;

/**
 * When loading concurrently, we want to minimise the amount of locking needed for correctness and consistency.
 * To be able to lock more effectively, we need to lock selectively based on the creation of identical attributes among different transactions.
 *
 * The idea is that if two transactions insert the same attribute, we require them to acquire graph locks when committing.
 * To be able to recognise this situation taking place, we introduce the AttributeManager. The AttributeManager is a session-wide
 * object used to manage attribute mutations. All transactions must report their attribute insertions, deletions and commits to the AttributeManager.
 * The AttributeManager then tracks the attribute mutations as well as txs in which the mutations took place. By logging this information the AttributeManager can then
 * find and recognise possible contention. The contention resolution happens in the following way:
 *
 * When about to commit, each transaction polls the AttributeManager if it needs to use a lock during commit.
 * If the AttributeManager finds at least two transactions with a shared attribute, it will advise the competing transactions
 * to use a lock when committing.
 */
public interface AttributeManager {

    Cache<String, ConceptId> attributesCommitted();
    void ackAttributeInsert(String index, String txId);
    void ackAttributeDelete(String index, String txId);
    void ackCommit(Set<String> indices, String txId);
    boolean requiresLock(String txId);
    boolean isAttributeEphemeral(String index);

    @VisibleForTesting
    boolean lockCandidatesPresent();

    @VisibleForTesting
    boolean ephemeralAttributesPresent();
}
