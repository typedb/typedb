/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;

/**
 *
 * <p>
 *    Stores a cache of counts so that we can know which {@link ai.grakn.concept.Type}s to shard when they have too many
 *    instances.
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */
public interface CountStorage {
    /**
     * Adjusts the count for a specific concept.
     *
     * @param keyspace
     * @param conceptId
     * @param incrementBy the number to adjust the key by
     * @return the final value after being adjusted
     */
    long incrementInstanceCount(Keyspace keyspace, ConceptId conceptId, long incrementBy);

    /**
     * Adjusts the shard count for a specific concept.
     *
     * @param keyspace
     * @param conceptId
     * @param incrementBy the number to adjust the key by
     * @return the final value after being adjusted
     */
    long incrementShardCount(Keyspace keyspace, ConceptId conceptId, long incrementBy);

    /**
     * Get the instance count for a specific concept.
     *
     * @param keyspace
     * @param conceptId
     * @return the instance count
     */
    long getInstanceCount(Keyspace keyspace, ConceptId conceptId);

    /**
     * Get the shard count for a specific concept.
     *
     * @param keyspace
     * @param conceptId
     * @return the shard count
     */
    long getShardCount(Keyspace keyspace, ConceptId conceptId);
}
