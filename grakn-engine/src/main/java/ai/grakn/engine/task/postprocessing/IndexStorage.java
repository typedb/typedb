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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.task.postprocessing;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * <p>
 *     Stores a list of indices and vertex ids representing those indices which need to be post processed
 * </p>
 *
 * @author lolski
 */

public interface IndexStorage {
    /**
     * Add an index to the list of indices which needs to be post processed
     */
    void addIndex(Keyspace keyspace, String index, Set<ConceptId> conceptIds);

    /**
     * Gets and removes the next index to post process
     */
    @Nullable
    String popIndex(Keyspace keyspace);

    /**
     * Gets and removes all the ids which we need to post process
     */
    Set<ConceptId> popIds(Keyspace keyspace, String index);
}
