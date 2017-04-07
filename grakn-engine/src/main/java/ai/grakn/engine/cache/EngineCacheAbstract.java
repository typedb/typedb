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

package ai.grakn.engine.cache;

import ai.grakn.concept.ConceptId;
import ai.grakn.graph.admin.ConceptCache;

import java.util.Map;
import java.util.Set;

/**
 * <p>
 *     Engine's internal Concept ID cache
 * </p>
 *
 * <p>
 *     This stores the common implementation details between {@link EngineCacheDistributed} and {@link EngineCacheStandAlone}
 * </p>
 *
 * @author fppt
 */
abstract class EngineCacheAbstract implements ConceptCache {

    @Override
    public long getNumJobs(String keyspace) {
        return getNumCastingJobs(keyspace) + getNumResourceJobs(keyspace);
    }

    @Override
    public long getNumCastingJobs(String keyspace) {
        return getNumJobsCount(getCastingJobs(keyspace));
    }

    @Override
    public long getNumResourceJobs(String keyspace) {
        return getNumJobsCount(getResourceJobs(keyspace));
    }

    private long getNumJobsCount(Map<String, Set<ConceptId>> cache){
        return cache.values().stream().mapToLong(Set::size).sum();
    }
}
