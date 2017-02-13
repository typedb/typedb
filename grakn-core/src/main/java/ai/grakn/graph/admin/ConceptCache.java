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

package ai.grakn.graph.admin;

import ai.grakn.concept.ConceptId;

/**
 * Admin interface for Caching concepts which can be used later.
 *
 * @author Filipe Teixeira
 */
public interface ConceptCache {
    /**
     *
     * @param keyspace The keyspace of the concepts
     * @param castingIndex The unique index value which should be enforced.
     * @param castingId The casting ID which needs post processing
     */
    void addJobCasting(String keyspace, String castingIndex, ConceptId castingId);

    /**
     *
     * @param keyspace The keyspace of the concepts
     * @param castingIndex The unique index value which should be enforced.
     * @param castingId The casting Id which no longer needs post processing.
     */
    void deleteJobCasting(String keyspace, String castingIndex, ConceptId castingId);

    /**
     *
     * @param keyspace The keyspace of the concepts
     * @param resourceIndex The unique index value which should be enforced.
     * @param resourceId The resource Id which needs post processing
     */
    void addJobResource(String keyspace, String resourceIndex, ConceptId resourceId);

    /**
     *
     * @param keyspace The keyspace of the concepts
     * @param resourceIndex The unique index value which should be enforced.
     * @param resourceId The resourceId which no longer needs post prcoessing
     */
    void deleteJobResource(String keyspace, String resourceIndex, ConceptId resourceId);

    /**
     *
     * @param keyspace The keyspace of the concepts
     * @param conceptIndex The unique index value which has been enforced.
     */
    void clearJobSetResources(String keyspace, String conceptIndex);

    /**
     *
     * @param keyspace The keyspace of the concepts
     * @param conceptIndex The unique index value has been enforced.
     */
    void clearJobSetCastings(String keyspace, String conceptIndex);

    /**
     * Removes all jobs from the keyspace
     *
     * @param keyspace The keyspace to purge of all jobs
     */
    void clearAllJobs(String keyspace);
}
