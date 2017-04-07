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
import ai.grakn.concept.TypeLabel;

import java.util.Map;
import java.util.Set;

/**
 * Admin interface for Caching concepts which can be used later.
 *
 * @author Filipe Teixeira
 */
//TODO: Remove bloat from this interface
public interface ConceptCache {

    /**
     *
     * @return All the keyspaces with jobs currently pending on them
     */
    Set<String> getKeyspaces();

    /**
     *
     * @param keyspace The keyspace of a specific graph.
     * @return The number of jobs pending to be performed on that graph
     */
    long getNumJobs(String keyspace);

    /**
     * Removes all jobs from the keyspace
     *
     * @param keyspace The keyspace to purge of all jobs
     */
    void clearAllJobs(String keyspace);

    /**
     *
     * @return The timestamp of the last time a job was added
     */
    //TODO: This should also be keyspace specific
    long getLastTimeJobAdded();

    //-------------------- Instance Count Jobs
    /**
     *
     * @param keyspace The keyspace of a specific graph.
     * @return The types and the number of instances that have been removed or added to the type
     */
    Map<TypeLabel, Long> getInstanceCountJobs(String keyspace);

    /**
     *
     * @param keyspace The keyspace of the concepts
     * @param name The name of the type with new or removed instances
     * @param instanceCount The number of new or removed instances
     */
    void addJobInstanceCount(String keyspace, TypeLabel name, long instanceCount);

    /**
     *
     * @param keyspace The keyspace of the concepts
     * @param name The name of the type with new or removed instances
     */
    void deleteJobInstanceCount(String keyspace, TypeLabel name);

    //-------------------- Casting Jobs
    /**
     *
     * @param keyspace The keyspace of a specific graph.
     * @return The unique index and set of concept ids which should be merged into that index
     */
    Map<String, Set<ConceptId>> getCastingJobs(String keyspace);

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
     * @param keyspace The keyspace of a specific graph.
     * @return The number of casting jobs pending to be performed on that graph
     */
    long getNumCastingJobs(String keyspace);

    /**
     * Deletes the job set only if there are no pending jobs
     *
     * @param keyspace The keyspace of the concepts
     * @param conceptIndex The unique index value has been enforced.
     */
    void clearJobSetCastings(String keyspace, String conceptIndex);

    //-------------------- Resource Jobs
    /**
     *
     * @param keyspace The keyspace of a specific graph.
     * @return The unique index and set of concept ids which should be merged into that index
     */
    Map<String, Set<ConceptId>> getResourceJobs(String keyspace);

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
     * @param keyspace The keyspace of a specific graph.
     * @return The number of resource jobs pending to be performed on that graph
     */
    long getNumResourceJobs(String keyspace);

    /**
     * Deletes the job set only if there are no pending jobs
     *
     * @param keyspace The keyspace of the concepts
     * @param conceptIndex The unique index value which has been enforced.
     */
    void clearJobSetResources(String keyspace, String conceptIndex);
}
