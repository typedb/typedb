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

package ai.grakn.util;

import ai.grakn.concept.ConceptId;

import java.util.Map;
import java.util.Set;

/**
 * <p>
 *     Engine's internal Concept ID cache
 * </p>
 *
 * <p>
 *    This is a cache which houses {@link org.apache.tinkerpop.gremlin.structure.Vertex} ids. It keeps these ids
 *    so that we can lookup the vertices in need of postprocessing directly.
 *
 *    We cannot relay on {@link ai.grakn.concept.ConceptId}s because the indexed lookup maybe faulty with
 *    vertices in need of post processing.
 * </p>
 *
 * @author fppt
 */
public interface EngineCache {
    /**
     *
     * @return All keyspaces which require post processing at this stage
     */
    Set<String> getKeyspaces();

    /**
     *
     * @param keyspace The keyspace with post processing jobs
     * @return The number of jobs currently pending for that keyspace
     */
    long getNumJobs(String keyspace);

    /**
     *
     * @param keyspace The keyspace with casting post processing jobs
     * @return The number of jobs currently pending for that keyspace
     */
    long getNumCastingJobs(String keyspace);

    /**
     *
     * @param keyspace The keyspace with resource post processing jobs
     * @return The number of jobs currently pending for that keyspace
     */
    long getNumResourceJobs(String keyspace);

    //-------------------- Casting Jobs
    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @return Casting Indices and Ids which require post processing
     */
    Map<String, Set<ConceptId>> getCastingJobs(String keyspace);

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @param castingIndex The unique index of this casting
     * @param castingId The casting vertex id
     */
    void addJobCasting(String keyspace, String castingIndex, ConceptId castingId);

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @param castingIndex The index of the casting id which has been post processed
     * @param castingId The castingId which has been post processed
     */
    void deleteJobCasting(String keyspace, String castingIndex, ConceptId castingId);

    //-------------------- Resource Jobs

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @return Resources Indices and Ids which require post processing
     */
    Map<String, Set<ConceptId>> getResourceJobs(String keyspace);

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @param resourceIndex The unique index of this resource
     * @param resourceId The resource vertex id
     */
    void addJobResource(String keyspace, String resourceIndex, ConceptId resourceId);

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @param resourceIndex The index of the resource id which has been post processed
     * @param resourceId The resourceId which has been post processed
     */
    void deleteJobResource(String keyspace, String resourceIndex, ConceptId resourceId);
}
