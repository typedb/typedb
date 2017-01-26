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

    //-------------------- Casting Jobs
    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @return Casting Ids which require post processing
     */
    Set<String> getCastingJobs(String keyspace);

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @param castingIds Casting Ids which require post processing
     */
    void addJobCasting(String keyspace, Set<String> castingIds);

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @param castingId The castingId which has been post processed
     */
    void deleteJobCasting(String keyspace, String castingId);

    //-------------------- Resource Jobs
    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @return Resources Ids which require post processing
     */
    Set<String> getResourceJobs(String keyspace);

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @param resourceIds Resources Ids which require post processing
     */
    void addJobResource(String keyspace, Set<String> resourceIds);

    /**
     *
     * @param keyspace The keyspace containing casting jobs which need to be post processed
     * @param resourceId The resourceId which has been post processed
     */
    void deleteJobResource(String keyspace, String resourceId);
}
