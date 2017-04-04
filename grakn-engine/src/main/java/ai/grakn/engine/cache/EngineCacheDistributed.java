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
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.graph.admin.ConceptCache;

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
 *
 *    This distributed version persists everything using {@link ai.grakn.engine.tasks.manager.ZookeeperConnection}
 * </p>
 *
 * @author fppt
 */
//TODO: Maybe we can merge this with Stand alone engine cache using Kafka for example?
public class EngineCacheDistributed implements ConceptCache {
    private static EngineCacheDistributed instance = null;


    private EngineCacheDistributed(ZookeeperConnection connection){
        if(connection == null) throw  new RuntimeException("This is just some placeholder logic");
    }

    public static EngineCacheDistributed init(ZookeeperConnection connection){
        if(instance == null) instance = new EngineCacheDistributed(connection);
        return instance;
    }

    @Override
    public Set<String> getKeyspaces() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getNumJobs(String keyspace) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getNumCastingJobs(String keyspace) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getNumResourceJobs(String keyspace) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Map<String, Set<ConceptId>> getCastingJobs(String keyspace) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void addJobCasting(String keyspace, String castingIndex, ConceptId castingId) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void deleteJobCasting(String keyspace, String castingIndex, ConceptId castingId) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Map<String, Set<ConceptId>> getResourceJobs(String keyspace) {
        return null;
    }

    @Override
    public void addJobResource(String keyspace, String resourceIndex, ConceptId resourceId) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void deleteJobResource(String keyspace, String resourceIndex, ConceptId resourceId) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void clearJobSetResources(String keyspace, String conceptIndex) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void clearJobSetCastings(String keyspace, String conceptIndex) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void clearAllJobs(String keyspace) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getLastTimeJobAdded() {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
