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

package ai.grakn.engine.postprocessing;

import ai.grakn.util.EngineCache;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
public class EngineCacheImpl implements EngineCache {
    private final Map<String, Set<String>> castings;
    private final Map<String, Set<String>> resources;
    private final AtomicBoolean saveInProgress;

    private static EngineCacheImpl instance=null;
    private final AtomicLong lastTimeModified;

    public static synchronized EngineCacheImpl getInstance(){
        if(instance==null) instance=new EngineCacheImpl();
        return instance;
    }

    private EngineCacheImpl(){
        castings = new ConcurrentHashMap<>();
        resources = new ConcurrentHashMap<>();
        saveInProgress = new AtomicBoolean(false);
        lastTimeModified = new AtomicLong(System.currentTimeMillis());
    }

    public boolean isSaveInProgress() {
        return saveInProgress.get();
    }

    public Set<String> getKeyspaces(){
        Set<String> keyspaces = new HashSet<>();
        keyspaces.addAll(castings.keySet());
        keyspaces.addAll(resources.keySet());
        return keyspaces;
    }

    //-------------------- Casting Jobs
    @Override
    public Set<String> getCastingJobs(String keyspace) {
        keyspace = keyspace.toLowerCase();
        return castings.computeIfAbsent(keyspace, (key) -> ConcurrentHashMap.newKeySet());
    }
    @Override
    public void addJobCasting(String keyspace, Set<String> castingIds) {
        getCastingJobs(keyspace).addAll(castingIds);
        updateLastTimeJobAdded();
    }
    @Override
    public void deleteJobCasting(String keyspace, String castingId) {
        getCastingJobs(keyspace).remove(castingId);
    }

    //-------------------- Resource Jobs
    @Override
    public Set<String> getResourceJobs(String keyspace) {
        keyspace = keyspace.toLowerCase();
        return resources.computeIfAbsent(keyspace, (key) -> ConcurrentHashMap.newKeySet());
    }
    @Override
    public void addJobResource(String keyspace, Set<String> resourceIds) {
        getResourceJobs(keyspace).addAll(resourceIds);
        updateLastTimeJobAdded();
    }
    @Override
    public void deleteJobResource(String keyspace, String resourceId) {
        getResourceJobs(keyspace).remove(resourceId);
    }

    /**
     * @return the last time a job was added to the EngineCacheImpl.
     */
    public long getLastTimeJobAdded(){
        return lastTimeModified.get();
    }

    /**
     * Keep a record of the last time something was added to the EngineCacheImpl.
     */
    private void updateLastTimeJobAdded(){
        lastTimeModified.set(System.currentTimeMillis());
    }
}
