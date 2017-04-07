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
import ai.grakn.concept.TypeLabel;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 *
 *    This stand alone version keeps everything in memory when running a single engine.
 * </p>
 *
 * @author fppt
 */
public class EngineCacheStandAlone extends EngineCacheAbstract{
    private final AtomicLong lastTimeModified;
    //These are maps of keyspaces to indices to vertex ids
    private final Map<String, Map<String, Set<ConceptId>>> castings;
    private final Map<String, Map<String, Set<ConceptId>>> resources;
    private final Map<String, Map<TypeLabel, Long>> instanceCounts;

    private static EngineCacheStandAlone instance=null;

    public static synchronized EngineCacheStandAlone getCache(){
        if(instance == null) instance = new EngineCacheStandAlone();
        return instance;
    }

    private EngineCacheStandAlone(){
        lastTimeModified = new AtomicLong(System.currentTimeMillis());
        castings = new ConcurrentHashMap<>();
        resources = new ConcurrentHashMap<>();
        instanceCounts = new ConcurrentHashMap<>();
    }

    @Override
    public Set<String> getKeyspaces(){
        Set<String> keyspaces = new HashSet<>();
        keyspaces.addAll(castings.keySet());
        keyspaces.addAll(resources.keySet());
        keyspaces.addAll(instanceCounts.keySet());
        return keyspaces;
    }

    @Override
    public long getLastTimeJobAdded() {
        return lastTimeModified.get();
    }

    void updateLastTimeJobAdded(){
        lastTimeModified.set(System.currentTimeMillis());
    }

    //-------------------- Casting Jobs
    @Override
    public Map<String, Set<ConceptId>> getCastingJobs(String keyspace) {
        return castings.computeIfAbsent(keyspace, key -> new ConcurrentHashMap<>());
    }

    @Override
    public void addJobCasting(String keyspace, String castingIndex, ConceptId castingId) {
        addJob(castings, keyspace, castingIndex, castingId);
    }

    @Override
    public void deleteJobCasting(String keyspace, String castingIndex, ConceptId castingId) {
        deleteJob(castings, keyspace, castingIndex, castingId);
    }

    //-------------------- Resource Jobs
    @Override
    public Map<String, Set<ConceptId>> getResourceJobs(String keyspace) {
        return resources.computeIfAbsent(keyspace, key -> new ConcurrentHashMap<>());
    }

    @Override
    public void addJobResource(String keyspace, String resourceIndex, ConceptId resourceId) {
        addJob(resources, keyspace, resourceIndex, resourceId);
    }

    @Override
    public void deleteJobResource(String keyspace, String resourceIndex, ConceptId resourceId) {
        deleteJob(resources, keyspace, resourceIndex, resourceId);
    }

    private void addJob(Map<String, Map<String, Set<ConceptId>>> cache, String keyspace, String index, ConceptId vertexId){
        updateLastTimeJobAdded();

        Map<String, Set<ConceptId>> keyspaceSpecificCache = cache.computeIfAbsent(keyspace, key -> new ConcurrentHashMap<>());
        Set<ConceptId> indexSpecificSet = keyspaceSpecificCache.computeIfAbsent(index, i -> ConcurrentHashMap.newKeySet());
        indexSpecificSet.add(vertexId);
    }

    private void deleteJob(Map<String, Map<String, Set<ConceptId>>> cache, String keyspace, String index, ConceptId vertexId){
        updateLastTimeJobAdded();

        Map<String, Set<ConceptId>> keyspaceSpecificCache = cache.get(keyspace);
        if(keyspaceSpecificCache != null){
            Set<ConceptId> indexSpecificSet = keyspaceSpecificCache.get(index);
            indexSpecificSet.remove(vertexId);
        }
    }

    @Override
    public void clearJobSetResources(String keyspace, String conceptIndex){
        clearJobSet(conceptIndex, resources.get(keyspace));
    }

    @Override
    public void clearJobSetCastings(String keyspace, String conceptIndex){
        clearJobSet(conceptIndex, castings.get(keyspace));
    }
    private void clearJobSet(String conceptIndex, Map<String, Set<ConceptId>> cache){
        updateLastTimeJobAdded();

        if(cache.containsKey(conceptIndex) && cache.get(conceptIndex).isEmpty()){
           cache.remove(conceptIndex);
        }
    }

    @Override
    public void clearAllJobs(String keyspace){
        updateLastTimeJobAdded();
        if(castings.containsKey(keyspace)) castings.remove(keyspace);
        if(resources.containsKey(keyspace)) resources.remove(keyspace);
        if(instanceCounts.containsKey(keyspace)) instanceCounts.remove(keyspace);
    }

    //-------------------- Instance Count Jobs

    @Override
    public Map<TypeLabel, Long> getInstanceCountJobs(String keyspace) {
        return instanceCounts.computeIfAbsent(keyspace, (key) -> new ConcurrentHashMap<>());
    }

    @Override
    public void addJobInstanceCount(String keyspace, TypeLabel name, long instanceCount) {
        getInstanceCountJobs(keyspace).compute(name, (key, value) -> value == null ? instanceCount : value + instanceCount);
    }

    @Override
    public void deleteJobInstanceCount(String keyspace, TypeLabel name) {
        getInstanceCountJobs(keyspace).remove(name);
    }
}
