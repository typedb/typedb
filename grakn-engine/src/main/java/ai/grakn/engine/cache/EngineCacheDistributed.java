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
import ai.grakn.exception.EngineStorageException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_CACHE_JOBS;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_CACHE_KEYSPACES;
import static java.lang.String.format;
import static org.apache.commons.lang.SerializationUtils.deserialize;
import static org.apache.spark.util.Utils.serialize;

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
//TODO: This class may be in need of sever optimisation. Constantly serialising and deserialising maps and sets is likely to be slow.
public class EngineCacheDistributed extends EngineCacheAbstract{
    private static EngineCacheDistributed instance = null;
    private final ZookeeperConnection zookeeper;


    private EngineCacheDistributed(ZookeeperConnection zookeeper){
        this.zookeeper = zookeeper;
    }

    public static synchronized EngineCacheDistributed init(ZookeeperConnection connection){
        if(instance == null) instance = new EngineCacheDistributed(connection);
        return instance;
    }

    private String getPathCastings(String keyspace){
        return format(ENGINE_CACHE_JOBS, keyspace, "castings");
    }

    private String getPathResources(String keyspace){
        return format(ENGINE_CACHE_JOBS, keyspace, "resources");
    }

    @Override
    public Set<String> getKeyspaces() {
        //noinspection unchecked
        return (Set<String>) getObjectFromZookeeper(ENGINE_CACHE_KEYSPACES).orElse(new HashSet<>());
    }

    private void updateKeyspaces(String newKeyspace){
        Set<String> currentKeyspaces = getKeyspaces();
        if(!currentKeyspaces.contains(newKeyspace)){
            currentKeyspaces.add(newKeyspace);
            writeObjectToZookeeper(ENGINE_CACHE_KEYSPACES, currentKeyspaces);
        }
    }

    private void writeObjectToZookeeper(String path, Object object){
        try{
            if(zookeeper.connection().checkExists().forPath(path) == null) {
                zookeeper.connection().create().creatingParentContainersIfNeeded().forPath(path, serialize(object));
            } else {
                zookeeper.connection().inTransaction().setData().forPath(path, serialize(object)).and().commit();
            }
        } catch (Exception e) {
            throw new EngineStorageException(e);
        }
    }

    private Optional<Object> getObjectFromZookeeper(String path){
        try {
            if(zookeeper.connection().checkExists().forPath(path) == null) return Optional.empty();
            return Optional.of(deserialize(zookeeper.connection().getData().forPath(path)));
        } catch (Exception e) {
            throw new EngineStorageException(e);
        }
    }

    @Override
    public void deleteJobCasting(String keyspace, String castingIndex, ConceptId castingId) {
        deleteJob(getPathCastings(keyspace), castingIndex, castingId);
    }
    @Override
    public void deleteJobResource(String keyspace, String resourceIndex, ConceptId resourceId) {
        deleteJob(getPathResources(keyspace), resourceIndex, resourceId);
    }
    private void deleteJob(String path, String index, ConceptId id){
        Map<String, Set<ConceptId>> currentJobs = getJobs(path);
        if(currentJobs.containsKey(index) && currentJobs.get(index).contains(id)){
            currentJobs.get(index).remove(id);
            writeObjectToZookeeper(path, currentJobs);
        }
    }

    @Override
    public Map<String, Set<ConceptId>> getCastingJobs(String keyspace) {
        return getJobs(getPathCastings(keyspace));
    }
    @Override
    public Map<String, Set<ConceptId>> getResourceJobs(String keyspace) {
        return getJobs(getPathResources(keyspace));
    }
    private Map<String, Set<ConceptId>> getJobs(String path){
        //noinspection unchecked
        return (Map<String, Set<ConceptId>>) getObjectFromZookeeper(path).orElse(new HashMap<>());
    }

    @Override
    public void addJobCasting(String keyspace, String castingIndex, ConceptId castingId) {
        addJob(getPathCastings(keyspace), keyspace, castingIndex, castingId);
    }
    @Override
    public void addJobResource(String keyspace, String resourceIndex, ConceptId resourceId) {
        addJob(getPathResources(keyspace), keyspace, resourceIndex, resourceId);
    }
    private void addJob(String jobPath, String keyspace, String index, ConceptId id){
        updateLastTimeJobAdded();
        updateKeyspaces(keyspace);
        Map<String, Set<ConceptId>> currentJobs = getJobs(jobPath);
        Set<ConceptId> currentIds = currentJobs.computeIfAbsent(index, (k) -> new HashSet<>());
        if(!currentIds.contains(id)){
            currentIds.add(id);
            writeObjectToZookeeper(jobPath, currentJobs);
        }
    }

    @Override
    public void clearJobSetResources(String keyspace, String conceptIndex) {
        cleanJobSet(getPathResources(keyspace), conceptIndex);
    }
    @Override
    public void clearJobSetCastings(String keyspace, String conceptIndex) {
        cleanJobSet(getPathCastings(keyspace), conceptIndex);
    }
    private void cleanJobSet(String path, String index){
        Map<String, Set<ConceptId>> currentJobs = getJobs(path);
        if(currentJobs.containsKey(index)){
            currentJobs.remove(index);
            writeObjectToZookeeper(path, currentJobs);
        }
    }

    @Override
    public void clearAllJobs(String keyspace) {
        updateLastTimeJobAdded();
        synchronized (this) {
            writeObjectToZookeeper(getPathResources(keyspace), new HashMap<>());
            writeObjectToZookeeper(getPathCastings(keyspace), new HashMap<>());
        }
    }
}
