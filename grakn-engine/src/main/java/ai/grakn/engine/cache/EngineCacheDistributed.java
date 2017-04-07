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
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.exception.EngineStorageException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_CACHE_CONCEPT_IDS;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_CACHE_EXACT_JOB;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_CACHE_JOB_TYPE;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_CACHE_KEYSPACES;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_CACHE_TYPE_INSTANCE_COUNT;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_CACHE_UPDATE_TIME;
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
 *    We cannot rely on {@link ai.grakn.concept.ConceptId}s because the indexed lookup maybe faulty with
 *    vertices in need of post processing.
 *
 *    This distributed version persists everything using {@link ai.grakn.engine.tasks.manager.ZookeeperConnection}
 * </p>
 *
 * @author fppt
 */
public class EngineCacheDistributed extends EngineCacheAbstract{
    private static final String COUNTING_JOB = "counting";
    private static final String RESOURCE_JOB = "resources";
    private static final String CASTING_JOB = "castings";
    private static EngineCacheDistributed instance = null;
    private final ZookeeperConnection zookeeper;

    private EngineCacheDistributed(ZookeeperConnection zookeeper){
        this.zookeeper = zookeeper;
    }

    public static synchronized EngineCacheDistributed init(ZookeeperConnection connection){
        if(instance == null) instance = new EngineCacheDistributed(connection);
        return instance;
    }

    private String getPathJobRoot(String jobType, String keyspace){
        return format(ENGINE_CACHE_JOB_TYPE, keyspace, jobType);
    }
    private String getPathConceptIds(String jobType, String keyspace, String index){
        return format(ENGINE_CACHE_CONCEPT_IDS, keyspace, jobType, index);
    }
    private String getPathExactJob(String jobType, String keyspace, String index, ConceptId conceptId){
        return format(ENGINE_CACHE_EXACT_JOB, keyspace, jobType, index, conceptId.getValue());
    }
    private String getPathTypeInstanceCount(String keyspace, TypeLabel name){
        return format(ENGINE_CACHE_TYPE_INSTANCE_COUNT, keyspace, COUNTING_JOB, name);
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

    private void deleteObjectFromZookeeper(String path){
        try{
            if(zookeeper.connection().checkExists().forPath(path) != null) {
                zookeeper.connection().delete().forPath(path);
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

    private Optional<List<String>> getChildrenFromZookeeper(String path){
        try {
            if(zookeeper.connection().checkExists().forPath(path) == null) return Optional.empty();
            return Optional.of(zookeeper.connection().getChildren().forPath(path));
        } catch (Exception e) {
            throw new EngineStorageException(e);
        }
    }

    @Override
    public void deleteJobCasting(String keyspace, String castingIndex, ConceptId castingId) {
        deleteJob(CASTING_JOB, keyspace, castingIndex, castingId);
    }
    @Override
    public void deleteJobResource(String keyspace, String resourceIndex, ConceptId resourceId) {
        deleteJob(RESOURCE_JOB, keyspace, resourceIndex, resourceId);
    }
    private void deleteJob(String jobType, String keyspace, String index, ConceptId id){
        updateLastTimeJobAdded();
        deleteObjectFromZookeeper(getPathExactJob(jobType, keyspace, index, id));
    }

    @Override
    public Map<String, Set<ConceptId>> getCastingJobs(String keyspace) {
        return getJobs(CASTING_JOB, keyspace);
    }
    @Override
    public Map<String, Set<ConceptId>> getResourceJobs(String keyspace) {
        return getJobs(RESOURCE_JOB, keyspace);
    }
    private Map<String, Set<ConceptId>> getJobs(String jopType, String keyspace){
        Map<String, Set<ConceptId>> jobs = new HashMap<>();
        Optional<List<String>> indices = getChildrenFromZookeeper(getPathJobRoot(jopType, keyspace));

        if(indices.isPresent()){
            for (String index : indices.get()) {
                jobs.put(index, new HashSet<>());
                Optional<List<String>> ids = getChildrenFromZookeeper(getPathConceptIds(jopType, keyspace, index));
                if(ids.isPresent()){
                    ids.get().forEach(id -> jobs.get(index).add(ConceptId.of(id)));
                } else {
                    cleanJobSet(jopType, keyspace, index, false); //Have an empty index. Time to kill it.
                }
            }
        }

        return jobs;
    }

    @Override
    public void addJobCasting(String keyspace, String castingIndex, ConceptId castingId) {
        addJob(CASTING_JOB, keyspace, castingIndex, castingId);
    }
    @Override
    public void addJobResource(String keyspace, String resourceIndex, ConceptId resourceId) {
        addJob(RESOURCE_JOB, keyspace, resourceIndex, resourceId);
    }
    private void addJob(String jobType, String keyspace, String index, ConceptId id){
        updateLastTimeJobAdded();
        updateKeyspaces(keyspace);
        writeObjectToZookeeper(getPathExactJob(jobType, keyspace, index, id), id.getValue());
    }

    @Override
    public void clearJobSetResources(String keyspace, String conceptIndex) {
        cleanJobSet(RESOURCE_JOB, keyspace, conceptIndex, false);
    }
    @Override
    public void clearJobSetCastings(String keyspace, String conceptIndex) {
        cleanJobSet(CASTING_JOB, keyspace, conceptIndex, false);
    }
    private void cleanJobSet(String jobType, String keyspace, String index, boolean force){
        String idPath = getPathConceptIds(jobType, keyspace, index);
        Optional<List<String>> ids = getChildrenFromZookeeper(idPath);

        if(force && ids.isPresent()){
            ids.get().forEach(id -> deleteJob(jobType, keyspace, index, ConceptId.of(id)));
        }

        if(force || ids.isPresent() && ids.get().isEmpty()){
            deleteObjectFromZookeeper(idPath);
        }
    }

    @Override
    public void clearAllJobs(String keyspace) {
        updateLastTimeJobAdded();
        clearAllJobs(RESOURCE_JOB, keyspace);
        clearAllJobs(CASTING_JOB, keyspace);
    }

    @Override
    public long getLastTimeJobAdded() {
        Optional<Object> value = getObjectFromZookeeper(ENGINE_CACHE_UPDATE_TIME);
        if(value.isPresent()){
            return (long) value.get();
        }
        return 0L;
    }

    private void updateLastTimeJobAdded(){
        writeObjectToZookeeper(ENGINE_CACHE_UPDATE_TIME, System.currentTimeMillis());
    }

    private void clearAllJobs(String jobType, String keyspace){
        Optional<List<String>> indices = getChildrenFromZookeeper(getPathJobRoot(jobType, keyspace));
        if(indices.isPresent()){
            indices.get().forEach(index -> cleanJobSet(jobType, keyspace, index, true));
        }
    }

    //-------------------- Instance Count Jobs

    @Override
    public Map<TypeLabel, Long> getInstanceCountJobs(String keyspace) {
        Map<TypeLabel, Long> results = new HashMap<>();
        Optional<List<String>> types = getChildrenFromZookeeper(getPathJobRoot(COUNTING_JOB, keyspace));

        if(types.isPresent()){
            types.get().forEach(name -> {
                String pathTypeIntanceCount = getPathTypeInstanceCount(keyspace, TypeLabel.of(name));
                Optional<Object> value = getObjectFromZookeeper(pathTypeIntanceCount);
                if(value.isPresent()){
                    results.put(TypeLabel.of(name), (Long) value.get());
                } else {
                    deleteObjectFromZookeeper(pathTypeIntanceCount); //We have a type saved with no count. Kill it
                }
            });
        }

        return results;
    }

    @Override
    public void addJobInstanceCount(String keyspace, TypeLabel name, long instanceCount) {
        String path = getPathTypeInstanceCount(keyspace, name);
        Optional<Object> currentValue = getObjectFromZookeeper(path);
        long newValue = instanceCount;

        if(currentValue.isPresent()){
            newValue +=  (long) currentValue.get();
        }

        writeObjectToZookeeper(path, newValue);
    }

    @Override
    public void deleteJobInstanceCount(String keyspace, TypeLabel name) {
        deleteObjectFromZookeeper(getPathTypeInstanceCount(keyspace, name));
    }
}
