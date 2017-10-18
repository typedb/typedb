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

package ai.grakn.kb.internal;

import ai.grakn.Grakn;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.util.EngineCommunicator;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import mjson.Json;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * <p>
 *     Stores the commit log of a {@link ai.grakn.GraknTx}.
 * </p>
 *
 * <p>
 *     Stores the commit log of a {@link ai.grakn.GraknTx} which is uploaded to the server when the {@link ai.grakn.GraknSession} is closed.
 *     The commit log is also uploaded periodically to make sure that if a failure occurs the counts are still roughly maintained.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class CommitLog {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<ConceptId, Long> newInstanceCount = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> newAttributes = new ConcurrentHashMap<>();
    private final Set<ConceptId> relationshipsWithNewRolePlayers = ConcurrentHashMap.newKeySet();


    void addNewAttributes(Map<String, ConceptId> attributes){
        lockDataAddition(() -> attributes.forEach((key, value) -> {
            newAttributes.merge(key, Sets.newHashSet(value.getValue()), (v1, v2) -> {
                v1.addAll(v2);
                return v1;
            });
        }));
    }

    void addNewInstances(Map<ConceptId, Long> instances){
        lockDataAddition(() -> instances.forEach((key, value) -> newInstanceCount.merge(key, value, (v1, v2) -> v1 + v2)));
    }

    void addRelationshipsWithNewRolePlayers(Set<ConceptId> relationships) {
        lockDataAddition(() -> relationshipsWithNewRolePlayers.addAll(relationships));
    }

    /**
     * Read locks are used when acquiring the data.
     * This is to ensure we are not busy clearing the data during a commit log submission.
     *
     * @param dataAdder the data addition
     */
    private void lockDataAddition(Runnable dataAdder) {
        try{
            lock.readLock().lock();
            dataAdder.run();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void clear(){
        newInstanceCount.clear();
        newAttributes.clear();
    }

    public Json getFormattedLog(){
        return formatLog(newInstanceCount, newAttributes, relationshipsWithNewRolePlayers);
    }

    /**
     * Submits the commit logs to the provided server address and under the provided {@link Keyspace}
     */
    public Optional<String> submit(String engineUri, Keyspace keyspace){
        if(newInstanceCount.isEmpty() && newAttributes.isEmpty()){
            return Optional.empty();
        }

        String endPoint = getCommitLogEndPoint(engineUri, keyspace);
        try{
            lock.writeLock().lock();
            String response = EngineCommunicator.contactEngine(endPoint, REST.HttpConn.POST_METHOD, getFormattedLog().toString());
            clear();
            return Optional.of("Response from engine [" + response + "]");
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static String getCommitLogEndPoint(String engineUri, Keyspace keyspace) {
        if (Grakn.IN_MEMORY.equals(engineUri)) {
            return Grakn.IN_MEMORY;
        }
        return engineUri + REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + keyspace;
    }

    static Json formatTxLog(Map<ConceptId, Long> instances, Map<String, ConceptId> attributes, Set<ConceptId> relationships){
        Map<String, Set<String>> newAttributes = new ConcurrentHashMap<>();
        attributes.forEach((key, value) -> {
            newAttributes.put(key, Sets.newHashSet(value.getValue()));
        });
        return formatLog(instances, newAttributes, relationships);
    }

    /**
     * Returns the Formatted Log which is uploaded to the server.
     * @return a formatted Json log
     */
    private static Json formatLog(Map<ConceptId, Long> instances, Map<String, Set<String>> attributes, Set<ConceptId> relationships){
        Json formattedLog = Json.object();

        //Concepts In Need of Inspection
        if(!relationships.isEmpty() || !attributes.isEmpty()){
            Json conceptsForInspection = Json.object();
            if(!attributes.isEmpty()){
                conceptsForInspection.set(Schema.BaseType.ATTRIBUTE.name(), Json.make(attributes));
            }

            if(!relationships.isEmpty()){
                Set<String> relIds = relationships.stream().map(ConceptId::getValue).collect(Collectors.toSet());
                conceptsForInspection.set(Schema.BaseType.RELATIONSHIP.name(), Json.make(relIds));
            }

            formattedLog.set(REST.Request.COMMIT_LOG_FIXING, conceptsForInspection);
        }

        //Types with instance changes
        Json typesWithInstanceChanges = Json.array();

        instances.forEach((key, value) -> {
            Json jsonObject = Json.object();
            jsonObject.set(REST.Request.COMMIT_LOG_CONCEPT_ID, key.getValue());
            jsonObject.set(REST.Request.COMMIT_LOG_SHARDING_COUNT, value);
            typesWithInstanceChanges.add(jsonObject);
        });

        formattedLog.set(REST.Request.COMMIT_LOG_COUNTING, typesWithInstanceChanges);

        return formattedLog;
    }
}
