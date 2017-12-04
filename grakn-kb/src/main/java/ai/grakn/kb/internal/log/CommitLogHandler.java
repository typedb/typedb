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

package ai.grakn.kb.internal.log;

import ai.grakn.Grakn;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.EngineCommunicator;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.Sets;
import mjson.Json;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * <p>
 *     Wraps a {@link CommitLog} and enables thread safe operations on the commit log.
 *     Speficially it ensure that the log is locked when trying to mutate it.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class CommitLogHandler {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final CommitLog commitLog = CommitLog.createThreadSafe();

    public CommitLog commitLog(){
        return commitLog;
    }

    public void addNewAttributes(Map<String, ConceptId> attributes){
        lockDataAddition(() -> attributes.forEach((key, value) -> {
            commitLog().attributes().merge(key, Sets.newHashSet(value), (v1, v2) -> {
                v1.addAll(v2);
                return v1;
            });
        }));
    }

    public void addNewInstances(Map<ConceptId, Long> instances){
        lockDataAddition(() -> instances.forEach((key, value) -> commitLog().instanceCount().merge(key, value, (v1, v2) -> v1 + v2)));
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

    public Json getFormattedLog(){
        return formatLog(commitLog().instanceCount(), commitLog().attributes());
    }

    /**
     * Submits the commit logs to the provided server address and under the provided {@link Keyspace}
     */
    public Optional<String> submit(String engineUri, Keyspace keyspace){
        if(commitLog().instanceCount().isEmpty() && commitLog().attributes().isEmpty()){
            return Optional.empty();
        }

        Optional<URI> endPoint = getCommitLogEndPoint(engineUri, keyspace);
        try{
            lock.writeLock().lock();
            String response = EngineCommunicator.contactEngine(endPoint, REST.HttpConn.POST_METHOD, getFormattedLog().toString());
            commitLog().clear();
            return Optional.of("Response from engine [" + response + "]");
        } finally {
            lock.writeLock().unlock();
        }
    }

    static Optional<URI> getCommitLogEndPoint(String engineUri, Keyspace keyspace) {
        if (Grakn.IN_MEMORY.equals(engineUri)) {
            return Optional.empty();
        }
        String path = REST.resolveTemplate(REST.WebPath.COMMIT_LOG_URI, keyspace.getValue());
        return Optional.of(UriBuilder.fromUri(new SimpleURI(engineUri).toURI()).path(path).build());
    }

    public static Json formatTxLog(Map<ConceptId, Long> instances, Map<String, ConceptId> attributes){
        Map<String, Set<ConceptId>> newAttributes = new ConcurrentHashMap<>();
        attributes.forEach((key, value) -> {
            newAttributes.put(key, Sets.newHashSet(value));
        });
        return formatLog(instances, newAttributes);
    }

    /**
     * Returns the Formatted Log which is uploaded to the server.
     * @return a formatted Json log
     */
    static Json formatLog(Map<ConceptId, Long> instances, Map<String, Set<ConceptId>> attributes){
        //TODO: Remove this hack (IN THIS PR):
        Map<String, Set<String>> map = attributes.entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().map(ConceptId::getValue).
                        collect(Collectors.toSet())));

        //Concepts In Need of Inspection
        Json conceptsForInspection = Json.object();
        conceptsForInspection.set(Schema.BaseType.ATTRIBUTE.name(), Json.make(map));

        //Types with instance changes
        Json typesWithInstanceChanges = Json.array();

        instances.forEach((key, value) -> {
            Json jsonObject = Json.object();
            jsonObject.set(REST.Request.COMMIT_LOG_CONCEPT_ID, key.getValue());
            jsonObject.set(REST.Request.COMMIT_LOG_SHARDING_COUNT, value);
            typesWithInstanceChanges.add(jsonObject);
        });

        //Final Commit Log
        Json formattedLog = Json.object();
        formattedLog.set(REST.Request.COMMIT_LOG_FIXING, conceptsForInspection);
        formattedLog.set(REST.Request.COMMIT_LOG_COUNTING, typesWithInstanceChanges);

        return formattedLog;
    }
}
