/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.internal.log;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.kb.log.CommitLog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wraps a {@link CommitLog} and enables thread safe operations on the commit log.
 * Speficially it ensure that the log is locked when trying to mutate it.
 *
 * @author Grakn Warriors
 */
public class CommitLogHandler {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final CommitLog commitLog;

    public CommitLogHandler(Keyspace keyspace){
        this.commitLog = CommitLog.createThreadSafe(keyspace);
    }

    private CommitLog commitLog(){
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
}
