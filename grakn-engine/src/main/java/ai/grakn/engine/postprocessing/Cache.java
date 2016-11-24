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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Cache {
    private final Map<String, Set<String>> castings;
    private final Map<String, Set<String>> resources;
    private final AtomicBoolean saveInProgress;

    private static Cache instance=null;
    private AtomicLong lastTimeModified;

    public static synchronized Cache getInstance(){
        if(instance==null) instance=new Cache();
        return instance;
    }

    private Cache(){
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
    public Set<String> getCastingJobs(String keyspace) {
        keyspace = keyspace.toLowerCase();
        return castings.computeIfAbsent(keyspace, (key) -> ConcurrentHashMap.newKeySet());
    }
    public void addJobCasting(String keyspace, Set<String> conceptIds) {
        getCastingJobs(keyspace).addAll(conceptIds);
        updateLastTimeJobAdded();
    }
    public void deleteJobCasting(String keyspace, String conceptId) {
        getCastingJobs(keyspace).remove(conceptId);
    }

    //-------------------- Resource Jobs
    public Set<String> getResourceJobs(String keyspace) {
        keyspace = keyspace.toLowerCase();
        return resources.computeIfAbsent(keyspace, (key) -> ConcurrentHashMap.newKeySet());
    }
    public void addJobResource(String keyspace, Set<String> conceptIds) {
        getResourceJobs(keyspace).addAll(conceptIds);
        updateLastTimeJobAdded();
    }
    public void deleteJobResource(String keyspace, String conceptId) {
        getResourceJobs(keyspace).remove(conceptId);
    }

    /**
     * @return the last time a job was added to the Cache.
     */
    public long getLastTimeJobAdded(){
        return lastTimeModified.get();
    }

    /**
     * Keep a record of the last time something was added to the Cache.
     */
    private void updateLastTimeJobAdded(){
        lastTimeModified.set(System.currentTimeMillis());
    }
}
