/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cache {
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Set<String>>> castingsToPostProcess;
    private final ConcurrentHashMap<String, Set<String>> assertionsToPostProcess;
    private final AtomicBoolean saveInProgress;

    private static Cache instance=null;

    public static synchronized Cache getInstance(){
        if(instance==null) instance=new Cache();
        return instance;
    }

    private Cache(){
        castingsToPostProcess = new ConcurrentHashMap<>();
        assertionsToPostProcess = new ConcurrentHashMap<>();
        saveInProgress = new AtomicBoolean(false);
    }

    public boolean isSaveInProgress() {
        return saveInProgress.get();
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<String, Set<String>>> getCastingJobs() {
        return castingsToPostProcess;
    }

    public ConcurrentHashMap<String, Set<String>> getAssertionJobs() {
        return assertionsToPostProcess;
    }


    public void addJobCasting(String type, String key, String castingId) {
        Map<String, Set<String>> typeMap = castingsToPostProcess.computeIfAbsent(type, k -> new ConcurrentHashMap<>());
        Set<String> innerSet = typeMap.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
        innerSet.add(castingId);
    }

    public void addJobAssertion(String type, String id) {
        Set<String> typeSet = assertionsToPostProcess.computeIfAbsent(type, k -> ConcurrentHashMap.newKeySet());
        typeSet.add(id);
    }

    public void deleteJobCasting(String type, String key) {
        getCastingJobs().get(type).remove(key);
    }

    public void deleteJobAssertion(String type, String key) {
        getAssertionJobs().get(type).remove(key);
    }

}
