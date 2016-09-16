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

package io.mindmaps.engine.postprocessing;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cache {
    private final Map<String, Set<String>> castings;
    private final Map<String, Set<String>> resources;
    private final AtomicBoolean saveInProgress;

    private static Cache instance=null;

    public static synchronized Cache getInstance(){
        if(instance==null) instance=new Cache();
        return instance;
    }

    private Cache(){
        castings = new ConcurrentHashMap<>();
        resources = new ConcurrentHashMap<>();
        saveInProgress = new AtomicBoolean(false);
    }

    public boolean isSaveInProgress() {
        return saveInProgress.get();
    }

    //-------------------- Casting Jobs
    public Map<String, Set<String>> getCastingJobs() {
        return castings;
    }
    public void addJobCasting(String graphName, Set<String> conceptIds) {
        getCastingJobs().computeIfAbsent(graphName, (key) -> ConcurrentHashMap.newKeySet()).addAll(conceptIds);
    }
    public void deleteJobCasting(String graphName, String conceptId) {
        getCastingJobs().get(graphName).remove(conceptId);
    }

    //-------------------- Resource Jobs
    public Map<String, Set<String>> getResourceJobs() {
        return resources;
    }
    public void addJobResource(String graphName, Set<String> conceptIds) {
        getResourceJobs().computeIfAbsent(graphName, (key) -> ConcurrentHashMap.newKeySet()).addAll(conceptIds);
    }
    public void deleteJobResource(String graphName, String conceptId) {
        getResourceJobs().get(graphName).remove(conceptId);
    }


}
