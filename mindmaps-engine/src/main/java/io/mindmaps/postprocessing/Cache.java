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

package io.mindmaps.postprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cache {
    private final Map<String, Set<String>> castingsToPostProcess;
    private final Map<String, Set<String>> relationsToPostProcess;
    private final AtomicBoolean saveInProgress;

    private final Logger LOG = LoggerFactory.getLogger(Cache.class);


    private static Cache instance=null;

    public static synchronized Cache getInstance(){
        if(instance==null) instance=new Cache();
        return instance;
    }

    private Cache(){
        castingsToPostProcess = new ConcurrentHashMap<>();
        relationsToPostProcess = new ConcurrentHashMap<>();
        saveInProgress = new AtomicBoolean(false);
    }

    public boolean isSaveInProgress() {
        return saveInProgress.get();
    }

    public Map<String, Set<String>> getCastingJobs() {
        return castingsToPostProcess;
    }

    public Map<String, Set<String>> getRelationJobs() {
        return relationsToPostProcess;
    }

    public void addJobCasting(String graphName, Set<String> conceptIds) {
        getCastingJobs().computeIfAbsent(graphName, (key) -> ConcurrentHashMap.newKeySet()).addAll(conceptIds);
    }
    public void addJobCasting(String graphName, String conceptId) {
        getCastingJobs().computeIfAbsent(graphName, (key) ->  ConcurrentHashMap.newKeySet()).add(conceptId);
    }

    public void addJobRelation(String graphName, Set<String> conceptIds) {
        getCastingJobs().computeIfAbsent(graphName, (key) ->  ConcurrentHashMap.newKeySet()).addAll(conceptIds);
    }
    public void addJobRelation(String graphName, String conceptId) {
        getRelationJobs().computeIfAbsent(graphName, (key) -> ConcurrentHashMap.newKeySet()).add(conceptId);
    }

    public void deleteJobCasting(String graphName, String conceptId) {
        getCastingJobs().get(graphName).remove(conceptId);
    }

    public void deleteJobRelation(String graphName, String conceptId) {
        getRelationJobs().get(graphName).remove(conceptId);
    }

    public void addCacheJobs(String graphName, Set<String> castings, Set<String> relations){
        addJobCasting(graphName, castings);
        addJobRelation(graphName, relations);
    }

}
