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
