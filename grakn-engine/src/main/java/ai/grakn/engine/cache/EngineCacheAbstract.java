package ai.grakn.engine.cache;

import ai.grakn.concept.ConceptId;
import ai.grakn.graph.admin.ConceptCache;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public abstract class EngineCacheAbstract implements ConceptCache {
    private final AtomicLong lastTimeModified;

    EngineCacheAbstract(){
        lastTimeModified = new AtomicLong(System.currentTimeMillis());
    }

    @Override
    public long getNumJobs(String keyspace) {
        return getNumCastingJobs(keyspace) + getNumResourceJobs(keyspace);
    }

    @Override
    public long getNumCastingJobs(String keyspace) {
        return getNumJobsCount(getCastingJobs(keyspace));
    }

    @Override
    public long getNumResourceJobs(String keyspace) {
        return getNumJobsCount(getResourceJobs(keyspace));
    }

    private long getNumJobsCount(Map<String, Set<ConceptId>> cache){
        return cache.values().stream().mapToLong(Set::size).sum();
    }

    @Override
    public long getLastTimeJobAdded() {
        return lastTimeModified.get();
    }

    void updateLastTimeJobAdded(){
        lastTimeModified.set(System.currentTimeMillis());
    }
}
