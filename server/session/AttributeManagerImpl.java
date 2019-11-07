package grakn.core.server.session;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.server.AttributeManager;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.spark_project.jetty.util.ConcurrentHashSet;

public class AttributeManagerImpl implements AttributeManager {
    private final static int TIMEOUT_MINUTES_ATTRIBUTES_CACHE = 2;
    private final static int ATTRIBUTES_CACHE_MAX_SIZE = 10000;

    private final Cache<String, ConceptId> attributesCache;
    private final ConcurrentHashMap<String, Integer> ephemeralAttributeCache;
    private final ConcurrentHashSet<String> lockCandidates;

    AttributeManagerImpl(){
        this.attributesCache = CacheBuilder.newBuilder()
                .expireAfterAccess(TIMEOUT_MINUTES_ATTRIBUTES_CACHE, TimeUnit.MINUTES)
                .maximumSize(ATTRIBUTES_CACHE_MAX_SIZE)
                .build();

        this.ephemeralAttributeCache = new ConcurrentHashMap<>();
        this.lockCandidates = new ConcurrentHashSet<>();
    }

    @Override
    public Cache<String, ConceptId> attributesCache() {
        return attributesCache;
    }

    @Override
    public void ackAttributeInsert(String index, String txId) {
        ephemeralAttributeCache.compute(index, (ind, entry) -> {
            if (entry == null) return 1;
            else{
                lockCandidates.add(txId);
                return entry +1;
            }
        });
    }

    @Override
    public void ackAttributeDelete(String index) {
        ephemeralAttributeCache.merge(index, 0, (existingValue, zero) -> existingValue == 0? null : existingValue - 1);
    }


    @Override
    public void ackCommit(String txId) {

    }

    @Override
    public boolean needsLock(String txId) {
        //System.out.println(Thread.currentThread() + ":" + txId + " " + lockCandidates);
        boolean contains = lockCandidates.contains(txId);
        if (!contains) System.out.println("doesnt need a lock!!!!");
        return contains;
    }

    @Override
    public void printEphemeralCache() {
        ephemeralAttributeCache.entrySet().forEach(System.out::println);
    }

}
