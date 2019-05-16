package grakn.core.server.statistics;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class KeyspaceStatistics {

    private ConcurrentHashMap<String, Long> instanceCounts;

    public KeyspaceStatistics() {
        instanceCounts = new ConcurrentHashMap<>();
    }

    public long count(String label) {
        return instanceCounts.getOrDefault(label, 0L);
    }

    public void commit(UncomittedStatisticsDelta statisticsDelta) {
        HashMap<String, Long> deltaMap = statisticsDelta.instanceDeltas();
        // atomically update entries using addition, which is commutative so as long as each update
        // happens atomically the ordering of the statistics commits doesn't matter
        deltaMap.forEach((key, value) -> instanceCounts.merge(key, value, (prior, delta) -> prior + delta));
    }
}
