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

package ai.grakn.test.engine.cache;

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.graph.admin.ConceptCache;
import ai.grakn.test.EngineContext;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

//NOTE: This test is only in grakn-test because it needs a running ZK
//Ideally this should be moved to the grakn-engine module
public class EngineCacheDistributedTest {
    private ConceptCache cache;

    //We do this just so we can start ZK
    @ClassRule
    public static final EngineContext engine = EngineContext.startSingleQueueServer();

    @Before
    public void setupCache(){
        cache = EngineCacheProvider.getCache();
    }

    @After
    public void clearCache(){
        cache.getKeyspaces().forEach(k -> cache.clearAllJobs(k));
    }

    @Test
    public void whenAddingJobsToCache_EnsureCachContainsJobs(){
        String keyspace = "my_fake_keyspace";

        //Fake Commit Logs
        Map<String, Set<ConceptId>> castingsFake = createFakeInternalConceptLog("Casting_Index_", 10, 5);
        Map<String, Set<ConceptId>> resourcesFake = createFakeInternalConceptLog("Casting_Index_", 6, 11);

        //Add stuff to the cache
        transferFakeCacheIntoRealCache(castingsFake, (index, id) -> cache.addJobCasting(keyspace, index, id));
        transferFakeCacheIntoRealCache(resourcesFake, (index, id) -> cache.addJobCasting(keyspace, index, id));

        //Check stuff is in cache
        Map<String, Set<ConceptId>> castingsReal = cache.getCastingJobs(keyspace);
        Map<String, Set<ConceptId>> resourcesReal = cache.getResourceJobs(keyspace);

        assertThat(castingsReal.keySet(), containsInAnyOrder(castingsFake.keySet()));
        assertThat(resourcesReal.keySet(), containsInAnyOrder(resourcesFake.keySet()));

        checkContentsOfCache(castingsFake, castingsReal);
        checkContentsOfCache(resourcesFake, resourcesReal);
    }
    private Map<String, Set<ConceptId>> createFakeInternalConceptLog(String indexPrefix, int numIndex, int numJobs){
        Map<String, Set<ConceptId>> internalCache = new HashMap<>();
        for(int i = 0; i < numIndex; i ++){
            for(int j = 0; j < numJobs; j++){
                internalCache.computeIfAbsent(indexPrefix + "_" + i, (key) -> new HashSet<>()).add(ConceptId.of(j));
            }
        }
        return internalCache;
    }
    private void transferFakeCacheIntoRealCache(Map<String, Set<ConceptId>> fakeCache, BiConsumer<String, ConceptId> cacheUpdater){
        fakeCache.entrySet().forEach(entry-> {
            String index = entry.getKey();
            entry.getValue().forEach(id -> cacheUpdater.accept(index, id));
        });
    }
    private void checkContentsOfCache(Map<String, Set<ConceptId>> realCache, Map<String, Set<ConceptId>> fakeCache){
        fakeCache.keySet().forEach(key-> assertThat(realCache.get(key), containsInAnyOrder(fakeCache.get(key))));
    }

}
