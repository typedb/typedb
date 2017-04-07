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
import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.cache.EngineCacheStandAlone;
import ai.grakn.graph.admin.ConceptCache;
import ai.grakn.test.EngineContext;
import org.junit.ClassRule;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

//NOTE: This test is only in grakn-test because it needs a running ZK
//Ideally this should be moved to the grakn-engine module
@RunWith(Theories.class)
public class EngineCacheTest {

    //We do this just so we can start ZK
    @ClassRule
    public static final EngineContext engine = EngineContext.startSingleQueueServer();

    @DataPoints
    public static Caches[] configValues = Caches.values();

    private static enum Caches{
        DISTRIBUTED, STAND_ALONE;
    }

    private ConceptCache getCache(Caches caches){
        switch(caches){
            case DISTRIBUTED:
                return EngineCacheProvider.getCache();
            case STAND_ALONE:
                return EngineCacheStandAlone.getCache();
        }
        throw new RuntimeException("Invalid cache [" + caches + "]");
    }

    @Theory
    public void whenDeletingFixingJobsFromCache_EnsureJobsAreNoLongerInCache(Caches caches){
        ConceptCache cache = getCache(caches);
        String keyspace = "my_fake_keyspace";

        //Fake Commit Logs
        Map<String, Set<ConceptId>> castingsFake = createFakeInternalConceptLog("Casting_Index_", 10, 5);
        Map<String, Set<ConceptId>> resourcesFake = createFakeInternalConceptLog("Resource_Index_", 6, 11);

        //Add stuff to the cache
        transferFakeCacheIntoRealCache(castingsFake, (index, id) -> cache.addJobCasting(keyspace, index, id));
        transferFakeCacheIntoRealCache(resourcesFake, (index, id) -> cache.addJobResource(keyspace, index, id));

        //Delete Random Jobs 1
        String deletedIndex1 = "Casting_Index_1";
        cache.deleteJobCasting(keyspace, deletedIndex1, ConceptId.of(0));
        cache.deleteJobCasting(keyspace, deletedIndex1, ConceptId.of(1));
        cache.deleteJobCasting(keyspace, deletedIndex1, ConceptId.of(2));
        cache.deleteJobCasting(keyspace, deletedIndex1, ConceptId.of(3));
        cache.deleteJobCasting(keyspace, deletedIndex1, ConceptId.of(4));
        assertThat(cache.getCastingJobs(keyspace).get(deletedIndex1), is(empty()));

        //Delete Random Jobs 2
        String deletedIndex2 = "Resource_Index_3";
        ConceptId deletedId1 = ConceptId.of(0);
        ConceptId deletedId2 = ConceptId.of(4);
        ConceptId deletedId3 = ConceptId.of(6);
        cache.deleteJobResource(keyspace, deletedIndex2, deletedId1);
        cache.deleteJobResource(keyspace, deletedIndex2, deletedId2);
        cache.deleteJobResource(keyspace, deletedIndex2, deletedId3);
        assertFalse("Job " + deletedId1 + " was not deleted form cache", cache.getResourceJobs(keyspace).get(deletedIndex2).contains(deletedId1));
        assertFalse("Job " + deletedId2 + " was not deleted form cache", cache.getResourceJobs(keyspace).get(deletedIndex2).contains(deletedId2));
        assertFalse("Job " + deletedId3 + " was not deleted form cache", cache.getResourceJobs(keyspace).get(deletedIndex2).contains(deletedId3));

        //Clear Jobs 1
        String deletedIndex3 = "Casting_Index_2";
        cache.deleteJobCasting(keyspace, deletedIndex3, ConceptId.of(0));
        cache.deleteJobCasting(keyspace, deletedIndex3, ConceptId.of(1));
        cache.deleteJobCasting(keyspace, deletedIndex3, ConceptId.of(2));
        cache.deleteJobCasting(keyspace, deletedIndex3, ConceptId.of(3));
        cache.deleteJobCasting(keyspace, deletedIndex3, ConceptId.of(4));
        cache.clearJobSetCastings(keyspace, deletedIndex3);
        assertFalse("Index [" + deletedIndex3 + "] was not cleared form the cache", cache.getCastingJobs(keyspace).containsKey(deletedIndex3));

        //Clear Jobs 2
        String deletedIndex4 = "Resource_Index_3";
        cache.clearJobSetResources(keyspace, deletedIndex4);
        assertTrue("Index [" + deletedIndex4 + "] was cleared form the cache even though it had pending jobs", cache.getResourceJobs(keyspace).containsKey(deletedIndex4));

        //Clear all Jobs
        cache.clearAllJobs(keyspace);
        assertTrue(cache.getCastingJobs(keyspace).isEmpty());
        assertTrue(cache.getResourceJobs(keyspace).isEmpty());
    }

    @Theory
    public void whenAddingFixingJobsToCacheOfSameKeyspace_EnsureCacheContainsJobs(Caches caches){
        ConceptCache cache = getCache(caches);
        String keyspace = "my_fake_keyspace";

        //Fake Commit Logs
        Map<String, Set<ConceptId>> castingsFake = createFakeInternalConceptLog("Casting_Index_", 10, 5);
        Map<String, Set<ConceptId>> resourcesFake = createFakeInternalConceptLog("Resource_Index_", 6, 11);

        long castingsFakeCount = castingsFake.values().stream().mapToLong(Set::size).sum();
        long resourcesFakeCount = resourcesFake.values().stream().mapToLong(Set::size).sum();

        //Add stuff to the cache
        transferFakeCacheIntoRealCache(castingsFake, (index, id) -> cache.addJobCasting(keyspace, index, id));
        transferFakeCacheIntoRealCache(resourcesFake, (index, id) -> cache.addJobResource(keyspace, index, id));

        //Check stuff is in cache
        checkContentsOfCache(cache.getCastingJobs(keyspace), castingsFake);
        checkContentsOfCache(cache.getResourceJobs(keyspace), resourcesFake);

        //Check counts
        assertEquals(castingsFakeCount, cache.getNumCastingJobs(keyspace));
        assertEquals(resourcesFakeCount, cache.getNumResourceJobs(keyspace));
        assertEquals(castingsFakeCount + resourcesFakeCount, cache.getNumJobs(keyspace));
    }

    @Theory
    public void whenAddingFixingJobsToCacheOfDifferentKeySpaces_EnsureCacheContainsJob(Caches caches){
        ConceptCache cache = getCache(caches);
        String keyspace1 = "key1";
        String keyspace2 = "key2";

        //Fake Commit Logs
        Map<String, Set<ConceptId>> key1_castingsFake = createFakeInternalConceptLog("Casting_Index_", 2, 1);
        Map<String, Set<ConceptId>> key1_resourcesFake = createFakeInternalConceptLog("Resource_Index_", 3, 6);
        Map<String, Set<ConceptId>> key2_castingsFake = createFakeInternalConceptLog("Casting_Index_", 8, 5);
        Map<String, Set<ConceptId>> key2_resourcesFake = createFakeInternalConceptLog("Resource_Index_", 2, 8);

        //Add stuff to the cache
        transferFakeCacheIntoRealCache(key1_castingsFake, (index, id) -> cache.addJobCasting(keyspace1, index, id));
        transferFakeCacheIntoRealCache(key1_resourcesFake, (index, id) -> cache.addJobResource(keyspace1, index, id));
        transferFakeCacheIntoRealCache(key2_castingsFake, (index, id) -> cache.addJobCasting(keyspace2, index, id));
        transferFakeCacheIntoRealCache(key2_resourcesFake, (index, id) -> cache.addJobResource(keyspace2, index, id));

        //Check stuff is in graph
        checkContentsOfCache(cache.getCastingJobs(keyspace1), key1_castingsFake);
        checkContentsOfCache(cache.getResourceJobs(keyspace1), key1_resourcesFake);
        checkContentsOfCache(cache.getCastingJobs(keyspace2), key2_castingsFake);
        checkContentsOfCache(cache.getResourceJobs(keyspace2), key2_resourcesFake);
    }

    @Theory
    public void whenAddingAndRemovingInstanceJobsToCache_CacheIsUpdated(Caches caches){
        ConceptCache cache = getCache(caches);
        String keyspace1 = "key1";

        //Create fake commit log
        Map<TypeLabel, Long> fakeCache = new HashMap<>();
        fakeCache.put(TypeLabel.of("A"), 1L);
        fakeCache.put(TypeLabel.of("B"), 2L);
        fakeCache.put(TypeLabel.of("C"), 3L);

        fakeCache.entrySet().forEach(entry -> cache.addJobInstanceCount(keyspace1, entry.getKey(), entry.getValue()));

        assertEquals(fakeCache.keySet(), cache.getInstanceCountJobs(keyspace1).keySet());
        fakeCache.entrySet().forEach(entry -> assertEquals(entry.getValue(), cache.getInstanceCountJobs(keyspace1).get(entry.getKey())));

        fakeCache.remove(TypeLabel.of("B"));
        cache.deleteJobInstanceCount(keyspace1, TypeLabel.of("B"));

        assertEquals(fakeCache.keySet(), cache.getInstanceCountJobs(keyspace1).keySet());
        fakeCache.entrySet().forEach(entry -> assertEquals(entry.getValue(), cache.getInstanceCountJobs(keyspace1).get(entry.getKey())));
    }

    private Map<String, Set<ConceptId>> createFakeInternalConceptLog(String indexPrefix, int numIndex, int numJobs){
        Map<String, Set<ConceptId>> internalCache = new HashMap<>();
        for(int i = 0; i < numIndex; i ++){
            for(int j = 0; j < numJobs; j++){
                internalCache.computeIfAbsent(indexPrefix  + i, (key) -> new HashSet<>()).add(ConceptId.of(j));
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
        assertEquals(realCache.keySet(), fakeCache.keySet());
        fakeCache.keySet().forEach(key-> assertEquals(realCache.get(key), fakeCache.get(key)));
    }




}
