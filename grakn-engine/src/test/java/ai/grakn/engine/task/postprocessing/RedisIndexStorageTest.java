/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.test.rule.InMemoryRedisContext;
import com.codahale.metrics.MetricRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * <p>
 *    Tests that index related post processing data can be stored and retrieved from redis
 * </p>
 *
 * @author fppt
 */
public class RedisIndexStorageTest {
    @ClassRule
    public static final InMemoryRedisContext IN_MEMORY_REDIS_CONTEXT = InMemoryRedisContext.create();
    private static RedisStorage directConnection;
    private static RedisIndexStorage indexStorage;
    private final static Keyspace keyspace1 = Keyspace.of("myhappypotato");
    private final static Keyspace keyspace2 = Keyspace.of("mysadapple");
    private final static String index1 = "index1";
    private final static String index2 = "index2";
    private final static Set<ConceptId> conceptIds = new HashSet<>(Arrays.asList(ConceptId.of("a"), ConceptId.of("b"), ConceptId.of("c")));

    @BeforeClass
    public static void getConnection(){
        MetricRegistry metricRegistry = new MetricRegistry();
        indexStorage = RedisIndexStorage.create(IN_MEMORY_REDIS_CONTEXT.jedisPool(), metricRegistry);
        directConnection = new RedisStorage(IN_MEMORY_REDIS_CONTEXT.jedisPool(), metricRegistry);
    }

    @Before
    public void preinitializedIndexStorage(){
        indexStorage.addIndex(keyspace1, index1, conceptIds);
        indexStorage.addIndex(keyspace2, index1, conceptIds);
        indexStorage.addIndex(keyspace2, index2, conceptIds);
    }

    @After
    public void removeDataFromStorage(){
        indexStorage.popIndex(keyspace1);
        indexStorage.popIndex(keyspace2);
        indexStorage.popIds(keyspace1, index1);
        indexStorage.popIds(keyspace2, index1);
        indexStorage.popIds(keyspace2, index2);
    }

    @Test
    public void onPreinitializedIndexStorage_EnsureKeyspaceListContainIndices(){
        assertEquals(index1, indexStorage.popIndex(keyspace1));
        assertEquals(null, indexStorage.popIndex(keyspace1));
        assertEquals(index1, indexStorage.popIndex(keyspace2));
        assertEquals(index2, indexStorage.popIndex(keyspace2));
    }

    @Test
    public void onPreinitializedIndexStorage_EnsureIndicesAreStoredUnderDifferentKeys(){
        assertJedisContains(RedisIndexStorage.getIndicesKey(keyspace1), index1);
        assertJedisContains(RedisIndexStorage.getIndicesKey(keyspace2), index1, index2);
    }

    @Test
    public void whenPoppingIndex_EnsureListOfIndicesAreUpdated(){
        assertJedisContains(RedisIndexStorage.getIndicesKey(keyspace2), index1, index2);
        String index = indexStorage.popIndex(keyspace2);
        assertEquals(index1, index);
        assertJedisContains(RedisIndexStorage.getIndicesKey(keyspace2), index2);
    }

    @Test
    public void whenPoppingIds_EnsureListOfIdsAreUpdated(){
        String [] ids = conceptIds.stream().map(ConceptId::getValue).toArray(String[]::new);
        String conceptIdsKey = RedisIndexStorage.getConceptIdsKey(keyspace2, index1);
        assertJedisContains(conceptIdsKey, ids);

        Set<ConceptId> foundIds = indexStorage.popIds(keyspace2, index1);
        assertEquals(conceptIds, foundIds);
        assertThat(directConnection.contactRedis(jedis -> jedis.smembers(conceptIdsKey)), empty());
    }

    private void assertJedisContains(String key, String... vals){
        Set<String> result = directConnection.contactRedis(jedis -> jedis.smembers(key));
        assertThat(result, containsInAnyOrder(vals));
    }
}
