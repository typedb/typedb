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

package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.task.postprocessing.IndexPostProcessor;
import ai.grakn.engine.task.postprocessing.InstanceCountPostProcessor;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.task.postprocessing.RedisCountStorage;
import ai.grakn.engine.task.postprocessing.RedisIndexStorage;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.SampleKBLoader;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.createDuplicateResource;
import static ai.grakn.util.Schema.VertexProperty.INDEX;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PostProcessingTest {
    private PostProcessor postProcessor;
    private GraknSession session;

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    @BeforeClass
    public static void onlyRunOnTinker() {
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Before
    public void setupPostProcessor() {
        MetricRegistry metricRegistry = new MetricRegistry();
        RedisIndexStorage indexStorage = RedisIndexStorage.create(engine.getJedisPool(), metricRegistry);
        IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(engine.server().lockProvider(), indexStorage);

        RedisCountStorage countStorage = RedisCountStorage.create(engine.getJedisPool(), metricRegistry);
        InstanceCountPostProcessor countPostProcessor = InstanceCountPostProcessor.create(engine.config(), engine.server().factory(), engine.server().lockProvider(), metricRegistry, countStorage);

        postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);
    }

    @After
    public void takeDown() throws InterruptedException {
        session.close();
    }

    @Test
    public void whenCreatingDuplicateResources_EnsureTheyAreMergedInPost() throws InvalidKBException, InterruptedException, JsonProcessingException {
        String value = "1";
        String sample = "Sample";

        //Create GraknTx With Duplicate Resources
        GraknTx tx = session.open(GraknTxType.WRITE);
        AttributeType<String> attributeType = tx.putAttributeType(sample, AttributeType.DataType.STRING);

        Attribute<String> attribute = attributeType.putAttribute(value);
        tx.admin().commitSubmitNoLogs();
        tx = session.open(GraknTxType.WRITE);

        assertEquals(1, attributeType.instances().count());
        //Check duplicates have been created
        Set<Vertex> resource1 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource2 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource3 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource4 = createDuplicateResource(tx, attributeType, attribute);
        assertEquals(5, attributeType.instances().count());

        // Attribute vertex index
        String resourceIndex = resource1.iterator().next().value(INDEX.name()).toString();

        // Merge the attribute sets
        Set<Vertex> merged = Sets.newHashSet();
        merged.addAll(resource1);
        merged.addAll(resource2);
        merged.addAll(resource3);
        merged.addAll(resource4);

        tx.close();

        // Casting sets as ConceptIds
        Set<ConceptId> resourceConcepts = merged.stream().map(c -> ConceptId.of(Schema.PREFIX_VERTEX + c.id().toString())).collect(toSet());

        //Create Commit Log
        CommitLog commitLog = CommitLog.createDefault(tx.keyspace());
        commitLog.attributes().put(resourceIndex, resourceConcepts);

        //Now fix everything
        //PostProcessingTask task = new PostProcessingTask();
        //TaskConfiguration configuration = TaskConfiguration.of(mapper.writeValueAsString(commitLog));
        //task.initialize(configuration, engine.config(), engine.server().factory(),
        //        new MetricRegistry(), postProcessor);

        //task.start();

        tx = session.open(GraknTxType.READ);

        //Check it's fixed
        assertEquals(1, tx.getAttributeType(sample).instances().count());

        tx.close();
    }

    @Test
    public void whenUpdatingInstanceCounts_EnsureRedisIsUpdated() throws InterruptedException {
        RedisCountStorage redis = engine.redis();
        Keyspace keyspace = SampleKBLoader.randomKeyspace();
        String entityType1 = "e1";
        String entityType2 = "e2";

        //Create Artificial configuration
        createAndExecuteCountTask(keyspace, ConceptId.of(entityType1), 6L);
        createAndExecuteCountTask(keyspace, ConceptId.of(entityType2), 3L);
        // Check cache in redis has been updated
        assertEquals(6L, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, ConceptId.of(entityType1))));
        assertEquals(3L, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, ConceptId.of(entityType2))));

        //Create Artificial configuration
        createAndExecuteCountTask(keyspace, ConceptId.of(entityType1), 1L);
        createAndExecuteCountTask(keyspace, ConceptId.of(entityType2), -1L);
        // Check cache in redis has been updated
        assertEquals(7L, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, ConceptId.of(entityType1))));
        assertEquals(2L, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, ConceptId.of(entityType2))));
    }

    private void createAndExecuteCountTask(Keyspace keyspace, ConceptId conceptId, long count) {
        //Create the fake commit log
        CommitLog commitLog = CommitLog.createDefault(keyspace);
        commitLog.instanceCount().put(conceptId, count);

        //Start up the Job
        postProcessor.index().updateIndices(commitLog);
    }

    @Test
    public void whenShardingThresholdIsBreached_ShardTypes() {
        Keyspace keyspace = SampleKBLoader.randomKeyspace();
        EntityType et1;
        EntityType et2;

        //Create Simple GraknTx
        try (GraknTx graknTx = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)) {
            et1 = graknTx.putEntityType("et1");
            et2 = graknTx.putEntityType("et2");
            graknTx.admin().commitSubmitNoLogs();
        }

        checkShardCount(keyspace, et1, 1);
        checkShardCount(keyspace, et2, 1);

        //Add new counts
        createAndExecuteCountTask(keyspace, et1.getId(), 99_999L);
        createAndExecuteCountTask(keyspace, et2.getId(), 99_999L);

        checkShardCount(keyspace, et1, 1);
        checkShardCount(keyspace, et2, 1);

        //Add new counts
        createAndExecuteCountTask(keyspace, et1.getId(), 2L);
        createAndExecuteCountTask(keyspace, et2.getId(), 1L);

        checkShardCount(keyspace, et1, 2);
        checkShardCount(keyspace, et2, 1);
    }

    private void checkShardCount(Keyspace keyspace, Concept concept, int expectedValue) {
        try (GraknTx graknTx = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)) {
            int shards = graknTx.admin().getTinkerTraversal().V().
                    has(Schema.VertexProperty.ID.name(), concept.getId().getValue()).
                    in(Schema.EdgeLabel.SHARD.getLabel()).toList().size();

            assertEquals(expectedValue, shards);

        }
    }
}