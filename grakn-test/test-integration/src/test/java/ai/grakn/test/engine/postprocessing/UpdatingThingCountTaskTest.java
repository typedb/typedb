package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import static ai.grakn.engine.GraknEngineConfig.REDIS_HOST;
import static ai.grakn.engine.TaskStatus.COMPLETED;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.util.SimpleURI;
import ai.grakn.test.EngineContext;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import ai.grakn.util.MockRedisRule;
import static ai.grakn.util.REST.Request.COMMIT_LOG_CONCEPT_ID;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_SHARDING_COUNT;
import static ai.grakn.util.REST.Request.KEYSPACE;
import ai.grakn.util.SampleKBLoader;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import static java.util.Collections.singleton;
import mjson.Json;
import static org.junit.Assert.assertEquals;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class UpdatingThingCountTaskTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.singleQueueServer();

    @ClassRule
    public static final MockRedisRule mockRedisRule = new MockRedisRule();

    @Test
    public void whenUpdatingInstanceCounts_EnsureRedisIsUpdated() throws InterruptedException {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        SimpleURI redisURI = new SimpleURI(engine.config().getProperty(REDIS_HOST));
        JedisPool jedisPool = new JedisPool(poolConfig, redisURI.getHost(), redisURI.getPort());
        RedisCountStorage redis = RedisCountStorage.create(jedisPool, new MetricRegistry());
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

    private void createAndExecuteCountTask(Keyspace keyspace, ConceptId conceptId, long count){
        Json instanceCounts = Json.array();
        instanceCounts.add(Json.object(COMMIT_LOG_CONCEPT_ID, conceptId.getValue(), COMMIT_LOG_SHARDING_COUNT, count));
        Json configuration = Json.object(
                KEYSPACE, keyspace.getValue(),
                COMMIT_LOG_COUNTING, instanceCounts
        );

        //Start up the Job
        TaskState task = TaskState.of(UpdatingInstanceCountTask.class, getClass().getName(), TaskSchedule.now(), TaskState.Priority.HIGH);
        engine.getTaskManager().addTask(task, TaskConfiguration.of(configuration));

        // Wait for task to complete
        waitForDoneStatus(engine.getTaskManager().storage(), singleton(task));

        // Check that task has ran
        // STOPPED because it is a recurring task
        assertEquals(COMPLETED, engine.getTaskManager().storage().getState(task.getId()).status());
    }

    @Test
    public void whenShardingThresholdIsBreached_ShardTypes(){
        Keyspace keyspace = SampleKBLoader.randomKeyspace();
        EntityType et1;
        EntityType et2;

        //Create Simple GraknTx
        try(GraknTx graknTx = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)){
            et1 = graknTx.putEntityType("et1");
            et2 = graknTx.putEntityType("et2");
            graknTx.admin().commitNoLogs();
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
    private void checkShardCount(Keyspace keyspace, Concept concept, int expectedValue){
        try(GraknTx graknTx = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)){
            int shards = graknTx.admin().getTinkerTraversal().V().
                    has(Schema.VertexProperty.ID.name(), concept.getId().getValue()).
                    in(Schema.EdgeLabel.SHARD.getLabel()).toList().size();

            assertEquals(expectedValue, shards);
        }
    }

}
