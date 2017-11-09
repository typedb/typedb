package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.postprocessing.RedisCountStorage;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.REST;
import ai.grakn.util.SampleKBLoader;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.util.REST.Request.COMMIT_LOG_CONCEPT_ID;
import static ai.grakn.util.REST.Request.COMMIT_LOG_SHARDING_COUNT;
import static org.junit.Assert.assertEquals;

public class PostProcessorTest {

    private PostProcessor postProcessor;

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    @Before
    public void setupPostProcessor(){
        postProcessor = PostProcessor.create(engine.config(), engine.getJedisPool(), engine.server().factory(), engine.server().lockProvider(), new MetricRegistry());
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

    private void createAndExecuteCountTask(Keyspace keyspace, ConceptId conceptId, long count){
        Json instanceCounts = Json.array();
        instanceCounts.add(Json.object(COMMIT_LOG_CONCEPT_ID, conceptId.getValue(), COMMIT_LOG_SHARDING_COUNT, count));

        Json fakeCommitLog = Json.object();
        fakeCommitLog.set(REST.Request.COMMIT_LOG_COUNTING, instanceCounts);

        //Start up the Job
        postProcessor.updateCounts(keyspace, fakeCommitLog);
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
    private void checkShardCount(Keyspace keyspace, Concept concept, int expectedValue){
        try(GraknTx graknTx = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)){
            int shards = graknTx.admin().getTinkerTraversal().V().
                    has(Schema.VertexProperty.ID.name(), concept.getId().getValue()).
                    in(Schema.EdgeLabel.SHARD.getLabel()).toList().size();

            assertEquals(expectedValue, shards);
        }
    }

}
