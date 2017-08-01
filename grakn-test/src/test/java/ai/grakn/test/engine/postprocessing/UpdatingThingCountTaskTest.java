package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import static ai.grakn.engine.TaskStatus.COMPLETED;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.test.EngineContext;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import ai.grakn.util.MockRedisRule;
import static ai.grakn.util.REST.Request.COMMIT_LOG_CONCEPT_ID;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_SHARDING_COUNT;
import static ai.grakn.util.REST.Request.KEYSPACE;
import ai.grakn.util.Schema;
import static java.util.Collections.singleton;
import java.util.UUID;
import mjson.Json;
import static org.junit.Assert.assertEquals;
import org.junit.ClassRule;
import org.junit.Test;

public class UpdatingThingCountTaskTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startSingleQueueServer();

    @ClassRule
    public static final MockRedisRule mockRedisRule = new MockRedisRule();

    @Test
    public void whenUpdatingInstanceCounts_EnsureRedisIsUpdated() throws InterruptedException {
        RedisCountStorage redis = engine.redis();
        String keyspace = UUID.randomUUID().toString();
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

    private void createAndExecuteCountTask(String keyspace, ConceptId conceptId, long count){
        Json instanceCounts = Json.array();
        instanceCounts.add(Json.object(COMMIT_LOG_CONCEPT_ID, conceptId.getValue(), COMMIT_LOG_SHARDING_COUNT, count));
        Json configuration = Json.object(
                KEYSPACE, keyspace,
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
        String keyspace = "anotherwonderfulkeyspace";
        EntityType et1;
        EntityType et2;

        //Create Simple Graph
        try(GraknGraph graknGraph = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)){
            et1 = graknGraph.putEntityType("et1");
            et2 = graknGraph.putEntityType("et2");
            graknGraph.admin().commitNoLogs();
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
    private void checkShardCount(String keyspace, Concept concept, int expectedValue){
        try(GraknGraph graknGraph = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)){
            int shards = graknGraph.admin().getTinkerTraversal().V().
                    has(Schema.VertexProperty.ID.name(), concept.getId().getValue()).
                    in(Schema.EdgeLabel.SHARD.getLabel()).toList().size();

            assertEquals(expectedValue, shards);
        }
    }

}
