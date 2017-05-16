package ai.grakn.test.engine.postprocessing;

import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.connection.RedisConnection;
import ai.grakn.test.EngineContext;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_INSTANCE_COUNT;
import static ai.grakn.util.REST.Request.COMMIT_LOG_TYPE_NAME;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class UpdatingInstanceCountTaskTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Test
    public void whenUpdatingInstanceCounts_EnsureRedisIsUpdated() throws InterruptedException {
        String keyspace = "mysimplekeyspace";
        String entityType1 = "e1";
        String entityType2 = "e2";

        //Create Artificial configuration
        Json instanceCounts = Json.array();
        instanceCounts.add(Json.object(COMMIT_LOG_TYPE_NAME, entityType1, COMMIT_LOG_INSTANCE_COUNT, 6));
        instanceCounts.add(Json.object(COMMIT_LOG_TYPE_NAME, entityType2, COMMIT_LOG_INSTANCE_COUNT, 3));
        Json configuration = Json.object(
                KEYSPACE, keyspace,
                COMMIT_LOG_COUNTING, instanceCounts
        );

        //Start up the Job
        TaskState task = TaskState.of(UpdatingInstanceCountTask.class, getClass().getName(), TaskSchedule.now());
        engine.getTaskManager().addLowPriorityTask(task, TaskConfiguration.of(configuration));

        // Wait for task to complete
        waitForDoneStatus(engine.getTaskManager().storage(), singleton(task));

        // Check that task has ran
        // STOPPED because it is a recurring task
        assertEquals(COMPLETED, engine.getTaskManager().storage().getState(task.getId()).status());

        // Check cache in redis has been updated
        RedisConnection redis = RedisConnection.getConnection();
        assertEquals(6L, redis.getCount(RedisConnection.getKeyNumInstances(keyspace, TypeLabel.of(entityType1))));
        assertEquals(3L, redis.getCount(RedisConnection.getKeyNumInstances(keyspace, TypeLabel.of(entityType2))));
    }

}
