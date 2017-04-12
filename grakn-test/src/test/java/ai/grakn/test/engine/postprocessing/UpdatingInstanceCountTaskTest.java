package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.test.EngineContext;
import ai.grakn.util.Schema;
import mjson.Json;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.engine.TaskStatus.STOPPED;
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
    public void whenUpdatingInstanceCounts_EnsureTypesInGraphAreUpdated() throws InterruptedException {
        String keyspace = "mysimplekeyspace";
        String entityType1 = "e1";
        String entityType2 = "e2";

        //Create Simple Graph
        try(GraknGraph graknGraph = Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE)){
            graknGraph.putEntityType(entityType1);
            graknGraph.putEntityType(entityType2);
            graknGraph.admin().commitNoLogs();
        }

        //Create Artificial configuration
        Json instanceCounts = Json.array();
        instanceCounts.add(Json.object(COMMIT_LOG_TYPE_NAME, entityType1, COMMIT_LOG_INSTANCE_COUNT, 6));
        instanceCounts.add(Json.object(COMMIT_LOG_TYPE_NAME, entityType2, COMMIT_LOG_INSTANCE_COUNT, 3));
        Json configuration = Json.object(
                KEYSPACE, keyspace,
                COMMIT_LOG_COUNTING, instanceCounts
        );

        //Start up the Job
        TaskState task = TaskState.of(UpdatingInstanceCountTask.class, getClass().getName(), TaskSchedule.now(), configuration);
        engine.getTaskManager().addLowPriorityTask(task);

        // Wait for task to complete
        waitForDoneStatus(engine.getTaskManager().storage(), singleton(task));

        // Check that task has ran
        // STOPPED because it is a recurring task
        assertEquals(STOPPED, engine.getTaskManager().storage().getState(task.getId()).status());

        // Check the results of the task
        try(GraknGraph graknGraph = Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE)){
            Vertex v1 = graknGraph.admin().getTinkerTraversal().has(Schema.ConceptProperty.ID.name(), graknGraph.getEntityType(entityType1).getId().getValue()).next();
            Vertex v2 = graknGraph.admin().getTinkerTraversal().has(Schema.ConceptProperty.ID.name(), graknGraph.getEntityType(entityType2).getId().getValue()).next();

            assertEquals(6L, (long) v1.value(Schema.ConceptProperty.INSTANCE_COUNT.name()));
            assertEquals(3L, (long) v2.value(Schema.ConceptProperty.INSTANCE_COUNT.name()));
        }
    }

}
