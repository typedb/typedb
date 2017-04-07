package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.util.EngineID;
import ai.grakn.graph.admin.ConceptCache;
import ai.grakn.test.EngineContext;
import ai.grakn.util.Schema;
import mjson.Json;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Date;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static org.junit.Assert.assertEquals;

public class UpdatingInstanceCountTaskTest {
    //Different task manager is required so we don't interfere with main engine running
    private StandaloneTaskManager taskManager = new StandaloneTaskManager(EngineID.of("hello"));

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Test
    public void whenUpdatingInstanceCounts_EnsureTypesInGraphAreUpdated() throws InterruptedException {
        String keyspace = "mysimplekeyspace";
        String entityType1 = "e1";
        String entityType2 = "e2";
        EngineCacheProvider.getCache().clearAllJobs(keyspace);

        //Create Simple Graph
        try(GraknGraph graknGraph = Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE)){
            graknGraph.putEntityType(entityType1);
            graknGraph.putEntityType(entityType2);
            graknGraph.commit();
        }

        //Create Artificial Caches
        ConceptCache cache = EngineCacheProvider.getCache();
        cache.addJobInstanceCount(keyspace, TypeLabel.of(entityType1), 6);
        cache.addJobInstanceCount(keyspace, TypeLabel.of(entityType2), 3);

        //Start up the Job
        TaskState task = TaskState.of(UpdatingInstanceCountTask.class, getClass().getName(), TaskSchedule.now(), Json.object());
        taskManager.addTask(task);

        // Wait for supervisor thread to mark task as completed
        final long initial = new Date().getTime();

        while ((new Date().getTime())-initial < 10000) {
            if (taskManager.storage().getState(task.getId()).status() == COMPLETED)
                break;
            Thread.sleep(100);
        }

        // Check that task has ran
        assertEquals(COMPLETED, taskManager.storage().getState(task.getId()).status());

        // Check the results of the task
        try(GraknGraph graknGraph = Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE)){
            Vertex v1 = graknGraph.admin().getTinkerTraversal().has(Schema.ConceptProperty.ID.name(), graknGraph.getEntityType(entityType1).getId().getValue()).next();
            Vertex v2 = graknGraph.admin().getTinkerTraversal().has(Schema.ConceptProperty.ID.name(), graknGraph.getEntityType(entityType2).getId().getValue()).next();

            assertEquals(6L, (long) v1.value(Schema.ConceptProperty.INSTANCE_COUNT.name()));
            assertEquals(3L, (long) v2.value(Schema.ConceptProperty.INSTANCE_COUNT.name()));
        }
    }

}
