package ai.grakn.engine.loader;

import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskSubmitter;
import ai.grakn.graql.Graql;
import static ai.grakn.util.ErrorMessage.READ_ONLY_QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.TASK_LOADER_MUTATIONS;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class MutatorTaskTest {

    private TaskConfiguration taskConfiguration;
    private String readOnlyQuery = Graql.match(Graql.var("x").isa("person")).toString();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setupConfiguration() {
        taskConfiguration = mock(TaskConfiguration.class);

        Json badTaskJson = Json.object();
        badTaskJson.set(KEYSPACE,"keyspace");
        Json readOnlyJson = Json.array();
        readOnlyJson.add(readOnlyQuery);
        badTaskJson.set(TASK_LOADER_MUTATIONS, readOnlyJson);
        when(taskConfiguration.json()).thenReturn(badTaskJson);

    }

    @Test
    public void checkReadOnlyQueriesAreRejected() {
        MutatorTask mutatorTask = new MutatorTask();
        TaskSubmitter taskSubmitter = new TaskSubmitter() {
            @Override
            public void addTask(TaskState taskState, TaskConfiguration configuration) {

            }

            @Override
            public void runTask(TaskState taskState, TaskConfiguration configuration) {

            }
        };
        mutatorTask.initialize((x) -> System.out.println(x.toString()), taskConfiguration, taskSubmitter, null, null, null, null,
                new MetricRegistry());
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(READ_ONLY_QUERY.getMessage(readOnlyQuery));
        mutatorTask.start();
    }
}