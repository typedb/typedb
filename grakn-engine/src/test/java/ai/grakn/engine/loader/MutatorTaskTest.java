package ai.grakn.engine.loader;

import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.graql.Graql;
import mjson.Json;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ai.grakn.util.REST.Request.TASK_LOADER_MUTATIONS;
import static ai.grakn.util.ErrorMessage.READ_ONLY_QUERY;

/**
 *
 */
public class MutatorTaskTest {

    TaskConfiguration taskConfiguration;
    String readOnlyQuery = Graql.match(Graql.var("x").isa("person")).toString();

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
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(READ_ONLY_QUERY.getMessage(readOnlyQuery));
        mutatorTask.start((x) -> System.out.println(x.toString()), taskConfiguration);
    }
}