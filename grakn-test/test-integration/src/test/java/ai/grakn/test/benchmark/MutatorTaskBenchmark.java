package ai.grakn.test.benchmark;

import ai.grakn.client.TaskClient;
import ai.grakn.engine.tasks.mock.LongExecutionMockTask;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.test.rule.EngineContext;
import mjson.Json;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.time.Instant;

import static java.time.Instant.now;

@State(Scope.Benchmark)
public class MutatorTaskBenchmark extends BenchmarkTest {

    // This cannot be used with `@Rule` because benchmarks are run in different JVMs
    private static final EngineContext engine = EngineContext.createWithEmbeddedRedis();

    private TaskClient client;

    @Setup
    public void setup() throws Throwable {
        client = TaskClient.of(engine.uri());
    }

    @TearDown
    public void tearDown() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Benchmark
    public void sendTaskAndWaitShort() {
        engine.execute(() -> {
            Class<?> taskClass = ShortExecutionMockTask.class;
            String creator = this.getClass().getName();
            Instant runAt = now();
            Json configuration = Json.object("id", "123");
            client.sendTask(taskClass, creator, runAt, null, configuration, true).getTaskId();;
        });
    }

    @Benchmark
    public void sendTaskAndWaitLong() {
        engine.execute(() -> {
            Class<?> taskClass = LongExecutionMockTask.class;
            String creator = this.getClass().getName();
            Instant runAt = now();
            Json configuration = Json.object("id", "123");
            client.sendTask(taskClass, creator, runAt, null, configuration, true).getTaskId();
        });
    }
}
