package ai.grakn.test.benchmark;

import ai.grakn.client.TaskClient;
import ai.grakn.engine.tasks.mock.LongExecutionMockTask;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.test.rule.EngineContext;
import mjson.Json;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static java.time.Instant.now;


public class MutatorTaskBenchmark extends BenchmarkTest {
    private static final Logger LOG = LoggerFactory.getLogger(MutatorTaskBenchmark.class);

    private EngineContext engine;
    private TaskClient client;

    @Setup
    public void setup() throws Throwable {
        engine = makeEngine();
        engine.before();
        client = TaskClient.of(engine.uri());
    }

    protected EngineContext makeEngine() {
        return EngineContext.createWithEmbeddedRedis();
    }

    @TearDown
    public void tearDown() {
        LOG.info("Starting teardown");
        LOG.info("Closing engine");
        engine.after();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Benchmark
    public void sendTaskAndWaitShort() {
        Class<?> taskClass = ShortExecutionMockTask.class;
        String creator = this.getClass().getName();
        Instant runAt = now();
        Json configuration = Json.object("id", "123");
        client.sendTask(taskClass, creator, runAt, null, configuration, true).getTaskId();;
    }

    @Benchmark
    public void sendTaskAndWaitLong() {
        Class<?> taskClass = LongExecutionMockTask.class;
        String creator = this.getClass().getName();
        Instant runAt = now();
        Json configuration = Json.object("id", "123");
        client.sendTask(taskClass, creator, runAt, null, configuration, true).getTaskId();
    }
}
