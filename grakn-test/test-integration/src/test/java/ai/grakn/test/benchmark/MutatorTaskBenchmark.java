package ai.grakn.test.benchmark;

import ai.grakn.client.TaskClient;
import ai.grakn.engine.tasks.mock.LongExecutionMockTask;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.util.SimpleURI;
import ai.grakn.test.EngineContext;
import com.codahale.metrics.ConsoleReporter;
import mjson.Json;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.time.Instant;

import static java.time.Instant.now;


public class MutatorTaskBenchmark extends BenchmarkTest {

    private EngineContext engine;
    private TaskClient client;
    private ConsoleReporter consoleReporter;

    @Setup
    public void setup() throws Throwable {
        engine = makeEngine();
        engine.before();
        SimpleURI simpleURI = new SimpleURI(engine.uri());
        client = TaskClient.of(simpleURI.getHost(), simpleURI.getPort());
        consoleReporter = ConsoleReporter
                .forRegistry(engine.getMetricRegistry()).build();
    }

    protected EngineContext makeEngine() {
        return EngineContext.createWithEmbeddedRedis();
    }

    @TearDown
    public void tearDown() {
        consoleReporter.report();
        engine.after();
    }

    @Benchmark
    public void sendTaskAndWaitShort() {
        Class<?> taskClass = ShortExecutionMockTask.class;
        String creator = this.getClass().getName();
        Instant runAt = now();
        Json configuration = Json.object("id", "123");
        client.sendTask(taskClass, creator, runAt, null, configuration, true).getTaskId();
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
