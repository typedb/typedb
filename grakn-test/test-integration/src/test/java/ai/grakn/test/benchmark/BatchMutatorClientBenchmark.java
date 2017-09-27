package ai.grakn.test.benchmark;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.BatchMutatorClient;
import ai.grakn.client.TaskClient;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.util.SimpleURI;
import ai.grakn.graql.Graql;
import static ai.grakn.graql.Graql.var;
import ai.grakn.graql.InsertQuery;
import ai.grakn.test.EngineContext;
import static ai.grakn.util.REST.Request.BATCH_NUMBER;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.TASK_LOADER_MUTATIONS;
import com.codahale.metrics.ConsoleReporter;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import mjson.Json;
import org.openjdk.jmh.annotations.BenchmarkMode;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;


public class BatchMutatorClientBenchmark extends BenchmarkTest {

    private final static String BENCHMARKTEST = "benchmarktest";

    private EngineContext engine;
    private ConsoleReporter consoleReporter;
    private BatchMutatorClient batchMutatorClient;
    private TaskClient simpleClient;

    @Setup
    public void setup() throws Throwable {
        engine = EngineContext.singleQueueServer();
        engine.before();
        SimpleURI simpleURI = new SimpleURI(engine.uri());
        GraknSession session = Grakn.session(simpleURI.toString(), BENCHMARKTEST);
        try(GraknTx graph = session.open(GraknTxType.WRITE)){
            EntityType nameTag = graph.putEntityType("name_tag");
            AttributeType<String> nameTagString = graph.putAttributeType("name_tag_string", AttributeType.DataType.STRING);
            AttributeType<String> nameTagId = graph.putAttributeType("name_tag_id", AttributeType.DataType.STRING);

            nameTag.attribute(nameTagString);
            nameTag.attribute(nameTagId);
            graph.admin().commitNoLogs();
        }
        session.close();
        batchMutatorClient = new BatchMutatorClient(Keyspace.of(BENCHMARKTEST), simpleURI.toString(), true, 3);
        // For some reason, it gets stuck with bigger numbers
        batchMutatorClient.setNumberActiveTasks(1);
        batchMutatorClient.setBatchSize(10);
        simpleClient = TaskClient.of(simpleURI.getHost(), simpleURI.getPort());
        consoleReporter = ConsoleReporter
                .forRegistry(engine.server().getMetricRegistry()).build();
    }

    @TearDown
    public void tearDown() {
        batchMutatorClient.waitToFinish();
        consoleReporter.report();
        engine.after();
    }

//  TODO: Currently taking too long
//    @Benchmark
    @BenchmarkMode(Throughput)
    public void batchInsert() {
        InsertQuery q = Graql.insert(
                var().isa("name_tag")
                        .has("name_tag_string", UUID.randomUUID().toString())
                        .has("name_tag_id", UUID.randomUUID().toString()));
        batchMutatorClient.add(q);
    }

//  TODO: Currently taking too long
//    @Benchmark
    @BenchmarkMode(Throughput)
    public void simpleInsert() {
        InsertQuery q = Graql.insert(
                var().isa("name_tag")
                        .has("name_tag_string", UUID.randomUUID().toString())
                        .has("name_tag_id", UUID.randomUUID().toString()));
        Json configuration = Json.object()
                .set(KEYSPACE_PARAM, BENCHMARKTEST)
                .set(BATCH_NUMBER, 0)
                .set(TASK_LOADER_MUTATIONS,
                        ImmutableList.of(q.toString()));
        simpleClient.sendTask(ai.grakn.engine.loader.MutatorTask.class,
                BatchMutatorClient.class.getName(),
                Instant.ofEpochMilli(new Date().getTime()), null, configuration, true);
    }
}
