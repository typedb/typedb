package ai.grakn.test.benchmark;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.BatchMutatorClient;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.util.SimpleURI;
import ai.grakn.graql.Graql;
import static ai.grakn.graql.Graql.var;
import ai.grakn.graql.InsertQuery;
import ai.grakn.test.EngineContext;
import com.codahale.metrics.ConsoleReporter;
import java.util.UUID;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;


public class BatchMutatorClientBenchmark extends BenchmarkTest {

    private EngineContext engine;
    private ConsoleReporter consoleReporter;
    private BatchMutatorClient batchMutatorClient;

    @Setup
    public void setup() throws Throwable {
        engine = EngineContext.singleQueueServer();
        engine.before();
        SimpleURI simpleURI = new SimpleURI(engine.uri());
        String benchmarktest = "benchmarktest";
        GraknSession session = Grakn.session(simpleURI.toString(), benchmarktest);
        try(GraknTx graph = session.open(GraknTxType.WRITE)){
            EntityType nameTag = graph.putEntityType("name_tag");
            AttributeType<String> nameTagString = graph.putAttributeType("name_tag_string", AttributeType.DataType.STRING);
            AttributeType<String> nameTagId = graph.putAttributeType("name_tag_id", AttributeType.DataType.STRING);

            nameTag.attribute(nameTagString);
            nameTag.attribute(nameTagId);
            graph.admin().commitNoLogs();
        }
        session.close();
        batchMutatorClient = new BatchMutatorClient(Keyspace.of(benchmarktest), simpleURI.toString(), true, 3);
        consoleReporter = ConsoleReporter
                .forRegistry(engine.server().getMetricRegistry()).build();
    }

    @TearDown
    public void tearDown() {
        batchMutatorClient.waitToFinish();
        consoleReporter.report();
        engine.after();
    }

    @Benchmark
    public void insert() {
        InsertQuery q = Graql.insert(
                var().isa("name_tag")
                        .has("name_tag_string", UUID.randomUUID().toString())
                        .has("name_tag_id", UUID.randomUUID().toString()));
        batchMutatorClient.add(q);
    }
}
