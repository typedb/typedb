package ai.grakn.test.benchmark;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.EntityType;
import ai.grakn.test.EngineContext;
import ai.grakn.util.SampleKBLoader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;


public class InsertBenchmark extends BenchmarkTest {

    private static final Keyspace KEYSPACE = SampleKBLoader.randomKeyspace();

    private EngineContext engine;
    private GraknSession session;
    private EntityType entityType;
    private GraknTx graph;

    @Setup
    public void setup() throws Throwable {
        engine = EngineContext.inMemoryServer();
        engine.before();
        session = Grakn.session(engine.uri(), KEYSPACE);
        graph = session.open(GraknTxType.WRITE);
        entityType = graph.putEntityType("benchmarkEntitytype");
    }

    @TearDown
    public void tearDown() {
        graph.commit();
        session.close();
        engine.after();
    }

    @Benchmark
    public void inserts() {
        entityType.addEntity();
    }
}
