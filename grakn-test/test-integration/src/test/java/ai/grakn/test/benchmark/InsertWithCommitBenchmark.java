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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;


@State(Scope.Benchmark)
public class InsertWithCommitBenchmark extends BenchmarkTest {

    private static final Keyspace KEYSPACE = SampleKBLoader.randomKeyspace();

    private EngineContext engine;
    private GraknSession session;
    private EntityType entityType;

    @Setup
    public void setup() throws Throwable {
        engine = EngineContext.inMemoryServer();
        engine.before();
        session = Grakn.session(engine.uri(), KEYSPACE);
        try(GraknTx graph = session.open(GraknTxType.WRITE)) {
            entityType = graph.putEntityType("benchmarkEntitytype");
            graph.commit();
        }
    }

    @TearDown
    public void tearDown() {
        session.close();
        engine.after();
    }

    @Benchmark
    public void inserts() {
        try(GraknTx graph = session.open(GraknTxType.WRITE)) {
            entityType.addEntity();
            graph.commit();
        }
    }
}
