package ai.grakn.test.benchmark;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.EntityType;
import ai.grakn.graql.GetQuery;
import static ai.grakn.graql.Graql.var;
import ai.grakn.graql.Match;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.EngineContext;
import ai.grakn.util.SampleKBLoader;
import java.util.Optional;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;


public class MatchBenchmark extends BenchmarkTest {

    private static final Keyspace KEYSPACE = SampleKBLoader.randomKeyspace();
    private static final String BENCHMARK_ENTITYTYPE = "benchmarkEntitytype";

    private EngineContext engine;
    private GraknSession session;

    @Setup
    public void setup() throws Throwable {
        engine = EngineContext.inMemoryServer();
        engine.before();
        session = Grakn.session(engine.uri(), KEYSPACE);
        GraknTx graph = session.open(GraknTxType.WRITE);
        EntityType entityType = graph.putEntityType("benchmarkEntitytype");
        entityType.addEntity();
        graph.commit();
    }

    @TearDown
    public void tearDown() {
        session.close();
        engine.after();
    }

    @Benchmark
    public void match() {
        GraknTx graph = session.open(GraknTxType.WRITE);
        Match match = graph.graql().match(var("x").isa(BENCHMARK_ENTITYTYPE));
        GetQuery answers = match.get();
        Optional<Answer> first = answers.stream().findFirst();
        first.get();
        graph.close();
    }
}
