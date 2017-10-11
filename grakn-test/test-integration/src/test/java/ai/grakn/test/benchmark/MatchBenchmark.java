package ai.grakn.test.benchmark;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.EntityType;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.EngineContext;
import ai.grakn.util.SampleKBLoader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Optional;

import static ai.grakn.graql.Graql.var;


public class MatchBenchmark extends BenchmarkTest {

    private static final Keyspace KEYSPACE = SampleKBLoader.randomKeyspace();
    private static final String BENCHMARK_ENTITYTYPE = "benchmarkEntitytype";

    private EngineContext engine;
    private GraknSession session;
    private GraknTx graph;

    @Setup
    public void setup() throws Throwable {
        engine = EngineContext.createWithInMemoryRedis();
        engine.before();
        session = Grakn.session(engine.uri(), KEYSPACE);
        GraknTx graphEntity = session.open(GraknTxType.WRITE);
        EntityType entityType = graphEntity.putEntityType("benchmarkEntitytype");
        entityType.addEntity();
        graphEntity.commit();
        graph = session.open(GraknTxType.WRITE);
    }

    @TearDown
    public void tearDown() {
        graph.close();
        session.close();
        engine.after();
    }

    @Benchmark
    public void match() {
        Match match = graph.graql().match(var("x").isa(BENCHMARK_ENTITYTYPE));
        GetQuery answers = match.get();
        Optional<Answer> first = answers.stream().findFirst();
        first.get();
    }
}
