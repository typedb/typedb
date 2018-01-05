package ai.grakn.test.benchmark;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.rule.SessionContext;
import org.junit.Rule;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Optional;

import static ai.grakn.graql.Graql.var;


public class MatchBenchmark extends BenchmarkTest {

    private static final String BENCHMARK_ENTITY_TYPE = "benchmarkEntityType";
    private static final String BENCHMARK_ATTRIBUTE_TYPE = "benchmarkAttributeType";

    @Rule
    public final SessionContext sessionContext = SessionContext.create();

    private GraknTx graph;

    @Setup
    public void setup() throws Throwable {
        GraknSession session = sessionContext.newSession();
        GraknTx graphEntity = session.open(GraknTxType.WRITE);
        EntityType entityType = graphEntity.putEntityType(BENCHMARK_ENTITY_TYPE);
        AttributeType<String> attributeType =
                graphEntity.putAttributeType(BENCHMARK_ATTRIBUTE_TYPE, AttributeType.DataType.STRING);
        entityType.attribute(attributeType);

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                entityType.addEntity().attribute(attributeType.putAttribute(String.valueOf(i)));
            }
        }
        graphEntity.commit();
        graph = session.open(GraknTxType.WRITE);
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    @Benchmark
    public void match() {
        Match match = graph.graql().match(
                var("x")
                        .isa(BENCHMARK_ENTITY_TYPE)
                        .has(BENCHMARK_ATTRIBUTE_TYPE, "0")
        );
        GetQuery answers = match.get();
        Optional<Answer> first = answers.stream().findFirst();
        first.get();
    }
}
