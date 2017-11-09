package ai.grakn.test.benchmark;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.SampleKBLoader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;


public class AddBenchmark extends BenchmarkTest {

    private static final Keyspace KEYSPACE = SampleKBLoader.randomKeyspace();

    private EngineContext engine;
    private GraknSession session;
    private EntityType entityType;
    private RelationshipType relationshipType;
    private GraknTx graph;
    private Role role1;
    private Role role2;

    @Setup
    public void setup() throws Throwable {
        engine = EngineContext.createWithInMemoryRedis();
        engine.before();
        session = Grakn.session(engine.uri(), KEYSPACE);
        graph = session.open(GraknTxType.WRITE);
        role1 = graph.putRole("benchmark_role1");
        role2 = graph.putRole("benchmark_role2");
        entityType = graph.putEntityType("benchmarkEntitytype").plays(role1).plays(role2);
        relationshipType = graph.putRelationshipType("benchmark_relationshipType").relates(role1).relates(role2);

    }

    @TearDown
    public void tearDown() {
        graph.commit();
        session.close();
        engine.after();
    }

    @Benchmark
    public void addEntity() {
        entityType.addEntity();
    }

    @Benchmark
    public void addRelation() {
            Entity entity1 = entityType.addEntity();
            Entity entity2 = entityType.addEntity();
            relationshipType.addRelationship().addRolePlayer(role1, entity1).addRolePlayer(role2, entity2);
    }
}
