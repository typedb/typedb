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
import ai.grakn.test.EngineContext;
import ai.grakn.util.SampleKBLoader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;


@State(Scope.Benchmark)
public class AddWithCommitBenchmark extends BenchmarkTest {

    private static final Keyspace KEYSPACE = SampleKBLoader.randomKeyspace();

    private EngineContext engine;
    private GraknSession session;
    private EntityType entityType;
    private RelationshipType relationshipType;
    private Role role1;
    private Role role2;

    @Setup
    public void setup() throws Throwable {
        engine = EngineContext.createWithInMemoryRedis();
        engine.before();
        session = Grakn.session(engine.uri(), KEYSPACE);
        try(GraknTx tx = session.open(GraknTxType.WRITE)) {
            role1 = tx.putRole("benchmark_role1");
            role2 = tx.putRole("benchmark_role2");
            entityType = tx.putEntityType("benchmark_Entitytype").plays(role1).plays(role2);
            relationshipType = tx.putRelationshipType("benchmark_relationshipType").relates(role1).relates(role2);
            tx.commit();
        }
    }

    @TearDown
    public void tearDown() {
        session.close();
        engine.after();
    }

    @Benchmark
    public void addEntity() {
        try(GraknTx graph = session.open(GraknTxType.WRITE)) {
            entityType.addEntity();
            graph.commit();
        }
    }

    @Benchmark
    public void addRelation() {
        try(GraknTx graph = session.open(GraknTxType.WRITE)) {
            Entity entity1 = entityType.addEntity();
            Entity entity2 = entityType.addEntity();
            relationshipType.addRelationship().addRolePlayer(role1, entity1).addRolePlayer(role2, entity2);
            graph.commit();
        }
    }
}
