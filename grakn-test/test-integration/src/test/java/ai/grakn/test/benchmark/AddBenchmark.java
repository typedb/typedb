package ai.grakn.test.benchmark;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.SessionContext;
import org.junit.Rule;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;


public class AddBenchmark extends BenchmarkTest {

    @Rule
    public final SessionContext sessionContext = SessionContext.create();

    private GraknSession session;
    private EntityType entityType;
    private RelationshipType relationshipType;
    private GraknTx graph;
    private Role role1;
    private Role role2;

    @Setup
    public void setup() throws Throwable {
        session = sessionContext.newSession();
        graph = session.open(GraknTxType.WRITE);
        role1 = graph.putRole("benchmark_role1");
        role2 = graph.putRole("benchmark_role2");
        entityType = graph.putEntityType("benchmarkEntitytype").plays(role1).plays(role2);
        relationshipType = graph.putRelationshipType("benchmark_relationshipType").relates(role1).relates(role2);
    }

    @TearDown
    public void tearDown() {
        graph.commit();
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
