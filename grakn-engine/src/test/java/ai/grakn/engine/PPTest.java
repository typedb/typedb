package ai.grakn.engine;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;

import java.util.Optional;

public class PPTest {
    @Test
    public void createAttributeJohnDoe() {
        EmbeddedGraknSession graknSession = EmbeddedGraknSession.create(Keyspace.of("grakn"), "0.0.0.0:4567");
        EmbeddedGraknTx graknTx = graknSession.transaction(GraknTxType.WRITE);
         graknTx.putAttributeType(Label.of("name"), AttributeType.DataType.STRING).create("test3");
         graknTx.commit();
         graknSession.close();
    }

    @Test
    public void test() {
        EmbeddedGraknSession graknSession = EmbeddedGraknSession.create(Keyspace.of("grakn"), "0.0.0.0:4567");
        EmbeddedGraknTx graknTx = graknSession.transaction(GraknTxType.WRITE);
        GraphTraversalSource tinker = graknTx.getTinkerTraversal();
        tinker.V().has("INDEX").toList().forEach(v -> {
            System.out.println("v label '" + v.label() + "', value '" + v.value("INDEX"));
            v.properties().forEachRemaining(p -> {
                System.out.println("property key '" + p.key() + "', value '" + p.value() + "'");
            });
        });
    }
}
