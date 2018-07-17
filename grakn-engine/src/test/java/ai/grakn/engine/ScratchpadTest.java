package ai.grakn.engine;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.concept.AttributeImpl;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;
import java.util.Optional;

import static ai.grakn.graql.Graql.*;

public class ScratchpadTest {
    @Test
    public void getAttribute() {
        EmbeddedGraknSession session = EmbeddedGraknSession.create(Keyspace.of("grakn"), "0.0.0.0:4567");
        EmbeddedGraknTx tx = session.transaction(GraknTxType.WRITE);
        GraphTraversalSource tinker = tx.getTinkerTraversal();
        tinker.V().has("INDEX").toList().forEach(v -> {
            System.out.println("v label '" + v.label() + "', value '" + v.value("INDEX"));
            v.properties().forEachRemaining(p -> {
                System.out.println("property key '" + p.key() + "', value '" + p.value() + "'");
            });
        });
    }

    @Test
    public void insertTwoPeopleWithIdenticalNames() {
        EmbeddedGraknSession session = EmbeddedGraknSession.create(Keyspace.of("grakn"), "0.0.0.0:4567");
        EmbeddedGraknTx tx = session.transaction(GraknTxType.WRITE);

        AttributeType at = tx.putAttributeType("name", AttributeType.DataType.STRING);
        EntityType et = tx.putEntityType("person").has(at);
        Entity person1 = et.create();
        Entity person2 = et.create();
        Attribute name1 = at.create("John");
        Attribute name2 = at.create("John");
        person1.has(name1);
        person2.has(name2);

        tx.commit();
        tx.close();
        session.close();
    }

    @Test
    public void matchPersonAndHisName() {
        EmbeddedGraknSession session = EmbeddedGraknSession.create(Keyspace.of("grakn"), "0.0.0.0:4567");
        EmbeddedGraknTx tx = session.transaction(GraknTxType.READ);

        tx.graql().match(var("p").isa("person").has("name", var("n"))).get().execute().forEach(answer -> {
            System.out.println("{ " + answer.get("p").asEntity() + "} has { " + answer.get("n").asAttribute() + "}");
        });

        tx.close();
        session.close();
    }

    @Test
    public void mergeAttribute() {
        EmbeddedGraknSession session = EmbeddedGraknSession.create(Keyspace.of("grakn"), "0.0.0.0:4567");
        EmbeddedGraknTx tx = session.transaction(GraknTxType.READ);

        mergeAttribteAlgorithm(tx, "name", "John");

        tx.close();
        session.close();
    }

    private void mergeAttribteAlgorithm(EmbeddedGraknTx tx, String label, String value) {
        String index = "ATTRIBUTE" + "-" + label + "-" + value;

        GraphTraversal<Vertex, Vertex> attributes = tx.getTinkerTraversal().V().has("INDEX", index);

        Vertex main = null;
        while (attributes.hasNext()) {
            if (main == null) {
                main = attributes.next();
                System.out.println("main attr = " + main);
            }
            else {
                Vertex duplicate = attributes.next();
//                Iterator<Vertex> neighbors = duplicate.vertices(Direction.BOTH);
//                System.out.print("dup attr = " + duplicate + ". neighbors = ");
//                neighbors.forEachRemaining(neighbor -> {
//                    System.out.print(neighbor + ", ");
//
//                });

//                tx.getTinkerTraversal().V(duplicate).bothE("attribute").outV();

                Iterator<Vertex> neighbors = duplicate.vertices(Direction.OUT);
                duplicate.remove();
                System.out.println();
            }
        }
    }
}
