package ai.grakn.engine;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.concept.AttributeImpl;
import ai.grakn.util.Schema;
import ai.grakn.util.SimpleURI;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

import static ai.grakn.graql.Graql.*;

public class ScratchpadTest {
    @Test
    public void printTheAnswerFromMatchIsaNameGet() throws InterruptedException {
        SimpleURI grakn = new SimpleURI("localhost", 48555);
        Keyspace keyspace = Keyspace.of("grakn6"); //

        try (Grakn.Session session = Grakn.session(grakn, keyspace)) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                tx.graql().match(var("n").isa("name")).get().execute().forEach(n -> {
                    System.out.println(n.get("n").id() + " = " + n.get("n").asAttribute().value());
                });
            }
        }
    }


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

    @Test
    public void doInsertDuplicates() {
        String grakn = "localhost:48555";
        String keyspaceFriendlyNameWithDate = ("grakn_" + (new Date()).toString().replace(" ", "_").replace(":", "_")).toLowerCase();
        System.out.println("keyspace name = '" + keyspaceFriendlyNameWithDate + "'");
        Keyspace keyspace = Keyspace.of(keyspaceFriendlyNameWithDate);
        String name = "John";
        int duplicateCount = 180;

        // TODO: check that we've turned off janus index and propertyUnique

        try (Grakn.Session session = Grakn.session(new SimpleURI(grakn), keyspace)) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                tx.graql().define(
                        label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                        label("parent").sub("role"),
                        label("child").sub("role"),
                        label("person").sub("entity").has("name").plays("parent").plays("child"),
                        label("parentchild").sub("relationship").relates("parent").relates("child")
                ).execute();
                tx.commit();
            }
        }

        try (Grakn.Session session = Grakn.session(new SimpleURI(grakn), keyspace)) {
            System.out.println("inserting a new name attribute with value '" + name + "'...");
            for (int i = 0; i < duplicateCount; ++i) {
                try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                    tx.graql().insert(var().isa("name").val(name)).execute();
                    tx.commit();
                }
            }
            System.out.println("done.");
        }
    }
}
