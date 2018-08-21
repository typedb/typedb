package ai.grakn.test.engine.attribute.uniqueness;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.concept.AttributeType;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.attribute.uniqueness.AttributeDeduplicator;
import ai.grakn.engine.attribute.uniqueness.KeyspaceValuePair;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;

public class AttributeDeduplicatorIT {
    @ClassRule
    public static final EmbeddedCassandraContext cassandra = EmbeddedCassandraContext.create();

    // TODO: fix
    @Test
    public void shouldMergeAttributesCorrectly() {
        // define the tx factory
        GraknConfig config = GraknConfig.create();
        KeyspaceStoreImpl keyspaceStore = new KeyspaceStoreImpl(config);
        keyspaceStore.loadSystemSchema();
        EngineGraknTxFactory txFactory = EngineGraknTxFactory.create(new ProcessWideLockProvider(), config, keyspaceStore);

        // the attribute value and the keyspace it belongs to
        Keyspace keyspace = Keyspace.of("grakn");
        String ownedAttributeValue = "owned-attribute-value";
        KeyspaceValuePair keyspaceValuePairs = KeyspaceValuePair.create(keyspace, ownedAttributeValue);

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label("owned-attribute").sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner1").sub("attribute").datatype(AttributeType.DataType.STRING).has("owned-attribute"),
                    label("owner2").sub("entity").has("owned-attribute")
            ).execute();
            tx.commit();
        }

        // insert some data
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().insert(var().isa("owner2").has("owned-attribute", ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner2").has("owned-attribute", ownedAttributeValue)).execute();
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory, new HashSet<>(Arrays.asList(keyspaceValuePairs)));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            System.out.println();
        }
    }
}
