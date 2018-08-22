package ai.grakn.test.engine.attribute.uniqueness;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.attribute.uniqueness.AttributeDeduplicator;
import ai.grakn.engine.attribute.uniqueness.KeyspaceIndexPair;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class AttributeDeduplicatorIT {
    @ClassRule
    public static EmbeddedCassandraContext cassandra = EmbeddedCassandraContext.create();

    @Test
    public void shouldDeduplicateAttributesCorrectly() {
        // the attribute value and the keyspace it belongs to
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        String ownedAttributeValue = "owned-attribute-value";
        KeyspaceIndexPair keyspaceIndexPairs = KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + "owned-attribute" + "-" + ownedAttributeValue);

        // initialise keyspace & define the tx factory
        GraknConfig config = GraknConfig.create();
        KeyspaceStoreImpl keyspaceStore = new KeyspaceStoreImpl(config);
        keyspaceStore.loadSystemSchema();
        keyspaceStore.addKeyspace(keyspace);
        EngineGraknTxFactory txFactory = EngineGraknTxFactory.create(new ProcessWideLockProvider(), config, keyspaceStore);

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label("owned-attribute").sub("attribute").datatype(AttributeType.DataType.STRING)
            ).execute();
            tx.commit();
        }

        // insert some data
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().insert(var().isa("owned-attribute").val(ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owned-attribute").val(ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owned-attribute").val(ownedAttributeValue)).execute();
            tx.commit();
        }

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(var("x").isa("owned-attribute").val(ownedAttributeValue)).get().execute();
            assertThat(conceptMaps, hasSize(3));
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory, new HashSet<>(Arrays.asList(keyspaceIndexPairs)));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(var("x").isa("owned-attribute").val(ownedAttributeValue)).get().execute();
            assertThat(conceptMaps, hasSize(1));
        }
    }

    @Test
    public void shouldDeduplicateAttributesConnectedToEntitiesCorrectly() {
        // the attribute value and the keyspace it belongs to
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        String ownedAttributeValue = "owned-attribute-value";
        KeyspaceIndexPair keyspaceIndexPairs = KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + "owned-attribute" + "-" + ownedAttributeValue);

        // initialise keyspace & define the tx factory
        GraknConfig config = GraknConfig.create();
        KeyspaceStoreImpl keyspaceStore = new KeyspaceStoreImpl(config);
        keyspaceStore.loadSystemSchema();
        keyspaceStore.addKeyspace(keyspace);
        EngineGraknTxFactory txFactory = EngineGraknTxFactory.create(new ProcessWideLockProvider(), config, keyspaceStore);

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label("owned-attribute").sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has("owned-attribute")
            ).execute();
            tx.commit();
        }

        // insert some data
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().insert(var().isa("owner").has("owned-attribute", ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner").has("owned-attribute", ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner").has("owned-attribute", ownedAttributeValue)).execute();
            tx.commit();
        }

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("a").isa("owned-attribute").val(ownedAttributeValue),
                    var("o").isa("owner")).get().execute();
            Set<String> a = new HashSet<>();
            Set<String> o = new HashSet<>();
            for (ConceptMap conceptMap: conceptMaps) {
                a.add(conceptMap.get("a").asAttribute().id().getValue());
                o.add(conceptMap.get("o").asEntity().id().getValue());
            }

            assertThat(a, hasSize(3));
            assertThat(o, hasSize(3));
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory, new HashSet<>(Arrays.asList(keyspaceIndexPairs)));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa("owned-attribute").val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            for (ConceptMap conceptMap: conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(3));
        }
    }

    @Test
    public void shouldDeduplicateAttributesConnectedToAttributesCorrectly() {
        // the attribute value and the keyspace it belongs to
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        String ownedAttributeValue = "owned-attribute-value";
        KeyspaceIndexPair keyspaceIndexPairs = KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + "owned-attribute" + "-" + ownedAttributeValue);

        // initialise keyspace & define the tx factory
        GraknConfig config = GraknConfig.create();
        KeyspaceStoreImpl keyspaceStore = new KeyspaceStoreImpl(config);
        keyspaceStore.loadSystemSchema();
        keyspaceStore.addKeyspace(keyspace);
        EngineGraknTxFactory txFactory = EngineGraknTxFactory.create(new ProcessWideLockProvider(), config, keyspaceStore);

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label("owned-attribute").sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("attribute").datatype(AttributeType.DataType.STRING).has("owned-attribute")
            ).execute();
            tx.commit();
        }

        // insert some data
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().insert(var().isa("owner").val("owner-value-1").has("owned-attribute", ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner").val("owner-value-1").has("owned-attribute", ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner").val("owner-value-2").has("owned-attribute", ownedAttributeValue)).execute();
            tx.commit();
        }

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa("owned-attribute").val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            for (ConceptMap conceptMap: conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asAttribute().id().getValue());
            }

            assertThat(owned, hasSize(3));
            assertThat(owner, hasSize(3));
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory, new HashSet<>(Arrays.asList(keyspaceIndexPairs)));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa("owned-attribute").val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            for (ConceptMap conceptMap: conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asAttribute().id().getValue());
            }

            assertThat(owned, hasSize(2));
            assertThat(owner, hasSize(1));
        }
    }
}
