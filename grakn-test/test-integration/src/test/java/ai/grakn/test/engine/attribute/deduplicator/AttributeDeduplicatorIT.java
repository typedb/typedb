package ai.grakn.test.engine.attribute.deduplicator;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.attribute.deduplicator.AttributeDeduplicator;
import ai.grakn.engine.attribute.deduplicator.KeyspaceIndexPair;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import org.junit.ClassRule;
import org.junit.Test;

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
    public void shouldDeduplicateAttributes() {
        // setup a keyspace & txFactory
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        EngineGraknTxFactory txFactory = createEngineGraknTxFactory();

        String testAttributeLabel = "test-attribute";
        String testAttributeValue = "test-attribute-value";

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(label(testAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING)).execute();
            tx.commit();
        }

        // insert 3 instances with the same value
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().insert(var().isa(testAttributeLabel).val(testAttributeValue)).execute();
            tx.graql().insert(var().isa(testAttributeLabel).val(testAttributeValue)).execute();
            tx.graql().insert(var().isa(testAttributeLabel).val(testAttributeValue)).execute();
            tx.commit();
        }

        // perform deduplicate on the instances
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + testAttributeLabel + "-" + testAttributeValue));

        // verify if we only have 1 instances after deduplication
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(var(testAttributeLabel).isa(testAttributeLabel).val(testAttributeValue)).get().execute();
            assertThat(conceptMaps, hasSize(1));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesInTheDeduplicating1() {
        // setup a keyspace & txFactory
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        EngineGraknTxFactory txFactory = createEngineGraknTxFactory();

        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ).execute();
            tx.commit();
        }

        // insert 3 "owner" (which is an entity) and "owned-attribute". each "owner" has an "owned-attribute"
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.commit();
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + ownedAttributeLabel + "-" + ownedAttributeValue));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            for (ConceptMap conceptMap: conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(3));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesWhenDeduplicating2() {
        // setup a keyspace & txFactory
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        EngineGraknTxFactory txFactory = createEngineGraknTxFactory();

        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";
        String ownerLabel = "owner";
        String ownerValue1 = "owner-value-1";
        String ownerValue2 = "owner-value-2";

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label(ownerLabel).sub("attribute").datatype(AttributeType.DataType.STRING).has(ownedAttributeLabel)
            ).execute();
            tx.commit();
        }

        // insert 3 "owner" and 3 "owned-attribute". each "owner" has an "owned attribute"
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.graql().insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.graql().insert(var().isa(ownerLabel).val(ownerValue2).has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.commit();
        }

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa(ownerLabel)).get().execute();
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
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + ownedAttributeLabel + "-" + ownedAttributeValue));
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + ownerLabel + "-" + ownerValue1));
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + ownerLabel + "-" + ownerValue2));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa(ownerLabel)).get().execute();
            for (ConceptMap conceptMap: conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asAttribute().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(2));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesInTheDeduplicating3() {
        // setup a keyspace & txFactory
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        EngineGraknTxFactory txFactory = createEngineGraknTxFactory();

        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ).execute();
            tx.commit();
        }

        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().parse("insert $owned isa owned-attribute \"" + ownedAttributeValue  + "\"; $owner1 isa owner has owned-attribute $owned; $owner2 isa owner has owned-attribute $owned;").execute();
            tx.graql().parse("insert $owned isa owned-attribute \"" + ownedAttributeValue  + "\"; $owner1 isa owner has owned-attribute $owned; $owner2 isa owner has owned-attribute $owned;").execute();
            tx.graql().parse("insert $owned isa owned-attribute \"" + ownedAttributeValue + "\"; $owner1 isa owner has owned-attribute $owned;").execute();
            tx.commit();
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(txFactory,
                KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + ownedAttributeLabel + "-" + ownedAttributeValue));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            for (ConceptMap conceptMap: conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(5));
        }
    }

    @Test
    public void shouldAlsoMergeReifiedEdgesWhenDeduplicating() {
        // setup a keyspace & txFactory
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        EngineGraknTxFactory txFactory = createEngineGraknTxFactory();

        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ).execute();
            tx.commit();
        }

        // use the 'via' feature when inserting to force reification
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;").execute();
            tx.graql().parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;").execute();
            tx.graql().parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;").execute();
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory,
                KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + ownedAttributeLabel + "-" + ownedAttributeValue));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            for (ConceptMap conceptMap: conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(3));
        }
    }

    @Test
    public void shouldAlsoMergeRolePlayerEdgesInTheDeduplicating() {
        // setup a keyspace & txFactory
        Keyspace keyspace = Keyspace.of("attrdedupit_" + UUID.randomUUID().toString().replace("-", "_"));
        EngineGraknTxFactory txFactory = createEngineGraknTxFactory();

        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().define(
                    label("owner").sub("relationship").relates("entity-role-player").relates("attribute-role-player"),
                    label("owned-entity").sub("entity").plays("entity-role-player"),
                    label(ownedAttributeLabel).sub("attribute").plays("attribute-role-player").datatype(AttributeType.DataType.STRING)
            ).execute();
            tx.commit();
        }

        // insert relationships, each having an attribute as one of the role player
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.WRITE)) {
            tx.graql().insert(
                    var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp"))).execute();
            tx.graql().insert(
                    var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp"))).execute();
            tx.graql().insert(
                    var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp"))).execute();
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory,
                KeyspaceIndexPair.create(keyspace, "ATTRIBUTE-" + ownedAttributeLabel + "-" + ownedAttributeValue));

        // verify
        try (EmbeddedGraknTx tx = txFactory.tx(keyspace, GraknTxType.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(var("owner").isa("owner")
                    .rel("attribute-role-player", var("arp"))
            ).get().execute();
            Set<String> owner = new HashSet<>();
            Set<String> arp = new HashSet<>();
            for (ConceptMap conceptMap: conceptMaps) {
                owner.add(conceptMap.get("owner").asRelationship().id().getValue());
                arp.add(conceptMap.get("arp").asAttribute().id().getValue());
            }

            assertThat(arp, hasSize(1));
            assertThat(owner, hasSize(3));
        }
    }

    /**
     * @return a new {@link EngineGraknTxFactory}
     */
    private EngineGraknTxFactory createEngineGraknTxFactory() {
        GraknConfig config = GraknConfig.create();
        KeyspaceStoreImpl keyspaceStore = new KeyspaceStoreImpl(config);
        keyspaceStore.loadSystemSchema();

        return EngineGraknTxFactory.create(new ProcessWideLockProvider(), config, keyspaceStore);
    }
}
