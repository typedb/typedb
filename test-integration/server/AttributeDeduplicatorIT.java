package grakn.core.server;

import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Label;
import grakn.core.server.session.TransactionImpl;
import grakn.core.server.deduplicator.AttributeDeduplicator;
import grakn.core.server.deduplicator.KeyspaceIndexPair;
import grakn.core.server.session.SessionStore;
import grakn.core.server.session.SessionImpl;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.rule.ConcurrentGraknServer;
import grakn.core.graql.internal.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static grakn.core.graql.Graql.label;
import static grakn.core.graql.Graql.var;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@SuppressWarnings("CheckReturnValue")
public class AttributeDeduplicatorIT {
    private SessionImpl session;
    private SessionStore txFactory;

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        txFactory = server.txFactory();
    }

    @After
    public void closeSession() { session.close(); }

    @Test
    public void shouldDeduplicateAttributes() {
        String testAttributeLabel = "test-attribute";
        String testAttributeValue = "test-attribute-value";

        // define the schema
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().define(label(testAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING)).execute();
            tx.commit();
        }

        // insert 3 instances with the same value
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().insert(var().isa(testAttributeLabel).val(testAttributeValue)).execute();
            tx.graql().insert(var().isa(testAttributeLabel).val(testAttributeValue)).execute();
            tx.graql().insert(var().isa(testAttributeLabel).val(testAttributeValue)).execute();
            tx.commit();
        }

        // perform deduplicate on the instances
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(testAttributeLabel), testAttributeValue)));

        // verify if we only have 1 instances after deduplication
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(var(testAttributeLabel).isa(testAttributeLabel).val(testAttributeValue)).get().execute();
            assertThat(conceptMaps, hasSize(1));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesInTheDeduplicating1() {
        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ).execute();
            tx.commit();
        }

        // insert 3 "owner" (which is an entity) and "owned-attribute". each "owner" has an "owned-attribute"
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.graql().insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.commit();
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            for (ConceptMap conceptMap : conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(3));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesWhenDeduplicating2() {
        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";
        String ownerLabel = "owner";
        String ownerValue1 = "owner-value-1";
        String ownerValue2 = "owner-value-2";

        // define the schema
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label(ownerLabel).sub("attribute").datatype(AttributeType.DataType.STRING).has(ownedAttributeLabel)
            ).execute();
            tx.commit();
        }

        // insert 3 "owner" and 3 "owned-attribute". each "owner" has an "owned attribute"
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.graql().insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.graql().insert(var().isa(ownerLabel).val(ownerValue2).has(ownedAttributeLabel, ownedAttributeValue)).execute();
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownerLabel), ownerValue1)));
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownerLabel), ownerValue1)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa(ownerLabel)).get().execute();
            for (ConceptMap conceptMap : conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asAttribute().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(2));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesInTheDeduplicating3() {
        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ).execute();
            tx.commit();
        }

        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().parse("insert $owned isa owned-attribute \"" + ownedAttributeValue + "\"; $owner1 isa owner has owned-attribute $owned; $owner2 isa owner has owned-attribute $owned;").execute();
            tx.graql().parse("insert $owned isa owned-attribute \"" + ownedAttributeValue + "\"; $owner1 isa owner has owned-attribute $owned; $owner2 isa owner has owned-attribute $owned;").execute();
            tx.graql().parse("insert $owned isa owned-attribute \"" + ownedAttributeValue + "\"; $owner1 isa owner has owned-attribute $owned;").execute();
            tx.commit();
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(txFactory,
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            for (ConceptMap conceptMap : conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(5));
        }
    }

    @Test
    public void shouldAlsoMergeReifiedEdgesWhenDeduplicating() {
        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ).execute();
            tx.commit();
        }

        // use the 'via' feature when inserting to force reification
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;").execute();
            tx.graql().parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;").execute();
            tx.graql().parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;").execute();
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory,
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.graql().match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get().execute();
            for (ConceptMap conceptMap : conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(1));
            assertThat(owner, hasSize(3));
        }
    }

    @Test
    public void shouldAlsoMergeRolePlayerEdgesInTheDeduplicating() {

        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.graql().define(
                    label("owner").sub("relationship").relates("entity-role-player").relates("attribute-role-player"),
                    label("owned-entity").sub("entity").plays("entity-role-player"),
                    label(ownedAttributeLabel).sub("attribute").plays("attribute-role-player").datatype(AttributeType.DataType.STRING)
            ).execute();
            tx.commit();
        }

        // insert relationships, each having an attribute as one of the role player
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
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
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptMap> conceptMaps = tx.graql().match(var("owner").isa("owner")
                    .rel("attribute-role-player", var("arp"))
            ).get().execute();
            Set<String> owner = new HashSet<>();
            Set<String> arp = new HashSet<>();
            for (ConceptMap conceptMap : conceptMaps) {
                owner.add(conceptMap.get("owner").asRelationship().id().getValue());
                arp.add(conceptMap.get("arp").asAttribute().id().getValue());
            }

            assertThat(arp, hasSize(1));
            assertThat(owner, hasSize(3));
        }
    }
}