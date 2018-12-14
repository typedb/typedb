package grakn.core.server;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.InsertQuery;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.deduplicator.AttributeDeduplicator;
import grakn.core.server.deduplicator.KeyspaceIndexPair;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.SessionStore;
import grakn.core.server.session.TransactionImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AttributeDeduplicatorIT {
    private SessionImpl session;
    private SessionStore txFactory;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

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
            tx.execute(Graql.define(label(testAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING)));
            tx.commit();
        }

        // insert 3 instances with the same value
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.insert(var().isa(testAttributeLabel).val(testAttributeValue)));
            tx.execute(Graql.insert(var().isa(testAttributeLabel).val(testAttributeValue)));
            tx.execute(Graql.insert(var().isa(testAttributeLabel).val(testAttributeValue)));
            tx.commit();
        }

        // perform deduplicate on the instances
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(testAttributeLabel), testAttributeValue)));

        // verify if we only have 1 instances after deduplication
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(var(testAttributeLabel).isa(testAttributeLabel).val(testAttributeValue)).get());
            assertThat(conceptMaps, hasSize(1));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesInTheDeduplicating1() {
        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // insert 3 "owner" (which is an entity) and "owned-attribute". each "owner" has an "owned-attribute"
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)));
            tx.execute(Graql.insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)));
            tx.execute(Graql.insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)));
            tx.commit();
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(txFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get());
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
            tx.execute(Graql.define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label(ownerLabel).sub("attribute").datatype(AttributeType.DataType.STRING).has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // insert 3 "owner" and 3 "owned-attribute". each "owner" has an "owned attribute"
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue)));
            tx.execute(Graql.insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue)));
            tx.execute(Graql.insert(var().isa(ownerLabel).val(ownerValue2).has(ownedAttributeLabel, ownedAttributeValue)));
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
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa(ownerLabel)).get());
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
            tx.execute(Graql.define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.<InsertQuery>parse("insert $owned isa owned-attribute \"" + ownedAttributeValue + "\"; $owner1 isa owner has owned-attribute $owned; $owner2 isa owner has owned-attribute $owned;"));
            tx.execute(Graql.<InsertQuery>parse("insert $owned isa owned-attribute \"" + ownedAttributeValue + "\"; $owner1 isa owner has owned-attribute $owned; $owner2 isa owner has owned-attribute $owned;"));
            tx.execute(Graql.<InsertQuery>parse("insert $owned isa owned-attribute \"" + ownedAttributeValue + "\"; $owner1 isa owner has owned-attribute $owned;"));
            tx.commit();
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(txFactory,
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get());
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
            tx.execute(Graql.define(
                    label(ownedAttributeLabel).sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // use the 'via' feature when inserting to force reification
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.<InsertQuery>parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;"));
            tx.execute(Graql.<InsertQuery>parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;"));
            tx.execute(Graql.<InsertQuery>parse("insert $owner isa owner has owned-attribute '" + ownedAttributeValue + "' via $reified;"));
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory,
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get());
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
            tx.execute(Graql.define(
                    label("owner").sub("relationship").relates("entity-role-player").relates("attribute-role-player"),
                    label("owned-entity").sub("entity").plays("entity-role-player"),
                    label(ownedAttributeLabel).sub("attribute").plays("attribute-role-player").datatype(AttributeType.DataType.STRING)
            ));
            tx.commit();
        }

        // insert relationships, each having an attribute as one of the role player
        try (TransactionImpl tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.insert(
                    var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp"))));
            tx.execute(Graql.insert(
                    var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp"))));
            tx.execute(Graql.insert(
                    var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp"))));
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(txFactory,
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionImpl tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(var("owner").isa("owner")
                    .rel("attribute-role-player", var("arp"))
            ).get());
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