/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Label;
import grakn.core.graql.internal.Schema;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.deduplicator.AttributeDeduplicator;
import grakn.core.server.deduplicator.KeyspaceIndexPair;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.SessionFactory;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AttributeDeduplicatorIT {
    private SessionImpl session;
    private SessionFactory sessionFactory;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        sessionFactory = server.sessionFactory();
    }

    @After
    public void closeSession() { session.close(); }

    @Test
    public void shouldDeduplicateAttributes() {
        String testAttributeLabel = "test-attribute";
        String testAttributeValue = "test-attribute-value";

        // define the schema
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(type(testAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING)));
            tx.commit();
        }

        // insert 3 instances with the same value
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.insert(var().isa(testAttributeLabel).val(testAttributeValue)));
            tx.execute(Graql.insert(var().isa(testAttributeLabel).val(testAttributeValue)));
            tx.execute(Graql.insert(var().isa(testAttributeLabel).val(testAttributeValue)));
            tx.commit();
        }

        // perform deduplicate on the instances
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(testAttributeLabel), testAttributeValue)));

        // verify if we only have 1 instances after deduplication
        try (TransactionOLTP tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(var(testAttributeLabel).isa(testAttributeLabel).val(testAttributeValue)).get());
            assertThat(conceptMaps, hasSize(1));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesInTheDeduplicating1() {
        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(
                    type(ownedAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // insert 3 "owner" (which is an entity) and "owned-attribute". each "owner" has an "owned-attribute"
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)));
            tx.execute(Graql.insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)));
            tx.execute(Graql.insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue)));
            tx.commit();
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionOLTP tx = session.transaction(Transaction.Type.READ)) {
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
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(
                    type(ownedAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type(ownerLabel).sub("attribute").datatype(Graql.Token.DataType.STRING).has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // insert 3 "owner" and 3 "owned-attribute". each "owner" has an "owned attribute"
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue)));
            tx.execute(Graql.insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue)));
            tx.execute(Graql.insert(var().isa(ownerLabel).val(ownerValue2).has(ownedAttributeLabel, ownedAttributeValue)));
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownerLabel), ownerValue1)));
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownerLabel), ownerValue1)));

        // verify
        try (TransactionOLTP tx = session.transaction(Transaction.Type.READ)) {
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
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(
                    type(ownedAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("insert $owned \"" + ownedAttributeValue + "\"isa owned-attribute; $owner1 isa owner, has owned-attribute $owned; $owner2 isa owner, has owned-attribute $owned;").asInsert());
            tx.execute(Graql.parse("insert $owned \"" + ownedAttributeValue + "\" isa owned-attribute; $owner1 isa owner, has owned-attribute $owned; $owner2 isa owner, has owned-attribute $owned;").asInsert());
            tx.execute(Graql.parse("insert $owned \"" + ownedAttributeValue + "\" isa owned-attribute; $owner1 isa owner, has owned-attribute $owned;").asInsert());
            tx.commit();
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(sessionFactory,
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionOLTP tx = session.transaction(Transaction.Type.READ)) {
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
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(
                    type(ownedAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // use the 'via' feature when inserting to force reification
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("insert $owner isa owner, has owned-attribute '" + ownedAttributeValue + "' via $reified;").asInsert());
            tx.execute(Graql.parse("insert $owner isa owner, has owned-attribute '" + ownedAttributeValue + "' via $reified;").asInsert());
            tx.execute(Graql.parse("insert $owner isa owner, has owned-attribute '" + ownedAttributeValue + "' via $reified;").asInsert());
            tx.commit();
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(sessionFactory,
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionOLTP tx = session.transaction(Transaction.Type.READ)) {
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
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.define(
                    type("owner").sub("relationship").relates("entity-role-player").relates("attribute-role-player"),
                    type("owned-entity").sub("entity").plays("entity-role-player"),
                    type(ownedAttributeLabel).sub("attribute").plays("attribute-role-player").datatype(Graql.Token.DataType.STRING)
            ));
            tx.commit();
        }

        // insert relationships, each having an attribute as one of the role player
        try (TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
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
        AttributeDeduplicator.deduplicate(sessionFactory,
                KeyspaceIndexPair.create(session.keyspace(), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionOLTP tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(var("owner").isa("owner")
                    .rel("attribute-role-player", var("arp"))
            ).get());
            Set<String> owner = new HashSet<>();
            Set<String> arp = new HashSet<>();
            for (ConceptMap conceptMap : conceptMaps) {
                owner.add(conceptMap.get("owner").asRelation().id().getValue());
                arp.add(conceptMap.get("arp").asAttribute().id().getValue());
            }

            assertThat(arp, hasSize(1));
            assertThat(owner, hasSize(3));
        }
    }
}