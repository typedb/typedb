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

import grakn.core.common.util.Collections;
import grakn.core.concept.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.deduplicator.AttributeDeduplicator;
import grakn.core.server.deduplicator.KeyspaceAttributeTriple;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.SessionFactory;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

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
    public void closeSession() {
        session.close();
    }

    @Test
    public void shouldDeduplicateAttributes() {
        String testAttributeLabel = "test-attribute";
        String testAttributeValue = "test-attribute-value";

        // define the schema
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.define(type(testAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING)));
            tx.commit();
        }

        // insert 3 instances with the same value
        GraqlInsert query = Graql.insert(var("x").isa(testAttributeLabel).val(testAttributeValue));

        insertConcurrently(query, 16);

        // verify there are 16 attribute instances in the graph before deduplication
        try (TransactionOLTP tx = session.transaction().read()) {
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(var(testAttributeLabel).isa(testAttributeLabel).val(testAttributeValue)).get());
            assertThat(conceptMaps, hasSize(16));
        }

        String attributeIndex = Schema.generateAttributeIndex(Label.of(testAttributeLabel), testAttributeValue);
        // perform deduplicate on the instances
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceAttributeTriple.create(session.keyspace(), Label.of(testAttributeLabel), attributeIndex));

        // verify if we only have 1 instances after deduplication
        try (TransactionOLTP tx = session.transaction().read()) {
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(var(testAttributeLabel).isa(testAttributeLabel).val(testAttributeValue)).get());
            assertThat(conceptMaps, hasSize(1));
        }
    }

    @Test
    public void shouldAlsoMergeHasEdgesInTheDeduplicating1() {
        String ownedAttributeLabel = "owned-attribute";
        String ownedAttributeValue = "owned-attribute-value";

        // define the schema
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.define(
                    type(ownedAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // insert 3 "owner" (which is an entity) and "owned-attribute". each "owner" has an "owned-attribute"
        GraqlInsert query = Graql.insert(var().isa("owner").has(ownedAttributeLabel, ownedAttributeValue));
        insertConcurrently(query, 3);

        // verify there are 3 owners with 3 different attribute instances
        try (TransactionOLTP tx = session.transaction().read()) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get());
            for (ConceptMap conceptMap : conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(3));
            assertThat(owner, hasSize(3));
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceAttributeTriple.create(session.keyspace(), Label.of(ownedAttributeLabel), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify there are 3 owners linked to only 1 attribute instance
        try (TransactionOLTP tx = session.transaction().read()) {
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
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.define(
                    type(ownedAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type(ownerLabel).sub("attribute").datatype(Graql.Token.DataType.STRING).has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // insert 3 "owner" and 3 "owned-attribute". each "owner" has an "owned attribute"
        GraqlInsert query1 = Graql.insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue));
        GraqlInsert query2 = Graql.insert(var().isa(ownerLabel).val(ownerValue1).has(ownedAttributeLabel, ownedAttributeValue));
        GraqlInsert query3 = Graql.insert(var().isa(ownerLabel).val(ownerValue2).has(ownedAttributeLabel, ownedAttributeValue));
        insertConcurrently(Collections.list(query1, query2, query3));

        // verify before deduplication
        try (TransactionOLTP tx = session.transaction().read()) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa(ownerLabel)).get());
            for (ConceptMap conceptMap : conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asAttribute().id().getValue());
            }

            assertThat(owned, hasSize(3));
            assertThat(owner, hasSize(3));
        }


        // deduplicate
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceAttributeTriple.create(session.keyspace(), Label.of(ownedAttributeLabel), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceAttributeTriple.create(session.keyspace(), Label.of(ownerLabel), Schema.generateAttributeIndex(Label.of(ownerLabel), ownerValue1)));
        AttributeDeduplicator.deduplicate(sessionFactory, KeyspaceAttributeTriple.create(session.keyspace(), Label.of(ownerLabel), Schema.generateAttributeIndex(Label.of(ownerLabel), ownerValue1)));

        // verify
        try (TransactionOLTP tx = session.transaction().read()) {
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
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.define(
                    type(ownedAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        GraqlInsert query1 = Graql.parse("insert $owned \"" + ownedAttributeValue + "\"isa owned-attribute; $owner1 isa owner, has owned-attribute $owned; $owner2 isa owner, has owned-attribute $owned;").asInsert();
        GraqlInsert query2 = Graql.parse("insert $owned \"" + ownedAttributeValue + "\" isa owned-attribute; $owner1 isa owner, has owned-attribute $owned; $owner2 isa owner, has owned-attribute $owned;").asInsert();
        GraqlInsert query3 = Graql.parse("insert $owned \"" + ownedAttributeValue + "\" isa owned-attribute; $owner1 isa owner, has owned-attribute $owned;").asInsert();
        insertConcurrently(Collections.list(query1, query2, query3));

        // verify before deduplication
        try (TransactionOLTP tx = session.transaction().read()) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get());
            for (ConceptMap conceptMap : conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(3));
            assertThat(owner, hasSize(5));
        }

        // perform deduplicate on the attribute
        AttributeDeduplicator.deduplicate(sessionFactory,
                KeyspaceAttributeTriple.create(session.keyspace(), Label.of(ownedAttributeLabel), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionOLTP tx = session.transaction().read()) {
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
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.define(
                    type(ownedAttributeLabel).sub("attribute").datatype(Graql.Token.DataType.STRING),
                    type("owner").sub("entity").has(ownedAttributeLabel)
            ));
            tx.commit();
        }

        // use the 'via' feature when inserting to force reification
        GraqlInsert query1 = Graql.parse("insert $owner isa owner, has owned-attribute '" + ownedAttributeValue + "' via $reified;").asInsert();
        GraqlInsert query2 = Graql.parse("insert $owner isa owner, has owned-attribute '" + ownedAttributeValue + "' via $reified;").asInsert();
        GraqlInsert query3 = Graql.parse("insert $owner isa owner, has owned-attribute '" + ownedAttributeValue + "' via $reified;").asInsert();
        insertConcurrently(Collections.list(query1, query2, query3));

        // verify before deduplication
        try (TransactionOLTP tx = session.transaction().read()) {
            Set<String> owned = new HashSet<>();
            Set<String> owner = new HashSet<>();
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(
                    var("owned").isa(ownedAttributeLabel).val(ownedAttributeValue),
                    var("owner").isa("owner")).get());
            for (ConceptMap conceptMap : conceptMaps) {
                owned.add(conceptMap.get("owned").asAttribute().id().getValue());
                owner.add(conceptMap.get("owner").asEntity().id().getValue());
            }

            assertThat(owned, hasSize(3));
            assertThat(owner, hasSize(3));
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(sessionFactory,
                KeyspaceAttributeTriple.create(session.keyspace(), Label.of(ownedAttributeLabel), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionOLTP tx = session.transaction().read()) {
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
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.define(
                    type("owner").sub("relation").relates("entity-role-player").relates("attribute-role-player"),
                    type("owned-entity").sub("entity").plays("entity-role-player"),
                    type(ownedAttributeLabel).sub("attribute").plays("attribute-role-player").datatype(Graql.Token.DataType.STRING)
            ));
            tx.commit();
        }

        // insert relations, each having an attribute as one of the role player
        GraqlInsert query1 = Graql.insert(
                var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp")));
        GraqlInsert query2 = Graql.insert(
                var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp")));
        GraqlInsert query3 = Graql.insert(
                var("erp").isa("owned-entity"), var("arp").isa(ownedAttributeLabel).val(ownedAttributeValue),
                var("owner").isa("owner").rel("entity-role-player", var("erp")).rel("attribute-role-player", var("arp")));
        insertConcurrently(Collections.list(query1, query2, query3));

        // verify before deduplication
        try (TransactionOLTP tx = session.transaction().read()) {
            List<ConceptMap> conceptMaps = tx.execute(Graql.match(var("owner").isa("owner")
                    .rel("attribute-role-player", var("arp"))
            ).get());
            Set<String> owner = new HashSet<>();
            Set<String> arp = new HashSet<>();
            for (ConceptMap conceptMap : conceptMaps) {
                owner.add(conceptMap.get("owner").asRelation().id().getValue());
                arp.add(conceptMap.get("arp").asAttribute().id().getValue());
            }

            assertThat(arp, hasSize(3));
            assertThat(owner, hasSize(3));
        }

        // deduplicate
        AttributeDeduplicator.deduplicate(sessionFactory,
                KeyspaceAttributeTriple.create(session.keyspace(), Label.of(ownedAttributeLabel), Schema.generateAttributeIndex(Label.of(ownedAttributeLabel), ownedAttributeValue)));

        // verify
        try (TransactionOLTP tx = session.transaction().read()) {
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

    private void insertConcurrently(GraqlInsert query, int repetitions) {
        List<GraqlInsert> queries = new ArrayList<>();
        for (int i = 0; i < repetitions; i++) {
            queries.add(query);
        }
        insertConcurrently(queries);
    }

    private void insertConcurrently(Collection<GraqlInsert> queries) {
        // use latch to make sure all threads will insert a new attribute instance
        CountDownLatch commitLatch = new CountDownLatch(queries.size());
        ExecutorService executorService = Executors.newFixedThreadPool(queries.size());
        List<Future> futures = new ArrayList<>();
        queries.forEach(query -> {
            futures.add(executorService.submit(() -> {
                TransactionOLTP tx = session.transaction().write();
                tx.execute(query);
                commitLatch.countDown();
                try {
                    commitLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                tx.commit();
            }));
        });
        for (Future future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}