/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.internal.cache;

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.TxTestBase;
import ai.grakn.kb.internal.concept.RelationshipImpl;
import ai.grakn.kb.internal.structure.Casting;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 *
 * Tests to ensure that future code changes do not cause concepts to be missed by the tracking functionality.
 * This is very important to ensure validation is applied to ALL concepts that have been added/changed plus
 * and concepts that have had new vertices added.
 *
 */
public class TxCacheTest extends TxTestBase {

    @Test
    public void whenNewAddingTypesToTheGraph_EnsureTheConceptLogContainsThem() {
        // add concepts to rootGraph in as many ways as possible
        tx.putEntityType("1");
        RelationshipType t2 = tx.putRelationshipType("2");
        Role t3 = tx.putRole("3");
        tx.putAttributeType("4", AttributeType.DataType.STRING);

        // verify the concepts that we expected are returned in the set
        assertThat(tx.txCache().getConceptCache().values(), hasItem(t3));
        assertThat(tx.txCache().getConceptCache().values(), hasItem(t2));
    }

    @Test
    public void whenCreatingRelations_EnsureRolePlayersAreCached(){
        Role r1 = tx.putRole("r1");
        Role r2 = tx.putRole("r2");
        EntityType t1 = tx.putEntityType("t1").plays(r1).plays(r2);
        RelationshipType rt1 = tx.putRelationshipType("rel1").relates(r1).relates(r2);

        Entity e1 = t1.addEntity();
        Entity e2 = t1.addEntity();

        assertThat(tx.txCache().getModifiedCastings(), empty());

        Set<Casting> castings = ((RelationshipImpl) rt1.addRelationship().
                addRolePlayer(r1, e1).
                addRolePlayer(r2, e2)).reified().get().
                castingsRelation().collect(toSet());

        assertTrue(tx.txCache().getModifiedCastings().containsAll(castings));
    }

    @Test
    public void whenCreatingSuperTypes_EnsureLogContainsSubTypeCastings() {
        Role r1 = tx.putRole("r1");
        Role r2 = tx.putRole("r2");
        EntityType t1 = tx.putEntityType("t1").plays(r1).plays(r2);
        EntityType t2 = tx.putEntityType("t2");
        RelationshipType rt1 = tx.putRelationshipType("rel1").relates(r1).relates(r2);
        Entity i1 = t1.addEntity();
        Entity i2 = t1.addEntity();
        RelationshipImpl relation = (RelationshipImpl) rt1.addRelationship().addRolePlayer(r1, i1).addRolePlayer(r2, i2);

        tx.commit();
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).transaction(GraknTxType.WRITE);

        assertThat(tx.txCache().getModifiedCastings(), is(empty()));

        t1.sup(t2);
        assertTrue(tx.txCache().getModifiedCastings().containsAll(relation.reified().get().castingsRelation().collect(toSet())));
    }

    @Test
    public void whenCreatingInstances_EnsureLogContainsInstance() {
        EntityType t1 = tx.putEntityType("1");

        tx.commit();
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).transaction(GraknTxType.WRITE);

        Entity i1 = t1.addEntity();
        assertThat(tx.txCache().getConceptCache().values(), hasItem(i1));
    }

    @Test
    public void whenDeletingAnInstanceWithNoRelations_EnsureLogIsEmpty(){
        EntityType t1 = tx.putEntityType("1");
        Entity i1 = t1.addEntity();

        tx.commit();
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).transaction(GraknTxType.WRITE);

        assertThat(tx.txCache().getModifiedThings(), is(empty()));

        i1.delete();
        assertThat(tx.txCache().getModifiedThings(), is(empty()));
    }

    @Test
    public void whenAddingAndRemovingInstancesFromTypes_EnsureLogTracksNumberOfChanges(){
        EntityType entityType = tx.putEntityType("My Type");
        RelationshipType relationshipType = tx.putRelationshipType("My Relationship Type");

        TxCache txCache = tx.txCache();
        assertThat(txCache.getShardingCount().keySet(), empty());

        //Add some instances
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        relationshipType.addRelationship();
        assertEquals(2, (long) txCache.getShardingCount().get(entityType.getId()));
        assertEquals(1, (long) txCache.getShardingCount().get(relationshipType.getId()));

        //Remove an entity
        e1.delete();
        assertEquals(1, (long) txCache.getShardingCount().get(entityType.getId()));
        assertEquals(1, (long) txCache.getShardingCount().get(relationshipType.getId()));

        //Remove another entity
        e2.delete();
        assertFalse(txCache.getShardingCount().containsKey(entityType.getId()));
        assertEquals(1, (long) txCache.getShardingCount().get(relationshipType.getId()));
    }

    @Test
    public void whenClosingTransaction_EnsureTransactionCacheIsEmpty(){
        TxCache cache = tx.txCache();

        //Load some sample data
        AttributeType<String> attributeType = tx.putAttributeType("Attribute Type", AttributeType.DataType.STRING);
        Role role1 = tx.putRole("role 1");
        Role role2 = tx.putRole("role 2");
        EntityType entityType = tx.putEntityType("My Type").plays(role1).plays(role2).attribute(attributeType);
        RelationshipType relationshipType = tx.putRelationshipType("My Relationship Type").relates(role1).relates(role2);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        Attribute<String> r1 = attributeType.putAttribute("test");

        e1.attribute(r1);
        relationshipType.addRelationship().addRolePlayer(role1, e1).addRolePlayer(role2, e2);

        //Check the caches are not empty
        assertThat(cache.getConceptCache().keySet(), not(empty()));
        assertThat(cache.getSchemaConceptCache().keySet(), not(empty()));
        assertThat(cache.getLabelCache().keySet(), not(empty()));
        assertThat(cache.getShardingCount().keySet(), not(empty()));
        assertThat(cache.getModifiedCastings(), not(empty()));

        //Close the transaction
        tx.commit();

        //Check the caches are empty
        assertThat(cache.getConceptCache().keySet(), empty());
        assertThat(cache.getSchemaConceptCache().keySet(), empty());
        assertThat(cache.getLabelCache().keySet(), empty());
        assertThat(cache.getShardingCount().keySet(), empty());
        assertThat(cache.getModifiedThings(), empty());
        assertThat(cache.getModifiedRoles(), empty());
        assertThat(cache.getModifiedRelationshipTypes(), empty());
        assertThat(cache.getModifiedRules(), empty());
        assertThat(cache.getModifiedCastings(), empty());
    }

    @Test
    public void whenMutatingSuperTypeOfConceptCreatedInAnotherTransaction_EnsureTransactionBoundConceptIsMutated(){
        EntityType e1 = tx.putEntityType("e1");
        EntityType e2 = tx.putEntityType("e2").sup(e1);
        EntityType e3 = tx.putEntityType("e3");
        tx.commit();

        //Check everything is okay
        tx = session.transaction(GraknTxType.WRITE);
        assertTxBoundConceptMatches(e2, Type::sup, is(e1));

        //Mutate Super Type
        e2.sup(e3);
        assertTxBoundConceptMatches(e2, Type::sup, is(e3));
    }

    @Test
    public void whenDeletingType_EnsureItIsRemovedFromTheCache(){
        String label = "e1";
        tx.putEntityType(label);
        tx.commit();

        tx = tx();
        EntityType entityType = tx.getEntityType(label);
        assertNotNull(entityType);
        assertTrue(tx.txCache().isTypeCached(Label.of(label)));
        entityType.delete();

        assertNull(tx.getEntityType(label));
        assertFalse(tx.txCache().isTypeCached(Label.of(label)));
        tx.commit();

        tx = tx();
        assertNull(tx.getEntityType(label));
        assertFalse(tx.txCache().isTypeCached(Label.of(label)));
    }

    @Test
    public void whenMutatingRoleTypesOfTypeCreatedInAnotherTransaction_EnsureTransactionBoundConceptsAreMutated(){
        Role rol1 = tx.putRole("role1");
        Role rol2 = tx.putRole("role2");
        EntityType e1 = tx.putEntityType("e1").plays(rol1).plays(rol2);
        EntityType e2 = tx.putEntityType("e2");
        RelationshipType rel = tx.putRelationshipType("rel").relates(rol1).relates(rol2);
        tx.commit();

        //Check concepts match what is in transaction cache
        tx = session.transaction(GraknTxType.WRITE);
        assertTxBoundConceptMatches(e1, t -> t.plays().collect(toSet()), containsInAnyOrder(rol1, rol2));
        assertTxBoundConceptMatches(rel, t -> t.relates().collect(toSet()), containsInAnyOrder(rol1, rol2));
        assertTxBoundConceptMatches(rol1, t -> t.playedByTypes().collect(toSet()), containsInAnyOrder(e1));
        assertTxBoundConceptMatches(rol2, t -> t.playedByTypes().collect(toSet()), containsInAnyOrder(e1));
        assertTxBoundConceptMatches(rol1, t -> t.relationshipTypes().collect(toSet()), containsInAnyOrder(rel));
        assertTxBoundConceptMatches(rol2, t -> t.relationshipTypes().collect(toSet()), containsInAnyOrder(rel));

        //Role Type 1 and 2 played by e2 now
        e2.plays(rol1);
        e2.plays(rol2);
        assertTxBoundConceptMatches(rol1, t -> t.playedByTypes().collect(toSet()), containsInAnyOrder(e1, e2));
        assertTxBoundConceptMatches(rol2, t -> t.playedByTypes().collect(toSet()), containsInAnyOrder(e1, e2));

        //e1 no longer plays role 1
        e1.deletePlays(rol1);
        assertTxBoundConceptMatches(rol1, t -> t.playedByTypes().collect(toSet()), containsInAnyOrder(e2));
        assertTxBoundConceptMatches(rol2, t -> t.playedByTypes().collect(toSet()), containsInAnyOrder(e1, e2));

        //Role 2 no longer part of relation type
        rel.deleteRelates(rol2);
        assertTxBoundConceptMatches(rol2, t -> t.relationshipTypes().collect(toSet()), empty());
        assertTxBoundConceptMatches(rel, t -> t.relates().collect(toSet()), containsInAnyOrder(rol1));
    }

    /**
     * Helper method which will check that the cache and the provided type have the same expected values.
     *
     * @param type The type to check against as well as retreive from the concept cache
     * @param resultSupplier The result of executing some operation on the type
     * @param expectedMatch The expected result of the above operation
     */
    @SuppressWarnings("unchecked")
    private <T extends SchemaConcept> void assertTxBoundConceptMatches(T type, Function<T, Object> resultSupplier, Matcher expectedMatch){
        assertThat(resultSupplier.apply(type), expectedMatch);
        assertThat(resultSupplier.apply(tx.txCache().getCachedSchemaConcept(type.getLabel())), expectedMatch);
    }
}
