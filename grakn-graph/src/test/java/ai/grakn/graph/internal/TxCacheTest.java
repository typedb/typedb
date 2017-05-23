/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.ResourceType;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 *
 * Tests to ensure that future code changes do not cause concepts to be missed by the tracking functionality.
 * This is very important to ensure validation is applied to ALL concepts that have been added/changed plus
 * and concepts that have had new vertices added.
 *
 */
public class TxCacheTest extends GraphTestBase{

    @Test
    public void whenNewAddingTypesToTheGraph_EnsureTheConceptLogContainsThem() {
        // add concepts to rootGraph in as many ways as possible
        EntityType t1 = graknGraph.putEntityType("1");
        RelationType t2 = graknGraph.putRelationType("2");
        RoleType t3 = graknGraph.putRoleType("3");
        RuleType t4 = graknGraph.putRuleType("4");
        EntityType t5 = graknGraph.putEntityType("5");

        // verify the concepts that we expected are returned in the set
        assertThat(graknGraph.getTxCache().getModifiedConcepts(), containsInAnyOrder(t1, t2, t3, t4, t5));
    }

    @Test
    public void whenCreatingSuperTypes_EnsureLogContainsSubTypeCastings() {
        RoleType r1 = graknGraph.putRoleType("r1");
        RoleType r2 = graknGraph.putRoleType("r2");
        EntityType t1 = graknGraph.putEntityType("t1").plays(r1).plays(r2);
        EntityType t2 = graknGraph.putEntityType("t2");
        RelationType rt1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);
        Entity i1 = t1.addEntity();
        Entity i2 = t1.addEntity();
        rt1.addRelation().addRolePlayer(r1, i1).addRolePlayer(r2, i2);
        CastingImpl c1 = ((EntityImpl) i1).castings().iterator().next();
        CastingImpl c2 = ((EntityImpl) i2).castings().iterator().next();

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.getTxCache().getModifiedConcepts(), is(empty()));

        t1.superType(t2);
        assertThat(graknGraph.getTxCache().getModifiedConcepts(), containsInAnyOrder(c1, c2));
    }

    @Test
    public void whenCreatingInstances_EnsureLogContainsInstance() {
        EntityType t1 = graknGraph.putEntityType("1");

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.getTxCache().getModifiedConcepts(), is(empty()));

        Entity i1 = t1.addEntity();
        assertThat(graknGraph.getTxCache().getModifiedConcepts(), containsInAnyOrder(i1));
    }

    @Test
    public void whenCreatingRelations_EnsureLogContainsRelationAndCastings(){
        RoleType r1 = graknGraph.putRoleType("r1");
        RoleType r2 = graknGraph.putRoleType("r2");
        EntityType t1 = graknGraph.putEntityType("t1").plays(r1).plays(r2);
        RelationType rt1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);
        Entity i1 = t1.addEntity();
        Entity i2 = t1.addEntity();

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.getTxCache().getModifiedConcepts(), is(empty()));

        Relation rel1 = rt1.addRelation().addRolePlayer(r1, i1).addRolePlayer(r2, i2);
        CastingImpl c1 = ((EntityImpl) i1).castings().iterator().next();
        CastingImpl c2 = ((EntityImpl) i2).castings().iterator().next();
        assertThat(graknGraph.getTxCache().getModifiedConcepts(), containsInAnyOrder(rel1, c1, c2));
    }

    @Test
    public void whenDeletingAnInstanceWithNoRelations_EnsureLogIsEmpty(){
        EntityType t1 = graknGraph.putEntityType("1");
        Entity i1 = t1.addEntity();

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.getTxCache().getModifiedConcepts(), is(empty()));

        i1.delete();
        assertThat(graknGraph.getTxCache().getModifiedConcepts(), is(empty()));
    }

    @Test
    public void whenAddingAndRemovingInstancesFromTypes_EnsureLogTracksNumberOfChanges(){
        EntityType entityType = graknGraph.putEntityType("My Type");
        RelationType relationType = graknGraph.putRelationType("My Relation Type");

        TxCache txCache = graknGraph.getTxCache();
        assertThat(txCache.getShardingCount().keySet(), empty());

        //Add some instances
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        relationType.addRelation();
        assertEquals(2, (long) txCache.getShardingCount().get(entityType.getId()));
        assertEquals(1, (long) txCache.getShardingCount().get(relationType.getId()));

        //Remove an entity
        e1.delete();
        assertEquals(1, (long) txCache.getShardingCount().get(entityType.getId()));
        assertEquals(1, (long) txCache.getShardingCount().get(relationType.getId()));

        //Remove another entity
        e2.delete();
        assertFalse(txCache.getShardingCount().containsKey(entityType.getId()));
        assertEquals(1, (long) txCache.getShardingCount().get(relationType.getId()));
    }

    @Test
    public void whenClosingTransaction_EnsureTransactionCacheIsEmpty(){
        TxCache cache = graknGraph.getTxCache();

        //Load some sample data
        ResourceType<String> resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);
        RoleType roleType1 = graknGraph.putRoleType("role 1");
        RoleType roleType2 = graknGraph.putRoleType("role 2");
        EntityType entityType = graknGraph.putEntityType("My Type").plays(roleType1).plays(roleType2).resource(resourceType);
        RelationType relationType = graknGraph.putRelationType("My Relation Type").relates(roleType1).relates(roleType2);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        Resource<String> r1 = resourceType.putResource("test");

        e1.resource(r1);
        relationType.addRelation().addRolePlayer(roleType1, e1).addRolePlayer(roleType2, e2);

        //Check the caches are not empty
        assertThat(cache.getConceptCache().keySet(), not(empty()));
        assertThat(cache.getModifiedConcepts(), not(empty()));
        assertThat(cache.getModifiedCastings(), not(empty()));
        assertThat(cache.getTypeCache().keySet(), not(empty()));
        assertThat(cache.getLabelCache().keySet(), not(empty()));
        assertThat(cache.getModifiedRelations().keySet(), not(empty()));
        assertThat(cache.getModifiedResources(), not(empty()));
        assertThat(cache.getShardingCount().keySet(), not(empty()));

        //Close the transaction
        graknGraph.commit();

        //Check the caches are empty
        assertThat(cache.getConceptCache().keySet(), empty());
        assertThat(cache.getModifiedConcepts(), empty());
        assertThat(cache.getModifiedCastings(), empty());
        assertThat(cache.getTypeCache().keySet(), empty());
        assertThat(cache.getLabelCache().keySet(), empty());
        assertThat(cache.getModifiedRelations().keySet(), empty());
        assertThat(cache.getModifiedResources(), empty());
        assertThat(cache.getShardingCount().keySet(), empty());
    }

    @Test
    public void whenMutatingSuperTypeOfConceptCreatedInAnotherTransaction_EnsureTransactionBoundConceptIsMutated(){
        EntityType e1 = graknGraph.putEntityType("e1");
        EntityType e2 = graknGraph.putEntityType("e2").superType(e1);
        EntityType e3 = graknGraph.putEntityType("e3");
        graknGraph.commit();

        graknGraph = (AbstractGraknGraph<?>) graknSession.open(GraknTxType.WRITE);
        assertEquals(e1, e2.superType());

        //Mutate Super Type
        e2.superType(e3);
        assertEquals(e3, e2.superType());
        assertEquals(e3, graknGraph.getTxCache().getCachedType(e2.getLabel()).superType());
    }
}
