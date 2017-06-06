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
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import mjson.Json;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
        ResourceType t5 = graknGraph.putResourceType("5", ResourceType.DataType.STRING);

        // verify the concepts that we expected are returned in the set
        assertThat(graknGraph.getTxCache().getModifiedRoleTypes(), containsInAnyOrder(t3));
        assertThat(graknGraph.getTxCache().getModifiedRelationTypes(), containsInAnyOrder(t2));
    }

    @Test
    public void whenCreatingRelations_EnsureRolePlayersAreCached(){
        RoleType r1 = graknGraph.putRoleType("r1");
        RoleType r2 = graknGraph.putRoleType("r2");
        EntityType t1 = graknGraph.putEntityType("t1").plays(r1).plays(r2);
        RelationType rt1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);

        Entity e1 = t1.addEntity();
        Entity e2 = t1.addEntity();

        assertThat(graknGraph.getTxCache().getModifiedRolePlayers(), empty());

        Set<RolePlayer> rolePlayers = ((RelationImpl) rt1.addRelation().
                addRolePlayer(r1, e1).
                addRolePlayer(r2, e2)).
                getRolePlayers().collect(Collectors.toSet());

        assertTrue(graknGraph.getTxCache().getModifiedRolePlayers().containsAll(rolePlayers));
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
        RelationImpl relation = (RelationImpl) rt1.addRelation().addRolePlayer(r1, i1).addRolePlayer(r2, i2);

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.getTxCache().getModifiedRolePlayers(), is(empty()));

        t1.superType(t2);
        assertTrue(graknGraph.getTxCache().getModifiedRolePlayers().containsAll(relation.getRolePlayers().collect(Collectors.toSet())));
    }

    @Test
    public void whenCreatingInstances_EnsureLogContainsInstance() {
        EntityType t1 = graknGraph.putEntityType("1");

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.getTxCache().getModifiedEntities(), is(empty()));

        Entity i1 = t1.addEntity();
        assertThat(graknGraph.getTxCache().getModifiedEntities(), containsInAnyOrder(i1));
    }

    @Test
    public void whenCreatingRelations_EnsureLogContainsRelation(){
        RoleType r1 = graknGraph.putRoleType("r1");
        RoleType r2 = graknGraph.putRoleType("r2");
        EntityType t1 = graknGraph.putEntityType("t1").plays(r1).plays(r2);
        RelationType rt1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);
        Entity i1 = t1.addEntity();
        Entity i2 = t1.addEntity();

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.getTxCache().getModifiedRelations(), is(empty()));
        Relation rel1 = rt1.addRelation().addRolePlayer(r1, i1).addRolePlayer(r2, i2);
        assertThat(graknGraph.getTxCache().getModifiedRelations(), containsInAnyOrder(rel1));
    }

    @Test
    public void whenDeletingAnInstanceWithNoRelations_EnsureLogIsEmpty(){
        EntityType t1 = graknGraph.putEntityType("1");
        Entity i1 = t1.addEntity();

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.getTxCache().getModifiedEntities(), is(empty()));

        i1.delete();
        assertThat(graknGraph.getTxCache().getModifiedEntities(), is(empty()));
    }

    @Test
    public void whenNoOp_EnsureLogWellFormed() {
        Json expected = Json.read("{\"concepts-to-fix\":{\"CASTING\":{},\"RESOURCE\":{}},\"types-with-new-counts\":[]}");
        assertEquals("Unexpected graph logs", expected, graknGraph.getTxCache().getFormattedLog());
    }

    @Test
    public void whenAddedEntities_EnsureLogNotEmpty() {
        EntityType entityType = graknGraph.putEntityType("My Type");
        entityType.addEntity();
        entityType.addEntity();
        Json expected = Json.read("{\"concepts-to-fix\":{\"CASTING\":{},\"RESOURCE\":{}},\"types-with-new-counts\":[{\"concept-id\":\"55\",\"sharding-count\":2}]}");
        assertEquals("Unexpected graph logs", expected, graknGraph.getTxCache().getFormattedLog());
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
        assertThat(cache.getModifiedCastings(), not(empty()));
        assertThat(cache.getTypeCache().keySet(), not(empty()));
        assertThat(cache.getLabelCache().keySet(), not(empty()));
        assertThat(cache.getRelationIndexCache().keySet(), not(empty()));
        assertThat(cache.getModifiedResources(), not(empty()));
        assertThat(cache.getShardingCount().keySet(), not(empty()));

        //Close the transaction
        graknGraph.commit();

        //Check the caches are empty
        assertThat(cache.getConceptCache().keySet(), empty());
        assertThat(cache.getTypeCache().keySet(), empty());
        assertThat(cache.getLabelCache().keySet(), empty());
        assertThat(cache.getRelationIndexCache().keySet(), empty());
        assertThat(cache.getShardingCount().keySet(), empty());
        assertThat(cache.getModifiedEntities(), empty());
        assertThat(cache.getModifiedRoleTypes(), empty());
        assertThat(cache.getModifiedCastings(), empty());
        assertThat(cache.getModifiedRelationTypes(), empty());
        assertThat(cache.getModifiedRelations(), empty());
        assertThat(cache.getModifiedRules(), empty());
        assertThat(cache.getModifiedResources(), empty());
    }

    @Test
    public void whenMutatingSuperTypeOfConceptCreatedInAnotherTransaction_EnsureTransactionBoundConceptIsMutated(){
        EntityType e1 = graknGraph.putEntityType("e1");
        EntityType e2 = graknGraph.putEntityType("e2").superType(e1);
        EntityType e3 = graknGraph.putEntityType("e3");
        graknGraph.commit();

        //Check everything is okay
        graknGraph = (AbstractGraknGraph<?>) graknSession.open(GraknTxType.WRITE);
        assertTxBoundConceptMatches(e2, Type::superType, is(e1));

        //Mutate Super Type
        e2.superType(e3);
        assertTxBoundConceptMatches(e2, Type::superType, is(e3));
    }

    @Test
    public void whenMutatingRoleTypesOfTypeCreatedInAnotherTransaction_EnsureTransactionBoundConceptsAreMutated(){
        RoleType rol1 = graknGraph.putRoleType("role1");
        RoleType rol2 = graknGraph.putRoleType("role2");
        EntityType e1 = graknGraph.putEntityType("e1").plays(rol1).plays(rol2);
        EntityType e2 = graknGraph.putEntityType("e2");
        RelationType rel = graknGraph.putRelationType("rel").relates(rol1).relates(rol2);
        graknGraph.commit();

        //Check concepts match what is in transaction cache
        graknGraph = (AbstractGraknGraph<?>) graknSession.open(GraknTxType.WRITE);
        assertTxBoundConceptMatches(e1, Type::plays, containsInAnyOrder(rol1, rol2));
        assertTxBoundConceptMatches(rel, RelationType::relates, containsInAnyOrder(rol1, rol2));
        assertTxBoundConceptMatches(rol1, RoleType::playedByTypes, containsInAnyOrder(e1));
        assertTxBoundConceptMatches(rol2, RoleType::playedByTypes, containsInAnyOrder(e1));
        assertTxBoundConceptMatches(rol1, RoleType::relationTypes, containsInAnyOrder(rel));
        assertTxBoundConceptMatches(rol2, RoleType::relationTypes, containsInAnyOrder(rel));

        //Role Type 1 and 2 played by e2 now
        e2.plays(rol1);
        e2.plays(rol2);
        assertTxBoundConceptMatches(rol1, RoleType::playedByTypes, containsInAnyOrder(e1, e2));
        assertTxBoundConceptMatches(rol2, RoleType::playedByTypes, containsInAnyOrder(e1, e2));

        //e1 no longer plays role 1
        e1.deletePlays(rol1);
        assertTxBoundConceptMatches(rol1, RoleType::playedByTypes, containsInAnyOrder(e2));
        assertTxBoundConceptMatches(rol2, RoleType::playedByTypes, containsInAnyOrder(e1, e2));

        //Role 2 no longer part of relation type
        rel.deleteRelates(rol2);
        assertTxBoundConceptMatches(rol2, RoleType::relationTypes, empty());
        assertTxBoundConceptMatches(rel, RelationType::relates, containsInAnyOrder(rol1));
    }

    /**
     * Helper method which will check that the cache and the provided type have the same expected values.
     *
     * @param type The type to check against as well as retreive from the concept cache
     * @param resultSupplier The result of executing some operation on the type
     * @param expectedMatch The expected result of the above operation
     */
    @SuppressWarnings("unchecked")
    private <T extends Type> void assertTxBoundConceptMatches(T type, Function<T, Object> resultSupplier, Matcher expectedMatch){
        assertThat(resultSupplier.apply(type), expectedMatch);
        assertThat(resultSupplier.apply(graknGraph.getTxCache().getCachedType(type.getLabel())), expectedMatch);
    }
}
