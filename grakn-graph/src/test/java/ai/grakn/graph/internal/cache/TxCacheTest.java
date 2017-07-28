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

package ai.grakn.graph.internal.cache;

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.graph.internal.concept.RelationImpl;
import ai.grakn.graph.internal.structure.Casting;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
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
public class TxCacheTest extends GraphTestBase {

    @Test
    public void whenNewAddingTypesToTheGraph_EnsureTheConceptLogContainsThem() {
        // add concepts to rootGraph in as many ways as possible
        EntityType t1 = graknGraph.putEntityType("1");
        RelationType t2 = graknGraph.putRelationType("2");
        Role t3 = graknGraph.putRole("3");
        RuleType t4 = graknGraph.putRuleType("4");
        ResourceType t5 = graknGraph.putResourceType("5", ResourceType.DataType.STRING);

        // verify the concepts that we expected are returned in the set
        assertThat(graknGraph.txCache().getModifiedRoles(), containsInAnyOrder(t3));
        assertThat(graknGraph.txCache().getModifiedRelationTypes(), containsInAnyOrder(t2));
    }

    @Test
    public void whenCreatingRelations_EnsureRolePlayersAreCached(){
        Role r1 = graknGraph.putRole("r1");
        Role r2 = graknGraph.putRole("r2");
        EntityType t1 = graknGraph.putEntityType("t1").plays(r1).plays(r2);
        RelationType rt1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);

        Entity e1 = t1.addEntity();
        Entity e2 = t1.addEntity();

        assertThat(graknGraph.txCache().getModifiedCastings(), empty());

        Set<Casting> castings = ((RelationImpl) rt1.addRelation().
                addRolePlayer(r1, e1).
                addRolePlayer(r2, e2)).reified().get().
                castingsRelation().collect(Collectors.toSet());

        assertTrue(graknGraph.txCache().getModifiedCastings().containsAll(castings));
    }

    @Test
    public void whenCreatingSuperTypes_EnsureLogContainsSubTypeCastings() {
        Role r1 = graknGraph.putRole("r1");
        Role r2 = graknGraph.putRole("r2");
        EntityType t1 = graknGraph.putEntityType("t1").plays(r1).plays(r2);
        EntityType t2 = graknGraph.putEntityType("t2");
        RelationType rt1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);
        Entity i1 = t1.addEntity();
        Entity i2 = t1.addEntity();
        RelationImpl relation = (RelationImpl) rt1.addRelation().addRolePlayer(r1, i1).addRolePlayer(r2, i2);

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.txCache().getModifiedCastings(), is(empty()));

        t1.sup(t2);
        assertTrue(graknGraph.txCache().getModifiedCastings().containsAll(relation.reified().get().castingsRelation().collect(Collectors.toSet())));
    }

    @Test
    public void whenCreatingInstances_EnsureLogContainsInstance() {
        EntityType t1 = graknGraph.putEntityType("1");

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.txCache().getModifiedEntities(), is(empty()));

        Entity i1 = t1.addEntity();
        assertThat(graknGraph.txCache().getModifiedEntities(), containsInAnyOrder(i1));
    }

    @Test
    public void whenCreatingRelations_EnsureLogContainsRelation(){
        Role r1 = graknGraph.putRole("r1");
        Role r2 = graknGraph.putRole("r2");
        EntityType t1 = graknGraph.putEntityType("t1").plays(r1).plays(r2);
        RelationType rt1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);
        Entity i1 = t1.addEntity();
        Entity i2 = t1.addEntity();

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.txCache().getModifiedRelations(), is(empty()));
        Relation rel1 = rt1.addRelation().addRolePlayer(r1, i1).addRolePlayer(r2, i2);
        assertThat(graknGraph.txCache().getModifiedRelations(), containsInAnyOrder(rel1));
    }

    @Test
    public void whenDeletingAnInstanceWithNoRelations_EnsureLogIsEmpty(){
        EntityType t1 = graknGraph.putEntityType("1");
        Entity i1 = t1.addEntity();

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertThat(graknGraph.txCache().getModifiedEntities(), is(empty()));

        i1.delete();
        assertThat(graknGraph.txCache().getModifiedEntities(), is(empty()));
    }

    @Test
    public void whenNoOp_EnsureLogWellFormed() {
        Json expected = Json.read("{\"" + REST.Request.COMMIT_LOG_FIXING +
                "\":{\"" + Schema.BaseType.RESOURCE.name() + "\":{}},\"" +
                REST.Request.COMMIT_LOG_COUNTING + "\":[]}");
        assertEquals("Unexpected graph logs", expected, graknGraph.txCache().getFormattedLog());
    }

    @Test
    public void whenAddedEntities_EnsureLogNotEmpty() {
        EntityType entityType = graknGraph.putEntityType("My Type");
        entityType.addEntity();
        entityType.addEntity();
        Json expected = Json.read("{\"" + REST.Request.COMMIT_LOG_FIXING +
                "\":{\"" + Schema.BaseType.RESOURCE.name() +
                "\":{}},\"" + REST.Request.COMMIT_LOG_COUNTING  +
                "\":[{\"" + REST.Request.COMMIT_LOG_CONCEPT_ID +
                "\":\"" + entityType.getId() + "\",\"" + REST.Request.COMMIT_LOG_SHARDING_COUNT + "\":2}]}");
        assertEquals("Unexpected graph logs", expected, graknGraph.txCache().getFormattedLog());
    }

    @Test
    public void whenAddingAndRemovingInstancesFromTypes_EnsureLogTracksNumberOfChanges(){
        EntityType entityType = graknGraph.putEntityType("My Type");
        RelationType relationType = graknGraph.putRelationType("My Relation Type");

        TxCache txCache = graknGraph.txCache();
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
        TxCache cache = graknGraph.txCache();

        //Load some sample data
        ResourceType<String> resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);
        Role role1 = graknGraph.putRole("role 1");
        Role role2 = graknGraph.putRole("role 2");
        EntityType entityType = graknGraph.putEntityType("My Type").plays(role1).plays(role2).resource(resourceType);
        RelationType relationType = graknGraph.putRelationType("My Relation Type").relates(role1).relates(role2);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        Resource<String> r1 = resourceType.putResource("test");

        e1.resource(r1);
        relationType.addRelation().addRolePlayer(role1, e1).addRolePlayer(role2, e2);

        //Check the caches are not empty
        assertThat(cache.getConceptCache().keySet(), not(empty()));
        assertThat(cache.getOntologyConceptCache().keySet(), not(empty()));
        assertThat(cache.getLabelCache().keySet(), not(empty()));
        assertThat(cache.getRelationIndexCache().keySet(), not(empty()));
        assertThat(cache.getModifiedResources(), not(empty()));
        assertThat(cache.getShardingCount().keySet(), not(empty()));
        assertThat(cache.getModifiedCastings(), not(empty()));

        //Close the transaction
        graknGraph.commit();

        //Check the caches are empty
        assertThat(cache.getConceptCache().keySet(), empty());
        assertThat(cache.getOntologyConceptCache().keySet(), empty());
        assertThat(cache.getLabelCache().keySet(), empty());
        assertThat(cache.getRelationIndexCache().keySet(), empty());
        assertThat(cache.getShardingCount().keySet(), empty());
        assertThat(cache.getModifiedEntities(), empty());
        assertThat(cache.getModifiedRoles(), empty());
        assertThat(cache.getModifiedRelationTypes(), empty());
        assertThat(cache.getModifiedRelations(), empty());
        assertThat(cache.getModifiedRules(), empty());
        assertThat(cache.getModifiedResources(), empty());
        assertThat(cache.getModifiedCastings(), empty());
    }

    @Test
    public void whenMutatingSuperTypeOfConceptCreatedInAnotherTransaction_EnsureTransactionBoundConceptIsMutated(){
        EntityType e1 = graknGraph.putEntityType("e1");
        EntityType e2 = graknGraph.putEntityType("e2").sup(e1);
        EntityType e3 = graknGraph.putEntityType("e3");
        graknGraph.commit();

        //Check everything is okay
        graknGraph = (AbstractGraknGraph<?>) graknSession.open(GraknTxType.WRITE);
        assertTxBoundConceptMatches(e2, Type::sup, is(e1));

        //Mutate Super Type
        e2.sup(e3);
        assertTxBoundConceptMatches(e2, Type::sup, is(e3));
    }

    @Test
    public void whenMutatingRoleTypesOfTypeCreatedInAnotherTransaction_EnsureTransactionBoundConceptsAreMutated(){
        Role rol1 = graknGraph.putRole("role1");
        Role rol2 = graknGraph.putRole("role2");
        EntityType e1 = graknGraph.putEntityType("e1").plays(rol1).plays(rol2);
        EntityType e2 = graknGraph.putEntityType("e2");
        RelationType rel = graknGraph.putRelationType("rel").relates(rol1).relates(rol2);
        graknGraph.commit();

        //Check concepts match what is in transaction cache
        graknGraph = (AbstractGraknGraph<?>) graknSession.open(GraknTxType.WRITE);
        assertTxBoundConceptMatches(e1, Type::plays, containsInAnyOrder(rol1, rol2));
        assertTxBoundConceptMatches(rel, RelationType::relates, containsInAnyOrder(rol1, rol2));
        assertTxBoundConceptMatches(rol1, Role::playedByTypes, containsInAnyOrder(e1));
        assertTxBoundConceptMatches(rol2, Role::playedByTypes, containsInAnyOrder(e1));
        assertTxBoundConceptMatches(rol1, Role::relationTypes, containsInAnyOrder(rel));
        assertTxBoundConceptMatches(rol2, Role::relationTypes, containsInAnyOrder(rel));

        //Role Type 1 and 2 played by e2 now
        e2.plays(rol1);
        e2.plays(rol2);
        assertTxBoundConceptMatches(rol1, Role::playedByTypes, containsInAnyOrder(e1, e2));
        assertTxBoundConceptMatches(rol2, Role::playedByTypes, containsInAnyOrder(e1, e2));

        //e1 no longer plays role 1
        e1.deletePlays(rol1);
        assertTxBoundConceptMatches(rol1, Role::playedByTypes, containsInAnyOrder(e2));
        assertTxBoundConceptMatches(rol2, Role::playedByTypes, containsInAnyOrder(e1, e2));

        //Role 2 no longer part of relation type
        rel.deleteRelates(rol2);
        assertTxBoundConceptMatches(rol2, Role::relationTypes, empty());
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
    private <T extends OntologyConcept> void assertTxBoundConceptMatches(T type, Function<T, Object> resultSupplier, Matcher expectedMatch){
        assertThat(resultSupplier.apply(type), expectedMatch);
        assertThat(resultSupplier.apply(graknGraph.txCache().getCachedOntologyElement(type.getLabel())), expectedMatch);
    }
}
