/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.server.session;

import grakn.core.concept.Concept;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.RelationEdge;
import grakn.core.server.kb.concept.RelationImpl;
import grakn.core.server.kb.concept.RelationReified;
import grakn.core.server.kb.concept.ThingImpl;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.session.cache.CacheProvider;
import grakn.core.server.session.cache.RuleCache;
import grakn.core.server.session.cache.TransactionCache;
import grakn.core.server.statistics.UncomittedStatisticsDelta;

import java.util.List;
import java.util.function.Supplier;

/**
 * ConceptObserver is notified of creation, deletion, and modification of Concepts so that
 * the caches and statistics may be updated. The caches are also shared with ConceptManager
 * and Transaction.
 *
 * The observer is entirely used to WRITE to the caches and statistics, and not read at all
 */
public class ConceptObserver {

    private TransactionCache transactionCache;
    private MultilevelSemanticCache queryCache;
    private RuleCache ruleCache;
    private UncomittedStatisticsDelta statistics;

    public ConceptObserver(CacheProvider cacheProvider, UncomittedStatisticsDelta statistics) {
        this.transactionCache = cacheProvider.getTransactionCache();
        this.queryCache = cacheProvider.getQueryCache();
        this.ruleCache = cacheProvider.getRuleCache();
        this.statistics = statistics;
    }

    private void conceptDeleted(Concept concept) {
        transactionCache.remove(concept);
    }

    public void thingDeleted(Thing thing) {
        Type type = thing.type();
        statistics.decrement(type.label());
        queryCache.ackDeletion(type);
        conceptDeleted(thing);
    }

    // Using a supplier instead of the concept avoids fetching the wrapping concept
    // when the edge is not inferred, which is probably most of the time
    public void relationEdgeDeleted(RelationEdge edge, Supplier<Concept> wrappingConceptGetter) {
        statistics.decrement(edge.type().label());
        if (edge.isInferred()) {
            Concept wrappingConcept = wrappingConceptGetter.get();
            if (wrappingConcept != null) {
                transactionCache.removeInferredInstance(wrappingConcept.asThing());
            }
        }
    }

    public void schemaConceptDeleted(SchemaConcept schemaConcept) {
        ruleCache.clear();
        conceptDeleted(schemaConcept);
    }

    /**
     * Sync the transaction caches to reflect the new concept that has been created
     *
     * @param thing new instance that was created
     * @param isInferred - flag that telling if that instance is inferred, saves a slow
     *                   read from the vertex properties
     */
    private void thingCreated(Thing thing, boolean isInferred) {
        Type thingType = thing.type();
        ruleCache.ackTypeInstance(thingType);
        statistics.increment(thingType.label());

        if (isInferred) {
            transactionCache.inferredInstance(thing);
        } else {
            //creation of inferred concepts is an integral part of reasoning
            //hence we only acknowledge non-inferred insertions
            queryCache.ackInsertion();
        }

        transactionCache.cacheConcept(thing);

        //This Thing gets tracked for validation only if it has keys which need to be checked.
        if (thingType.keys().findAny().isPresent()) {
            transactionCache.trackForValidation(thing);
        }
    }

    public <D> void attributeCreated(Attribute<D> attribute, D value, boolean isInferred) {
        Type type = attribute.type();
        //Track the attribute by index
        String index = Schema.generateAttributeIndex(type.label(), value.toString());
        transactionCache.addNewAttribute(type.label(), index, attribute.id());
        thingCreated(attribute, isInferred);
    }

    public void relationCreated(Relation relation, boolean isInferred) {
        transactionCache.addNewRelation(relation);
        thingCreated(relation, isInferred);
    }

    public void entityCreated(Entity entity, boolean isInferred) {
        thingCreated(entity, isInferred);
    }

    public void hasAttributeRelationCreated(Relation hasAttributeRelation, boolean isInferred) {
        thingCreated(hasAttributeRelation, isInferred);
    }

    public void ruleCreated(Rule rule) {
        transactionCache.trackForValidation(rule);
    }

    public void roleCreated(Role role) {
        transactionCache.trackForValidation(role);
    }

    public void relationTypeCreated(RelationType relationType) {
        transactionCache.trackForValidation(relationType);
    }

    /*
    TODO this pair of methods might be combinable somehow
     */
    public void labelRemoved(SchemaConcept schemaConcept) {
        transactionCache.remove(schemaConcept);
    }
    public void labelAdded(SchemaConcept schemaConcept) {
        transactionCache.cacheConcept(schemaConcept);
    }

    public void conceptSetAbstract(Type type, boolean isAbstract) {
        if (isAbstract) {
            transactionCache.removeFromValidation(type);
        } else {
            transactionCache.trackForValidation(type);
        }
    }

    public void trackRelationInstancesRolePlayers(RelationType relationType) {
        relationType.instances().forEach(concept -> {
            RelationImpl relation = RelationImpl.from(concept);
            RelationReified reifedRelation = relation.reified();
            if (reifedRelation != null) {
                reifedRelation.castingsRelation().forEach(rolePlayer -> transactionCache.trackForValidation(rolePlayer));
            }
        });
    }

    public void trackEntityInstancesRolesPlayed(EntityType entity) {
        entity.instances().forEach(concept -> ((ThingImpl<?, ?>) concept).castingsInstance().forEach(
                rolePlayer -> transactionCache.trackForValidation(rolePlayer)));
    }

    public  void trackAttributeInstancesRolesPlayed(AttributeType attributeType) {
        attributeType.instances().forEach(concept -> ((ThingImpl<?, ?>) concept).castingsInstance().forEach(
                rolePlayer -> transactionCache.trackForValidation(rolePlayer)));
    }

    public void castingDeleted(Casting casting) {
       transactionCache.deleteCasting(casting);
    }

    public void deleteReifiedOwner(Relation owner) {
        transactionCache.getNewRelations().remove(owner);
        if (owner.isInferred()) {
            transactionCache.removeInferredInstance(owner);
        }
    }

    public void relationRoleUnrelated(RelationType relationType, Role role, List<Casting> conceptsPlayingRole) {
        transactionCache.trackForValidation(relationType);
        transactionCache.trackForValidation(role);
        conceptsPlayingRole.forEach(casting -> transactionCache.trackForValidation(casting));
    }

    public void roleDeleted(Role role) {
        transactionCache.trackForValidation(role);
    }

    public void rolePlayerCreated(Casting casting) {
        transactionCache.trackForValidation(casting);
    }
}
