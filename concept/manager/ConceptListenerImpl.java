/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept.manager;

import grakn.core.concept.impl.RelationImpl;
import grakn.core.concept.impl.RelationReified;
import grakn.core.concept.impl.ThingImpl;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptListener;
import grakn.core.kb.concept.structure.Casting;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.keyspace.AttributeManager;
import grakn.core.kb.keyspace.StatisticsDelta;
import grakn.core.kb.server.cache.TransactionCache;

import java.util.List;
import java.util.function.Supplier;

/**
 * ConceptObserver is notified of creation, deletion, and modification of Concepts so that
 * the caches and statistics may be updated. The caches are also shared with ConceptManager
 * and Transaction.
 *
 * The observer is entirely used to WRITE to the caches and statistics, and not read at all
 */
public class ConceptListenerImpl implements ConceptListener {

    private final TransactionCache transactionCache;
    private final QueryCache queryCache;
    private final RuleCache ruleCache;
    private final StatisticsDelta statistics;
    private final AttributeManager attributeManager;
    private final String txId;

    public ConceptListenerImpl(TransactionCache transactionCache, QueryCache queryCache, RuleCache ruleCache, StatisticsDelta statistics, AttributeManager attributeManager, String txId) {
        this.transactionCache = transactionCache;
        this.queryCache = queryCache;
        this.ruleCache = ruleCache;
        this.statistics = statistics;
        this.attributeManager = attributeManager;
        this.txId = txId;
    }

    private void conceptDeleted(Concept concept) {
        transactionCache.remove(concept);
    }

    @Override
    public void thingDeleted(Thing thing) {
        Type type = thing.type();
        statistics.decrement(type);
        queryCache.ackDeletion(type);
        conceptDeleted(thing);
        if(thing.isAttribute()) attributeDeleted(thing.asAttribute());
    }

    // Using a supplier instead of the concept avoids fetching the wrapping concept
    // when the edge is not inferred, which is probably most of the time
    @Override
    public void relationEdgeDeleted(RelationType edgeTypeDeleted, boolean isInferredEdge, Supplier<Concept> wrappingConceptGetter) {
        statistics.decrement(edgeTypeDeleted);
        if (isInferredEdge) {
            Concept wrappingConcept = wrappingConceptGetter.get();
            if (wrappingConcept != null) {
                transactionCache.removeInferredInstance(wrappingConcept.asThing());
            }
        }
    }

    @Override
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
        ruleCache.ackTypeInstanceInsertion(thingType);
        statistics.increment(thingType);

        if (isInferred) {
            transactionCache.inferredInstance(thing);
        } else {
            //creation of inferred concepts is an integral part of reasoning
            //hence we only acknowledge non-inferred insertions
            queryCache.ackInsertion();
        }

        transactionCache.cacheConcept(thing);

        //This Thing gets tracked for validation only if it has keys which need to be checked
        if (thingType.keys().findAny().isPresent()) {
            transactionCache.trackForValidation(thing);
        }

        //acknowledge key relation modification if the thing is one
        if (thingType.isImplicit() && Schema.ImplicitType.isKey(thingType.label())){
            thing.asRelation().rolePlayers()
                    .filter(Concept::isAttribute)
                    .map(Concept::asAttribute)
                    .forEach(key -> {
                        Label label = Schema.ImplicitType.explicitLabel(thingType.label());
                        String index = Schema.generateAttributeIndex(label, key.value().toString());
                        transactionCache.addModifiedKeyIndex(index);
                    });
        }
    }

    @Override
    public <D> void attributeCreated(Attribute<D> attribute, D value, boolean isInferred) {
        Type type = attribute.type();
        //Track the attribute by index
        Label label = type.label();
        String index = Schema.generateAttributeIndex(label, value.toString());
        transactionCache.addNewAttribute(label, index, attribute.id());
        thingCreated(attribute, isInferred);
        attributeManager.ackAttributeInsert(index, txId);
    }

    private <D> void attributeDeleted(Attribute<D> attribute) {
        Type type = attribute.type();
        //Track the attribute by index
        String index = Schema.generateAttributeIndex(type.label(), attribute.value().toString());
        attributeManager.ackAttributeDelete(index, txId);
    }

    @Override
    public void relationCreated(Relation relation, boolean isInferred) {
        transactionCache.addNewRelation(relation);
        thingCreated(relation, isInferred);
    }

    @Override
    public void entityCreated(Entity entity, boolean isInferred) {
        thingCreated(entity, isInferred);
    }

    @Override
    public void hasAttributeRelationCreated(Relation hasAttributeRelation, boolean isInferred) {
        thingCreated(hasAttributeRelation, isInferred);
    }

    @Override
    public void ruleCreated(Rule rule) {
        transactionCache.trackForValidation(rule);
    }

    @Override
    public void roleCreated(Role role) {
        transactionCache.trackForValidation(role);
    }

    @Override
    public void relationTypeCreated(RelationType relationType) {
        transactionCache.trackForValidation(relationType);
    }

    /*
    TODO this pair of methods might be combinable somehow
     */
    @Override
    public void labelRemoved(SchemaConcept schemaConcept) {
        transactionCache.remove(schemaConcept);
    }
    @Override
    public void labelAdded(SchemaConcept schemaConcept) {
        transactionCache.cacheConcept(schemaConcept);
    }

    @Override
    public void conceptSetAbstract(Type type, boolean isAbstract) {
        if (isAbstract) {
            transactionCache.removeFromValidation(type);
        } else {
            transactionCache.trackForValidation(type);
        }
    }

    @Override
    public void trackRelationInstancesRolePlayers(RelationType relationType) {
        relationType.instances().forEach(concept -> {
            RelationImpl relation = RelationImpl.from(concept);
            RelationReified reifedRelation = relation.reified();
            if (reifedRelation != null) {
                reifedRelation.castingsRelation().forEach(rolePlayer -> transactionCache.trackForValidation(rolePlayer));
            }
        });
    }

    @Override
    public void trackEntityInstancesRolesPlayed(EntityType entity) {
        entity.instances().forEach(concept -> ((ThingImpl<?, ?>) concept).castingsInstance().forEach(
                rolePlayer -> transactionCache.trackForValidation(rolePlayer)));
    }

    @Override
    public void trackAttributeInstancesRolesPlayed(AttributeType attributeType) {
        attributeType.instances().forEach(concept -> ((ThingImpl<?, ?>) concept).castingsInstance().forEach(
                rolePlayer -> transactionCache.trackForValidation(rolePlayer)));
    }

    @Override
    public void castingDeleted(Casting casting) {
       transactionCache.deleteCasting(casting);
    }

    @Override
    public void deleteReifiedOwner(Relation owner) {
        transactionCache.getNewRelations().remove(owner);
        if (owner.isInferred()) {
            transactionCache.removeInferredInstance(owner);
        }
    }

    @Override
    public void relationRoleUnrelated(RelationType relationType, Role role, List<Casting> conceptsPlayingRole) {
        transactionCache.trackForValidation(relationType);
        transactionCache.trackForValidation(role);
        conceptsPlayingRole.forEach(casting -> transactionCache.trackForValidation(casting));
    }

    @Override
    public void roleDeleted(Role role) {
        transactionCache.trackForValidation(role);
    }

    @Override
    public void rolePlayerCreated(Casting casting) {
        transactionCache.trackForValidation(casting);
    }
}
