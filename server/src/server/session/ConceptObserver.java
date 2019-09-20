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

import grakn.core.concept.Label;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.session.cache.RuleCache;
import grakn.core.server.session.cache.TransactionCache;
import grakn.core.server.statistics.UncomittedStatisticsDelta;

public class ConceptObserver {

    private TransactionCache transactionCache;
    private MultilevelSemanticCache queryCache;
    private RuleCache ruleCache;
    private UncomittedStatisticsDelta statistics;

    public void setTransactionCache(TransactionCache transactionCache) {
        this.transactionCache = transactionCache;
    }

    public void  setQueryCache(MultilevelSemanticCache queryCache) {
        this.queryCache = queryCache;
    }

    public void setRuleCache(RuleCache ruleCache) {
        this.ruleCache = ruleCache;
    }

    public void setStatisticsDelta(UncomittedStatisticsDelta statistics) {
        this.statistics = statistics;
    }

    @Deprecated
    public TransactionCache transactionCache() {
        return transactionCache;
    }

    @Deprecated
    public MultilevelSemanticCache queryCache() {
        return queryCache;
    }

    @Deprecated
    public RuleCache ruleCache() {
        return ruleCache;
    }

    @Deprecated
    public UncomittedStatisticsDelta statistics() {
        return statistics;
    }


    public void deleteThing(Thing thing) {

    }

    public void deleteSchemaConcept(SchemaConcept schemaConcept) {

    }

    /**
     * Sync the transaction caches to reflect the new concept that has been created
     *
     * @param thing new instance that was created
     * @param isInferred - flag that telling if that instance is inferred, saves a slow
     *                   read from the vertex properties
     */
    private void createThing(Thing thing, boolean isInferred) {
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
    }

    public <D> void createAttribute(Attribute<D> attribute, D value, boolean isInferred) {
        Type type = attribute.type();
        //Track the attribute by index
        String index = Schema.generateAttributeIndex(type.label(), value.toString());
        transactionCache.addNewAttribute(type.label(), index, attribute.id());
        createThing(attribute, isInferred);
    }

    public void createRelation(Relation relation, boolean isInferred) {
        transactionCache.addNewRelation(relation);
        createThing(relation, isInferred);
    }

    public void createEntity(Entity entity, boolean isInferred) {
        createThing(entity, isInferred);
    }

    public void createHasAttributeRelation(Relation hasAttributeRelation, boolean isInferred) {
        createThing(hasAttributeRelation, isInferred);
    }

    public void createSchemaConcept(SchemaConcept schemaConcept) {

    }

    public void conceptSetAbstract(Type type, boolean isAbstract) {
        if (isAbstract) {
            transactionCache.removeFromValidation(type);
        } else {
            transactionCache.trackForValidation(type);
        }
    }


    public void trackRolePlayerForValidation(Casting rolePlayer) {
        transactionCache.trackForValidation(rolePlayer);
    }
}
