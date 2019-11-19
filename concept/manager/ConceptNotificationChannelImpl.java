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
 *
 */

package grakn.core.concept.manager;

import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.manager.ConceptObserver;
import grakn.core.kb.concept.structure.Casting;

import java.util.List;
import java.util.function.Supplier;

public class ConceptNotificationChannelImpl implements ConceptNotificationChannel {

    private ConceptObserver conceptObserver;

    @Override
    public void subscribe(ConceptObserver conceptObserver) {
        this.conceptObserver = conceptObserver;
    }

    @Override
    public void thingDeleted(Thing thing) {
        conceptObserver.thingDeleted(thing);
    }

    @Override
    public void relationEdgeDeleted(RelationType edgeTypeDeleted, boolean isInferredEdge, Supplier<Concept> wrappingConceptGetter) {
        conceptObserver.relationEdgeDeleted(edgeTypeDeleted, isInferredEdge, wrappingConceptGetter);
    }

    @Override
    public void schemaConceptDeleted(SchemaConcept schemaConcept) {
        conceptObserver.schemaConceptDeleted(schemaConcept);
    }

    @Override
    public void labelRemoved(SchemaConcept schemaConcept) {
        conceptObserver.labelRemoved(schemaConcept);
    }

    @Override
    public void labelAdded(SchemaConcept schemaConcept) {
        conceptObserver.labelRemoved(schemaConcept);
    }

    @Override
    public void conceptSetAbstract(Type type, boolean isAbstract) {
        conceptObserver.conceptSetAbstract(type, isAbstract);
    }

    @Override
    public void trackRelationInstancesRolePlayers(RelationType relationType) {
        conceptObserver.trackRelationInstancesRolePlayers(relationType);
    }

    @Override
    public void trackEntityInstancesRolesPlayed(EntityType entity) {
        conceptObserver.trackEntityInstancesRolesPlayed(entity);
    }

    @Override
    public void trackAttributeInstancesRolesPlayed(AttributeType attributeType) {
        conceptObserver.trackAttributeInstancesRolesPlayed(attributeType);
    }

    @Override
    public void castingDeleted(Casting casting) {
        conceptObserver.castingDeleted(casting);
    }

    @Override
    public void deleteReifiedOwner(Relation owner) {
        conceptObserver.deleteReifiedOwner(owner);
    }

    @Override
    public void relationRoleUnrelated(RelationType relationType, Role role, List<Casting> conceptsPlayingRole) {
        conceptObserver.relationRoleUnrelated(relationType, role, conceptsPlayingRole);
    }

    @Override
    public <D> void attributeCreated(Attribute<D> attribute, D value, boolean isInferred) {
        conceptObserver.attributeCreated(attribute, value, isInferred);
    }

    @Override
    public void relationCreated(Relation relation, boolean isInferred) {
        conceptObserver.relationCreated(relation, isInferred);
    }

    @Override
    public void entityCreated(Entity entity, boolean isInferred) {
        conceptObserver.entityCreated(entity, isInferred);
    }

    @Override
    public void hasAttributeRelationCreated(Relation hasAttributeRelation, boolean isInferred) {
        conceptObserver.hasAttributeRelationCreated(hasAttributeRelation, isInferred);
    }

    @Override
    public void roleDeleted(Role role) {
        conceptObserver.roleDeleted(role);
    }

    @Override
    public void rolePlayerCreated(Casting casting) {
        conceptObserver.rolePlayerCreated(casting);
    }

    @Override
    public void ruleCreated(Rule rule) {

    }

    @Override
    public void roleCreated(Role role) {

    }

    @Override
    public void relationTypeCreated(RelationType relationType) {

    }
}
