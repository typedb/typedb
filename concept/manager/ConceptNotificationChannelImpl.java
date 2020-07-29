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
import grakn.core.kb.concept.manager.ConceptListener;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.api.Casting;

import java.util.List;
import java.util.function.Supplier;

public class ConceptNotificationChannelImpl implements ConceptNotificationChannel {

    private ConceptListener conceptListener;

    @Override
    public void subscribe(ConceptListener conceptListener) {
        this.conceptListener = conceptListener;
    }

    @Override
    public void thingDeleted(Thing thing) {
        conceptListener.thingDeleted(thing);
    }

    @Override
    public void schemaConceptDeleted(SchemaConcept schemaConcept) {
        conceptListener.schemaConceptDeleted(schemaConcept);
    }

    @Override
    public void labelRemoved(SchemaConcept schemaConcept) {
        conceptListener.labelRemoved(schemaConcept);
    }

    @Override
    public void labelAdded(SchemaConcept schemaConcept) {
        conceptListener.labelAdded(schemaConcept);
    }

    @Override
    public void conceptSetAbstract(Type type, boolean isAbstract) {
        conceptListener.conceptSetAbstract(type, isAbstract);
    }

    @Override
    public void trackRelationInstancesRolePlayers(RelationType relationType) {
        conceptListener.trackRelationInstancesRolePlayers(relationType);
    }

    @Override
    public void trackEntityInstancesRolesPlayed(EntityType entity) {
        conceptListener.trackEntityInstancesRolesPlayed(entity);
    }

    @Override
    public void trackAttributeInstancesRolesPlayed(AttributeType attributeType) {
        conceptListener.trackAttributeInstancesRolesPlayed(attributeType);
    }

    @Override
    public void castingDeleted(Casting casting) {
        conceptListener.castingDeleted(casting);
    }

    @Override
    public void relationRoleUnrelated(RelationType relationType, Role role, List<Casting> conceptsPlayingRole) {
        conceptListener.relationRoleUnrelated(relationType, role, conceptsPlayingRole);
    }

    @Override
    public <D> void attributeCreated(Attribute<D> attribute, String uniqueIndex, boolean isInferred) {
        conceptListener.attributeCreated(attribute, uniqueIndex, isInferred);
    }

    @Override
    public void relationCreated(Relation relation, boolean isInferred) {
        conceptListener.relationCreated(relation, isInferred);
    }

    @Override
    public void entityCreated(Entity entity, boolean isInferred) {
        conceptListener.entityCreated(entity, isInferred);
    }

    @Override
    public void hasAttributeCreated(Thing owner, Attribute attribute, boolean isInferred) {
        conceptListener.hasAttributeCreated(owner, attribute, isInferred);
    }

    @Override
    public void hasAttributeRemoved(Thing owner, Attribute<?> owned, boolean isInferred) {
        conceptListener.hasAttributeRemoved(owner, owned, isInferred);
    }

    @Override
    public void roleUndefined(Role role) {
        conceptListener.roleUndefined(role);
    }

    @Override
    public void rolePlayerCreated(Casting casting) {
        conceptListener.rolePlayerCreated(casting);
    }

    @Override
    public void ruleCreated(Rule rule) {
        conceptListener.ruleCreated(rule);
    }

    @Override
    public void roleCreated(Role role) {
        conceptListener.roleCreated(role);
    }

    @Override
    public void relationTypeCreated(RelationType relationType) {
        conceptListener.relationTypeCreated(relationType);
    }
}
