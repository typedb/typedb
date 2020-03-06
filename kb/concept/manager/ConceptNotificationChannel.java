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

package grakn.core.kb.concept.manager;

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
import grakn.core.kb.concept.structure.Casting;

import java.util.List;
import java.util.function.Supplier;

/**
 * This was created to help resolve the circular dependency between: RuleCache -> ConceptManager -> ConceptObserver --(loop)-->
 *
 * A publish/subscriber channel that is used to notify components of the creation/modification of Concepts
 * Currently, the ConceptListener is the is only listener for this channel and updates its members according to what it listens to
 * TODO: a completely generic publish/subscribe model is mostly used to register subscribers at run time
 * TODO: Having a static line of code showing who is listening is more useful for debugging and reading code
 * TODO: so, we should invert this component to inject listeners, and instead the publisher's channel to also break the same dependency loop
 */
public interface ConceptNotificationChannel {
    void subscribe(ConceptListener conceptListener);

    void thingDeleted(Thing thing);

    // Using a supplier instead of the concept avoids fetching the wrapping concept
    // when the edge is not inferred, which is probably most of the time
    void relationEdgeDeleted(RelationType edgeTypeDeleted, boolean isInferredEdge, Supplier<Concept> wrappingConceptGetter);
    void castingDeleted(Casting casting);
    void deleteReifiedOwner(Relation owner);

    void schemaConceptDeleted(SchemaConcept schemaConcept);
    void roleDeleted(Role role);
    void relationRoleUnrelated(RelationType relationType, Role role, List<Casting> conceptsPlayingRole);

    <D> void attributeCreated(Attribute<D> attribute, D value, boolean isInferred);
    void relationCreated(Relation relation, boolean isInferred);
    void entityCreated(Entity entity, boolean isInferred);
    void hasAttributeRelationCreated(Relation hasAttributeRelation, boolean isInferred);
    void rolePlayerCreated(Casting casting);

    void ruleCreated(Rule rule);
    void roleCreated(Role role);
    void relationTypeCreated(RelationType relationType);

    /*
    TODO this pair of methods might be combinable somehow
    */
    void labelRemoved(SchemaConcept schemaConcept);
    void labelAdded(SchemaConcept schemaConcept);

    void conceptSetAbstract(Type type, boolean isAbstract);

    void trackRelationInstancesRolePlayers(RelationType relationType);

    void trackEntityInstancesRolesPlayed(EntityType entity);

    void trackAttributeInstancesRolesPlayed(AttributeType attributeType);


}
