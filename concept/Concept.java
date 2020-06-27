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

package hypergraph.concept;

import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.thing.Attribute;
import hypergraph.concept.thing.Entity;
import hypergraph.concept.thing.Relation;
import hypergraph.concept.thing.Thing;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.EntityType;
import hypergraph.concept.type.RelationType;
import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.ThingType;
import hypergraph.concept.type.Type;

import static hypergraph.common.exception.Error.ConceptRead.INVALID_CONCEPT_CASTING;

public interface Concept {

    String iid();

    void validate();

    boolean isDeleted();

    default Type asType() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Type.class.getCanonicalName()));
    }

    default ThingType asThingType() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(ThingType.class.getCanonicalName()));
    }

    default EntityType asEntityType() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(EntityType.class.getCanonicalName()));
    }

    default AttributeType asAttributeType() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(AttributeType.class.getCanonicalName()));
    }

    default RelationType asRelationType() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(RelationType.class.getCanonicalName()));
    }

    default RoleType asRoleType() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(RoleType.class.getCanonicalName()));
    }

    default Thing asThing() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Thing.class.getCanonicalName()));
    }

    default Entity asEntity() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Entity.class.getCanonicalName()));
    }

    default Attribute asAttribute() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Attribute.class.getCanonicalName()));
    }

    default Relation asRelation() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Relation.class.getCanonicalName()));
    }
}
