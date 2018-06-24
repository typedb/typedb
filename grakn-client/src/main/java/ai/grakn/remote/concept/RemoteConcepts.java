/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.remote.Transaction;

/**
 * Static factory methods for {@link RemoteConcept} instances, which communicate with a gRPC server using a
 * {@link Transaction}.
 *
 * @author Felix Chapman
 */
public class RemoteConcepts {

    private RemoteConcepts() {}

    public static <D> Attribute<D> createAttribute(Transaction tx, ConceptId id) {
        return RemoteAttribute.create(tx, id);
    }

    public static <D> AttributeType<D> createAttributeType(Transaction tx, ConceptId id) {
        return RemoteAttributeType.create(tx, id);
    }

    public static Entity createEntity(Transaction tx, ConceptId id) {
        return RemoteEntity.create(tx, id);
    }

    public static EntityType createEntityType(Transaction tx, ConceptId id) {
        return RemoteEntityType.create(tx, id);
    }

    public static Type createMetaType(Transaction tx, ConceptId id) {
        return RemoteMetaType.create(tx, id);
    }

    public static Relationship createRelationship(Transaction tx, ConceptId id) {
        return RemoteRelationship.create(tx, id);
    }

    public static RelationshipType createRelationshipType(Transaction tx, ConceptId id) {
        return RemoteRelationshipType.create(tx, id);
    }

    public static Role createRole(Transaction tx, ConceptId id) {
        return RemoteRole.create(tx, id);
    }

    public static Rule createRule(Transaction tx, ConceptId id) {
        return RemoteRule.create(tx, id);
    }
}
