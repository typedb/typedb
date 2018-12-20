/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.concept;

import grakn.core.server.exception.TransactionException;

import javax.annotation.CheckReturnValue;


/**
 * The base concept implementation.
 * A concept which can every object in the graph.
 * This class forms the basis of assuring the graph follows the Grakn object model.
 * It provides methods to retrieve information about the Concept, and determine if it is a {@link Type}
 * ({@link EntityType}, {@link Role}, {@link RelationType}, {@link Rule} or {@link AttributeType})
 * or an {@link Thing} ({@link Entity}, {@link Relation} , {@link Attribute}).
 */
public interface Concept {
    //------------------------------------- Accessors ----------------------------------

    /**
     * Get the unique ID associated with the Concept.
     *
     * @return A value the concept's unique id.
     */
    @CheckReturnValue
    ConceptId id();

    //------------------------------------- Other ---------------------------------

    /**
     * Return as a {@link SchemaConcept} if the {@link Concept} is a {@link SchemaConcept}.
     *
     * @return A {@link SchemaConcept} if the {@link Concept} is a {@link SchemaConcept}
     */
    @CheckReturnValue
    default SchemaConcept asSchemaConcept() {
        throw TransactionException.invalidCasting(this, SchemaConcept.class);
    }

    /**
     * Return as a {@link Type} if the {@link Concept} is a {@link Type}.
     *
     * @return A {@link Type} if the {@link Concept} is a {@link Type}
     */
    @CheckReturnValue
    default Type asType() {
        throw TransactionException.invalidCasting(this, Type.class);
    }

    /**
     * Return as an {@link Thing} if the {@link Concept} is an {@link Thing}.
     *
     * @return An {@link Thing} if the {@link Concept} is an {@link Thing}
     */
    @CheckReturnValue
    default Thing asThing() {
        throw TransactionException.invalidCasting(this, Thing.class);
    }

    /**
     * Return as an {@link EntityType} if the {@link Concept} is an {@link EntityType}.
     *
     * @return A {@link EntityType} if the {@link Concept} is an {@link EntityType}
     */
    @CheckReturnValue
    default EntityType asEntityType() {
        throw TransactionException.invalidCasting(this, EntityType.class);
    }

    /**
     * Return as a {@link Role} if the {@link Concept} is a {@link Role}.
     *
     * @return A {@link Role} if the {@link Concept} is a {@link Role}
     */
    @CheckReturnValue
    default Role asRole() {
        throw TransactionException.invalidCasting(this, Role.class);
    }

    /**
     * Return as a {@link RelationType} if the {@link Concept} is a {@link RelationType}.
     *
     * @return A {@link RelationType} if the {@link Concept} is a {@link RelationType}
     */
    @CheckReturnValue
    default RelationType asRelationshipType() {
        throw TransactionException.invalidCasting(this, RelationType.class);
    }

    /**
     * Return as a {@link AttributeType} if the {@link Concept} is a {@link AttributeType}
     *
     * @return A {@link AttributeType} if the {@link Concept} is a {@link AttributeType}
     */
    @CheckReturnValue
    default <D> AttributeType<D> asAttributeType() {
        throw TransactionException.invalidCasting(this, AttributeType.class);
    }

    /**
     * Return as a {@link Rule} if the {@link Concept} is a {@link Rule}.
     *
     * @return A {@link Rule} if the {@link Concept} is a {@link Rule}
     */
    @CheckReturnValue
    default Rule asRule() {
        throw TransactionException.invalidCasting(this, Rule.class);
    }

    /**
     * Return as an {@link Entity}, if the {@link Concept} is an {@link Entity} {@link Thing}.
     *
     * @return An {@link Entity} if the {@link Concept} is a {@link Thing}
     */
    @CheckReturnValue
    default Entity asEntity() {
        throw TransactionException.invalidCasting(this, Entity.class);
    }

    /**
     * Return as a {@link Relation} if the {@link Concept} is a {@link Relation} {@link Thing}.
     *
     * @return A {@link Relation}  if the {@link Concept} is a {@link Relation}
     */
    @CheckReturnValue
    default Relation asRelation() {
        throw TransactionException.invalidCasting(this, Relation.class);
    }

    /**
     * Return as a {@link Attribute}  if the {@link Concept} is a {@link Attribute} {@link Thing}.
     *
     * @return A {@link Attribute} if the {@link Concept} is a {@link Attribute}
     */
    @CheckReturnValue
    default <D> Attribute<D> asAttribute() {
        throw TransactionException.invalidCasting(this, Attribute.class);
    }

    /**
     * Determine if the {@link Concept} is a {@link SchemaConcept}
     *
     * @return true if the{@link Concept} concept is a {@link SchemaConcept}
     */
    @CheckReturnValue
    default boolean isSchemaConcept() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Type}.
     *
     * @return true if the{@link Concept} concept is a {@link Type}
     */
    @CheckReturnValue
    default boolean isType() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is an {@link Thing}.
     *
     * @return true if the {@link Concept} is an {@link Thing}
     */
    @CheckReturnValue
    default boolean isThing() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is an {@link EntityType}.
     *
     * @return true if the {@link Concept} is an {@link EntityType}.
     */
    @CheckReturnValue
    default boolean isEntityType() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Role}.
     *
     * @return true if the {@link Concept} is a {@link Role}
     */
    @CheckReturnValue
    default boolean isRole() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link RelationType}.
     *
     * @return true if the {@link Concept} is a {@link RelationType}
     */
    @CheckReturnValue
    default boolean isRelationshipType() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link AttributeType}.
     *
     * @return true if the{@link Concept} concept is a {@link AttributeType}
     */
    @CheckReturnValue
    default boolean isAttributeType() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Rule}.
     *
     * @return true if the {@link Concept} is a {@link Rule}
     */
    @CheckReturnValue
    default boolean isRule() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is an {@link Entity}.
     *
     * @return true if the {@link Concept} is a {@link Entity}
     */
    @CheckReturnValue
    default boolean isEntity() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Relation}.
     *
     * @return true if the {@link Concept} is a {@link Relation}
     */
    @CheckReturnValue
    default boolean isRelationship() {
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Attribute}.
     *
     * @return true if the {@link Concept} is a {@link Attribute}
     */
    @CheckReturnValue
    default boolean isAttribute() {
        return false;
    }

    /**
     * Delete the Concepts
     */
    void delete();

    /**
     * Return whether the concept has been deleted.
     */
    boolean isDeleted();
}