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

package ai.grakn.concept;

import ai.grakn.exception.GraphOperationException;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.CheckReturnValue;


/**
 * <p>
 *     The base concept implementation.
 * </p>
 *
 * <p>
 *     A concept which can represent anything in the graph which wraps a tinkerpop {@link Vertex}.
 *     This class forms the basis of assuring the graph follows the Grakn object model.
 *     It provides methods to retrieve information about the Concept, and determine if it is a {@link Type}
 *     ({@link EntityType}, {@link Role}, {@link RelationshipType}, {@link RuleType} or {@link AttributeType})
 *     or an {@link Thing} ({@link Entity}, {@link Relationship} , {@link Attribute}, {@link Rule}).
 * </p>
 *
 * @author fppt
 *
 */
public interface Concept extends Comparable<Concept>{
    //------------------------------------- Accessors ----------------------------------
    /**
     * Get the unique ID associated with the Concept.
     *
     * @return A value the concept's unique id.
     */
    @CheckReturnValue
    ConceptId getId();

    //------------------------------------- Other ---------------------------------
    /**
     * Return as a {@link SchemaConcept} if the {@link Concept} is a {@link SchemaConcept}.
     *
     * @return A {@link SchemaConcept} if the {@link Concept} is a {@link SchemaConcept}
     */
    @CheckReturnValue
    default SchemaConcept asSchemaConcept(){
        throw GraphOperationException.invalidCasting(this, SchemaConcept.class);
    }

    /**
     * Return as a {@link Type} if the {@link Concept} is a {@link Type}.
     *
     * @return A {@link Type} if the {@link Concept} is a {@link Type}
     */
    @CheckReturnValue
    default Type asType(){
        throw GraphOperationException.invalidCasting(this, Type.class);
    }

    /**
     * Return as an {@link Thing} if the {@link Concept} is an {@link Thing}.
     *
     * @return An {@link Thing} if the {@link Concept} is an {@link Thing}
     */
    @CheckReturnValue
    default Thing asThing(){
        throw GraphOperationException.invalidCasting(this, Thing.class);
    }

    /**
     * Return as an {@link EntityType} if the {@link Concept} is an {@link EntityType}.
     *
     * @return A {@link EntityType} if the {@link Concept} is an {@link EntityType}
     */
    @CheckReturnValue
    default EntityType asEntityType(){
        throw GraphOperationException.invalidCasting(this, EntityType.class);
    }

    /**
     * Return as a {@link Role} if the {@link Concept} is a {@link Role}.
     *
     * @return A {@link Role} if the {@link Concept} is a {@link Role}
     */
    @CheckReturnValue
    default Role asRole(){
        throw GraphOperationException.invalidCasting(this, Role.class);
    }

    /**
     * Return as a {@link RelationshipType} if the {@link Concept} is a {@link RelationshipType}.
     *
     * @return A {@link RelationshipType} if the {@link Concept} is a {@link RelationshipType}
     */
    @CheckReturnValue
    default RelationshipType asRelationshipType(){
        throw GraphOperationException.invalidCasting(this, RelationshipType.class);
    }

    /**
     * Return as a {@link RelationshipType} if the {@link Concept} is a {@link RelationshipType}
     *
     * @return A {@link RelationshipType} if the {@link Concept} is a {@link RelationshipType}
     */
    @CheckReturnValue
    default <D> AttributeType<D> asAttributeType(){
        throw GraphOperationException.invalidCasting(this, AttributeType.class);
    }

    /**
     * Return as a {@link RuleType} if the {@link Concept} is a {@link RuleType}.
     *
     * @return A {@link RuleType} if the {@link Concept} is a {@link RuleType}
     */
    @CheckReturnValue
    default RuleType asRuleType(){
        throw GraphOperationException.invalidCasting(this, RuleType.class);
    }

    /**
     * Return as an {@link Entity}, if the {@link Concept} is an {@link Entity} {@link Thing}.
     * @return An {@link Entity} if the {@link Concept} is a {@link Thing}
     */
    @CheckReturnValue
    default Entity asEntity(){
        throw GraphOperationException.invalidCasting(this, Entity.class);
    }

    /**
     * Return as a {@link Relationship} if the {@link Concept} is a {@link Relationship} {@link Thing}.
     *
     * @return A {@link Relationship}  if the {@link Concept} is a {@link Relationship}
     */
    @CheckReturnValue
    default Relationship asRelationship(){
        throw GraphOperationException.invalidCasting(this, Relationship.class);
    }

    /**
     * Return as a {@link Attribute}  if the {@link Concept} is a {@link Attribute} {@link Thing}.
     *
     * @return A {@link Attribute} if the {@link Concept} is a {@link Attribute}
     */
    @CheckReturnValue
    default <D> Attribute<D> asAttribute(){
        throw GraphOperationException.invalidCasting(this, Attribute.class);
    }

    /**
     * Return as a {@link Rule} if the {@link Concept} is a {@link Rule} {@link Thing}.
     *
     * @return A {@link Rule} if the {@link Concept} is a {@link Rule}
     */
    @CheckReturnValue
    default Rule asRule(){
        throw GraphOperationException.invalidCasting(this, Rule.class);
    }

    /**
     * Determine if the {@link Concept} is a {@link SchemaConcept}
     *
     * @return true if the{@link Concept} concept is a {@link SchemaConcept}
     */
    @CheckReturnValue
    default boolean isSchemaConcept(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Type}.
     *
     * @return true if the{@link Concept} concept is a {@link Type}
     */
    @CheckReturnValue
    default boolean isType(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is an {@link Thing}.
     *
     * @return true if the {@link Concept} is an {@link Thing}
     */
    @CheckReturnValue
    default boolean isThing(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is an {@link EntityType}.
     *
     * @return true if the {@link Concept} is an {@link EntityType}.
     */
    @CheckReturnValue
    default boolean isEntityType(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Role}.
     *
     * @return true if the {@link Concept} is a {@link Role}
     */
    @CheckReturnValue
    default boolean isRole(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link RelationshipType}.
     *
     * @return true if the {@link Concept} is a {@link RelationshipType}
     */
    @CheckReturnValue
    default boolean isRelationshipType(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link AttributeType}.
     *
     * @return true if the{@link Concept} concept is a {@link AttributeType}
     */
    @CheckReturnValue
    default boolean isAttributeType(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link RuleType}.
     *
     * @return true if the {@link Concept} is a {@link RuleType}
     */
    @CheckReturnValue
    default boolean isRuleType(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is an {@link Entity}.
     *
     * @return true if the {@link Concept} is a {@link Entity}
     */
    @CheckReturnValue
    default boolean isEntity(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Relationship}.
     *
     * @return true if the {@link Concept} is a {@link Relationship}
     */
    @CheckReturnValue
    default boolean isRelationship(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Attribute}.
     *
     * @return true if the {@link Concept} is a {@link Attribute}
     */
    @CheckReturnValue
    default boolean isAttribute(){
        return false;
    }

    /**
     * Determine if the {@link Concept} is a {@link Rule}.
     *
     * @return true if the {@link Concept} is a {@link Rule}
     */
    @CheckReturnValue
    default boolean isRule(){
        return false;
    }

    /**
     * Delete the Concept.
     *
     * @throws GraphOperationException Throws an exception if this is a type with incoming concepts.
     */
    void delete() throws GraphOperationException;
}