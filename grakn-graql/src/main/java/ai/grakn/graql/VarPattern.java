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

package ai.grakn.graql;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.graql.admin.VarPatternAdmin;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

/**
 * A variable together with its properties.
 * <p>
 * A {@link VarPattern} may be given a variable, or use an "anonymous" variable. {@code Graql} provides
 * static methods for constructing {@link VarPattern} objects.
 * <p>
 * The methods on {@link VarPattern} are used to set its properties. A {@link VarPattern} behaves differently depending
 * on the type of query its used in. In a {@link Match}, a {@link VarPattern} describes the properties any matching
 * concept must have. In an {@link InsertQuery}, it describes the properties that should be set on the inserted concept.
 * In a {@link DeleteQuery}, it describes the properties that should be deleted.
 *
 * @author Felix Chapman
 */
@SuppressWarnings("UnusedReturnValue")
public interface VarPattern extends Pattern {

    /**
     * @return an Admin class to allow inspection of this {@link VarPattern}
     */
    @CheckReturnValue
    VarPatternAdmin admin();

    /**
     * @param id a ConceptId that this variable's ID must match
     * @return this
     */
    @CheckReturnValue
    VarPattern id(ConceptId id);

    /**
     * @param label a string that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    VarPattern label(String label);

    /**
     * @param label a type label that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    VarPattern label(Label label);

    /**
     * @param value a value that this variable's value must exactly match
     * @return this
     */
    @CheckReturnValue
    VarPattern val(Object value);

    /**
     * @param predicate a atom this variable's value must match
     * @return this
     */
    @CheckReturnValue
    VarPattern val(ValuePredicate predicate);

    /**
     * the variable must have a resource of the given type with an exact matching value
     *
     * @param type  a resource type in the schema
     * @param value a value of a resource
     * @return this
     */
    @CheckReturnValue
    VarPattern has(String type, Object value);

    /**
     * the variable must have a resource of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param predicate a atom on the value of a resource
     * @return this
     */
    @CheckReturnValue
    VarPattern has(String type, ValuePredicate predicate);

    /**
     * the variable must have an {@link Attribute} of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param attribute a variable pattern representing an {@link Attribute}
     * @return this
     */
    @CheckReturnValue
    VarPattern has(String type, VarPattern attribute);

    /**
     * the variable must have an {@link Attribute} of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param attribute a variable pattern representing an {@link Attribute}
     * @return this
     */
    @CheckReturnValue
    VarPattern has(Label type, VarPattern attribute);

    /**
     * the variable must have an {@link Attribute} of the given type that matches {@code resource}.
     * The {@link Relationship} associating the two must match {@code relation}.
     *
     * @param type         a resource type in the ontology
     * @param attribute    a variable pattern representing an {@link Attribute}
     * @param relationship a variable pattern representing a {@link Relationship}
     * @return this
     */
    @CheckReturnValue
    VarPattern has(Label type, VarPattern attribute, VarPattern relationship);

    /**
     * @param type a concept type id that the variable must be of this type directly or indirectly
     * @return this
     */
    @CheckReturnValue
    VarPattern isa(String type);

    /**
     * @param type a concept type that this variable must be an instance of directly or indirectly
     * @return this
     */
    @CheckReturnValue
    VarPattern isa(VarPattern type);

    /**
     * @param type a concept type id that the variable must be of this type directly
     * @return this
     */
    @CheckReturnValue
    VarPattern isaExplicit(String type);

    /**
     * @param type a concept type that this variable must be an instance of directly
     * @return this
     */
    @CheckReturnValue
    VarPattern isaExplicit(VarPattern type);

    /**
     * @param type a concept type id that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    VarPattern sub(String type);

    /**
     * @param type a concept type that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    VarPattern sub(VarPattern type);

    /**
     * @param type a {@link Role} id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    VarPattern relates(String type);

    /**
     * @param type a {@link Role} that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    VarPattern relates(VarPattern type);

    /**
     * @param roleType a {@link Role} id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    VarPattern relates(String roleType, @Nullable String superRoleType);

    /**
     * @param roleType a {@link Role} that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    VarPattern relates(VarPattern roleType, @Nullable VarPattern superRoleType);

    /**
     * @param type a {@link Role} id that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    VarPattern plays(String type);

    /**
     * @param type a {@link Role} that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    VarPattern plays(VarPattern type);

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    VarPattern has(String type);

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    VarPattern has(VarPattern type);

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    VarPattern key(String type);

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    VarPattern key(VarPattern type);

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    VarPattern rel(String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    VarPattern rel(VarPattern roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given {@link Role}
     *
     * @param role       a {@link Role} in the schema
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    VarPattern rel(String role, String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given {@link Role}
     *
     * @param role       a variable pattern representing a {@link Role}
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    VarPattern rel(VarPattern role, String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given {@link Role}
     *
     * @param role       a {@link Role} in the schema
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    VarPattern rel(String role, VarPattern roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given {@link Role}
     *
     * @param role       a variable pattern representing a {@link Role}
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    VarPattern rel(VarPattern role, VarPattern roleplayer);

    /**
     * set this concept type variable as abstract, meaning it cannot have direct instances
     *
     * @return this
     */
    @CheckReturnValue
    VarPattern isAbstract();

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    VarPattern datatype(AttributeType.DataType<?> datatype);

    /**
     * Specify the regular expression instances of this resource type must match
     *
     * @param regex the regex to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    VarPattern regex(String regex);

    /**
     * @param when the left-hand side of this rule
     * @return this
     */
    @CheckReturnValue
    VarPattern when(Pattern when);

    /**
     * @param then the right-hand side of this rule
     * @return this
     */
    @CheckReturnValue
    VarPattern then(Pattern then);

    /**
     * Specify that the variable is different to another variable
     *
     * @param var the variable that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    VarPattern neq(String var);

    /**
     * Specify that the variable is different to another variable
     *
     * @param varPattern the variable pattern that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    VarPattern neq(VarPattern varPattern);
}
