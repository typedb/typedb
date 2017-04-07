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

package ai.grakn.graql;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.admin.VarAdmin;

import javax.annotation.CheckReturnValue;

/**
 * A wildcard variable to refers to a concept in a query.
 * <p>
 * A {@code Var} may be given a variable name, or left as an "anonymous" variable. {@code Graql} provides
 * static methods for constructing {@code Var} objects.
 * <p>
 * The methods on {@code Var} are used to set its properties. A {@code Var} behaves differently depending on the type of
 * query its used in. In a {@code MatchQuery}, a {@code Var} describes the properties any matching concept must have. In
 * an {@code InsertQuery}, it describes the properties that should be set on the inserted concept. In a
 * {@code DeleteQuery}, it describes the properties that should be deleted.
 *
 * @author Felix Chapman
 */
@SuppressWarnings("UnusedReturnValue")
public interface Var extends Pattern {

    /**
     * @param id a ConceptId that this variable's ID must match
     * @return this
     */
    @CheckReturnValue
    Var id(ConceptId id);

    /**
     * @param label a string that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    Var label(String label);

    /**
     * @param label a type label that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    Var label(TypeLabel label);

    /**
     * @param value a value that this variable's value must exactly match
     * @return this
     */
    @CheckReturnValue
    Var val(Object value);

    /**
     * @param predicate a atom this variable's value must match
     * @return this
     */
    @CheckReturnValue
    Var val(ValuePredicate predicate);

    /**
     * the variable must have a resource of the given type with an exact matching value
     *
     * @param type a resource type in the ontology
     * @param value a value of a resource
     * @return this
     */
    @CheckReturnValue
    Var has(String type, Object value);

    /**
     * the variable must have a resource of the given type that matches the given atom
     *
     * @param type a resource type in the ontology
     * @param predicate a atom on the value of a resource
     * @return this
     */
    @CheckReturnValue
    Var has(String type, ValuePredicate predicate);

    /**
     * the variable must have a resource of the given type that matches the given atom
     *
     * @param type a resource type in the ontology
     * @param var a variable representing a resource
     * @return this
     */
    @CheckReturnValue
    Var has(String type, Var var);

    /**
     * the variable must have a resource of the given type that matches the given atom
     *
     * @param type a resource type in the ontology
     * @param var a variable representing a resource
     * @return this
     */
    @CheckReturnValue
    Var has(TypeLabel type, Var var);

    /**
     * @param type a concept type id that the variable must be of this type
     * @return this
     */
    @CheckReturnValue
    Var isa(String type);

    /**
     * @param type a concept type that this variable must be an instance of
     * @return this
     */
    @CheckReturnValue
    Var isa(Var type);

    /**
     * @param type a concept type id that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    Var sub(String type);

    /**
     * @param type a concept type that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    Var sub(Var type);

    /**
     * @param type a role type id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    Var relates(String type);

    /**
     * @param type a role type that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    Var relates(Var type);

    /**
     * @param type a role type id that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    Var plays(String type);

    /**
     * @param type a role type that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    Var plays(Var type);

    /**
     * @param type a scope that this variable must have
     * @return this
     */
    @CheckReturnValue
    Var hasScope(Var type);

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    Var has(String type);

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    Var has(Var type);

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    Var key(String type);

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    Var key(Var type);

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    Var rel(String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    Var rel(Var roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a role type in the ontology
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    Var rel(String roletype, String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a variable representing a roletype
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    Var rel(Var roletype, String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a role type in the ontology
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    Var rel(String roletype, Var roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a variable representing a roletype
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    Var rel(Var roletype, Var roleplayer);

    /**
     * set this concept type variable as abstract, meaning it cannot have direct instances
     * @return this
     */
    @CheckReturnValue
    Var isAbstract();

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    Var datatype(ResourceType.DataType<?> datatype);

    /**
     * Specify the regular expression instances of this resource type must match
     * @param regex the regex to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    Var regex(String regex);

    /**
     * @param lhs the left-hand side of this rule
     * @return this
     */
    @CheckReturnValue
    Var lhs(Pattern lhs);

    /**
     * @param rhs the right-hand side of this rule
     * @return this
     */
    @CheckReturnValue
    Var rhs(Pattern rhs);

    /**
     * Specify that the variable is different to another variable
     * @param varName the variable name that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    Var neq(String varName);

    /**
     * Specify that the variable is different to another variable
     * @param var the variable that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    Var neq(Var var);

    /**
     * @return an Admin class to allow inspection of this Var
     */
    VarAdmin admin();
}
