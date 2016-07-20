package io.mindmaps.graql.api.query;

import io.mindmaps.core.implementation.Data;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.mindmaps.graql.api.query.ValuePredicate.eq;

/**
 * A wildcard variable to refers to a concept in a query.
 * <p>
 * A {@code Var} may be given a variable name, or left as an "anonymous" variable. {@code QueryBuilder} provides
 * static methods for constructing {@code Var} objects.
 * <p>
 * The methods on {@code Var} are used to set its properties. A {@code Var} behaves differently depending on the type of
 * query its used in. In a {@code MatchQuery}, a {@code Var} describes the properties any matching concept must have. In
 * an {@code InsertQuery}, it describes the properties that should be set on the inserted concept. In a
 * {@code DeleteQuery}, it describes the properties that should be deleted.
 */
@SuppressWarnings("UnusedReturnValue")
public interface Var extends Pattern {

    /**
     * @param id a string that this variable's ID must match
     * @return this
     */
    Var id(String id);

    /**
     * this variable must have a value
     * @return this
     */
    Var value();

    /**
     * @param value a value that this variable's value must exactly match
     * @return this
     */
    default Var value(Object value) {
        return value(eq(value));
    }

    /**
     * @param predicate a predicate this variable's value must match
     * @return this
     */
    Var value(ValuePredicate predicate);

    /**
     * @param type a resource type that this variable must have an instance of
     * @return this
     */
    Var has(String type);

    /**
     * the variable must have a resource or name of the given type with an exact matching value
     *
     * @param type a resource type in the ontology
     * @param value a value of a resource
     * @return this
     */
    default Var has(String type, Object value) {
        return has(type, eq(value));
    }

    /**
     * the variable must have a resource or name of the given type that matches the given predicate
     *
     * @param type a resource type in the ontology
     * @param predicate a predicate on the value of a resource
     * @return this
     */
    Var has(String type, ValuePredicate predicate);

    /**
     * @param type a concept type id that the variable must be of this type
     * @return this
     */
    default Var isa(String type) {
        return isa(QueryBuilder.id(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of
     * @return this
     */
    Var isa(Var type);

    /**
     * @param type a concept type id that this variable must be a kind of
     * @return this
     */
    default Var ako(String type) {
        return ako(QueryBuilder.id(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of
     * @return this
     */
    Var ako(Var type);

    /**
     * @param type a role type id that this relation type variable must have
     * @return this
     */
    default Var hasRole(String type) {
        return hasRole(QueryBuilder.id(type));
    }

    /**
     * @param type a role type that this relation type variable must have
     * @return this
     */
    Var hasRole(Var type);

    /**
     * @param type a role type id that this concept type variable must play
     * @return this
     */
    default Var playsRole(String type) {
        return playsRole(QueryBuilder.id(type));
    }

    /**
     * @param type a role type that this concept type variable must play
     * @return this
     */
    Var playsRole(Var type);

    /**
     * @param type a scope that this variable must have
     * @return this
     */
    Var hasScope(Var type);

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    Var hasResource(String type);

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    default Var rel(String roleplayer) {
        return rel(QueryBuilder.var(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    Var rel(Var roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a role type in the ontology
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    default Var rel(String roletype, String roleplayer) {
        return rel(QueryBuilder.id(roletype), QueryBuilder.var(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a variable representing a roletype
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    default Var rel(Var roletype, String roleplayer) {
        return rel(roletype, QueryBuilder.var(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a role type in the ontology
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    default Var rel(String roletype, Var roleplayer) {
        return rel(QueryBuilder.id(roletype), roleplayer);
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a variable representing a roletype
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    Var rel(Var roletype, Var roleplayer);

    /**
     * set this concept type variable as abstract, meaning it cannot have direct instances
     * @return this
     */
    Var isAbstract();

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    Var datatype(Data<?> datatype);

    /**
     * @param lhs the left-hand side of this rule
     * @return this
     */
    Var lhs(String lhs);

    /**
     * @param rhs the right-hand side of this rule
     * @return this
     */
    Var rhs(String rhs);

    /**
     * @return an Admin class to allow inspection of this Var
     */
    Admin admin();

    /**
     * Admin class for inspecting a Var
     */
    interface Admin extends Pattern.Admin, Var {

        @Override
        default boolean isVar() {
            return true;
        }

        @Override
        default Var.Admin asVar() {
            return this;
        }

        /**
         * @return the variable name of this variable
         */
        String getName();

        /**
         * @param name the new variable name of this variable
         */
        void setName(String name);

        /**
         * @return whether the user specified a name for this variable
         */
        boolean isUserDefinedName();

        /**
         * @return the type of this variable, if it has one specified
         */
        Optional<Var.Admin> getType();

        /**
         * @return the ako (supertype) of this type, if it has one specified
         */
        Optional<Var.Admin> getAko();

        /**
         * @return all roles this relation type has
         */
        Set<Var.Admin> getHasRoles();

        /**
         * @return all roles this type can play
         */
        Set<Var.Admin> getPlaysRoles();

        /**
         * @return all scopes this relation has
         */
        Set<Var.Admin> getScopes();

        /**
         * @return all resource types that this type's instances can have
         */
        Set<Var.Admin> getHasResourceTypes();

        /**
         * @return the datatype of this resource type, if one is set
         */
        Optional<Data<?>> getDatatype();

        /**
         * @return whether this variable is an abstract type
         */
        boolean getAbstract();

        /**
         * @return the ID this variable represents, if it represents something with a specific ID
         */
        Optional<String> getId();

        /**
         * @return if this var has only an ID set and no other properties, return that ID, else return nothing
         */
        Optional<String> getIdOnly();

        /**
         * @return all variables that this variable references
         */
        Set<Var.Admin> getInnerVars();

        /**
         * @return all type IDs that this variable refers to
         */
        Set<String> getTypeIds();

        /**
         * @return all role types that this variable refers to
         */
        Set<String> getRoleTypes();

        /**
         * @return whether this variable represents a relation
         */
        boolean isRelation();

        /**
         * @return all resource types that this variable refers to
         */
        Set<String> getResourceTypes();

        /**
         * @return the name of this variable, as it would be referenced in a native Graql query (e.g. '$x', 'movie')
         */
        String getPrintableName();

        /**
         * @return true if this variable has no properties set
         */
        boolean hasNoProperties();

        /**
         * @return whether this variable is specified to have a value
         */
        boolean hasValue();

        /**
         * @return all predicates on the value of this variable
         */
        Set<ValuePredicate.Admin> getValuePredicates();

        /**
         * @return the values that this variable must have
         */
        Set<?> getValueEqualsPredicates();

        /**
         * @return the left-hand side that this rule must have
         */
        Optional<String> getLhs();

        /**
         * @return the right-hand side that this rule must have
         */
        Optional<String> getRhs();

        /**
         * @return all predicates on resources of this variable (where the key is the resource type)
         */
        Map<Var.Admin, Set<ValuePredicate.Admin>> getResourcePredicates();

        /**
         * @return all values of resources on this variable (where the key is the resource type)
         */
        Map<Var.Admin, Set<?>> getResourceEqualsPredicates();

        /**
         * @return whether this variable uses any predicate that is not equality
         */
        boolean usesNonEqualPredicate();

        /**
         * @return all castings described on this relation (that is, pairs of role types and role players)
         */
        Set<Casting> getCastings();

        /**
         * @return the gremlin traversals that describe this variable
         */
        Set<MultiTraversal> getMultiTraversals();
    }

    /**
     * A casting, a pair of role type and role player (where the role type may not be present)
     */
    interface Casting {
        /**
         * @return the role type, if specified
         */
        Optional<Admin> getRoleType();

        /**
         * @return the role player
         */
        Admin getRolePlayer();
    }
}
