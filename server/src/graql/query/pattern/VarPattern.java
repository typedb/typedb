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

package grakn.core.graql.query.pattern;

import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.predicate.ValuePredicate;
import grakn.core.graql.admin.RelationPlayer;
import grakn.core.graql.query.pattern.property.UniqueVarProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.IsaExplicitProperty;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsAbstractProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.RelationshipProperty;
import grakn.core.graql.query.pattern.property.SubExplicitProperty;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.ThenProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;
import grakn.core.graql.internal.util.StringConverter;
import grakn.core.common.util.CommonUtil;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * A variable together with its properties in one Graql statement.
 * A VarPattern may be given a variable, or use an "anonymous" variable.
 * Graql provides static methods for constructing VarPattern objects.
 * The methods in VarPattern are used to set its properties. A VarPattern
 * behaves differently depending on the type of query its used in.
 * In a Match clause, a VarPattern describes the properties any matching
 * concept must have. In an InsertQuery, it describes the properties that
 * should be set on the inserted concept. In a DeleteQuery, it describes the
 * properties that should be deleted.
 */
public abstract class VarPattern implements Pattern {

    /**
     * @return the variable name of this variable
     */
    @CheckReturnValue
    public abstract Var var();

    protected abstract Set<VarProperty> properties();

    @Override
    public VarPattern asVarPattern() {
        return this;
    }

    /**
     * @return the name this variable represents, if it represents something with a specific name
     */
    @CheckReturnValue
    public final Optional<grakn.core.graql.concept.Label> getTypeLabel() {
        return getProperty(LabelProperty.class).map(LabelProperty::label);
    }

    /**
     * Get a stream of all properties of a particular type on this variable
     *
     * @param type the class of {@link VarProperty} to return
     * @param <T>  the type of {@link VarProperty} to return
     */
    @CheckReturnValue
    public final <T extends VarProperty> Stream<T> getProperties(Class<T> type) {
        return getProperties().filter(type::isInstance).map(type::cast);
    }

    /**
     * Get a unique property of a particular type on this variable, if it exists
     *
     * @param type the class of {@link VarProperty} to return
     * @param <T>  the type of {@link VarProperty} to return
     */
    @CheckReturnValue
    public final <T extends UniqueVarProperty> Optional<T> getProperty(Class<T> type) {
        return getProperties().filter(type::isInstance).map(type::cast).findAny();
    }

    /**
     * Get whether this {@link VarPattern} has a {@link VarProperty} of the given type
     *
     * @param type the type of the {@link VarProperty}
     * @param <T>  the type of the {@link VarProperty}
     * @return whether this {@link VarPattern} has a {@link VarProperty} of the given type
     */
    @CheckReturnValue
    public final <T extends VarProperty> boolean hasProperty(Class<T> type) {
        return getProperties(type).findAny().isPresent();
    }

    /**
     * @return all variables that this variable references
     */
    @CheckReturnValue
    public final Collection<VarPattern> innerVarPatterns() {
        Stack<VarPattern> newVars = new Stack<>();
        List<VarPattern> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarPattern var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::innerVarPatterns).forEach(newVars::add);
        }

        return vars;
    }

    /**
     * Get all inner variables, including implicit variables such as in a has property
     */
    @CheckReturnValue
    public final Collection<VarPattern> implicitInnerVarPatterns() {
        Stack<VarPattern> newVars = new Stack<>();
        List<VarPattern> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarPattern var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::implicitInnerVarPatterns).forEach(newVars::add);
        }

        return vars;
    }

    /**
     * @return all type names that this variable refers to
     */
    @CheckReturnValue
    public final Set<grakn.core.graql.concept.Label> getTypeLabels() {
        return getProperties()
                .flatMap(VarProperty::getTypes)
                .map(VarPattern::getTypeLabel).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());
    }

    @Override
    public final Disjunction<Conjunction<VarPattern>> getDisjunctiveNormalForm() {
        // a disjunction containing only one option
        Conjunction<VarPattern> conjunction = Graql.and(Collections.singleton(this));
        return Graql.or(Collections.singleton(conjunction));
    }

    @Override
    public final Set<Var> commonVars() {
        return innerVarPatterns().stream()
                .filter(v -> v.var().isUserDefinedName())
                .map(VarPattern::var)
                .collect(toSet());
    }

    /**
     * @param id a ConceptId that this variable's ID must match
     * @return this
     */
    @CheckReturnValue
    public final VarPattern id(ConceptId id) {
        return addProperty(IdProperty.of(id));
    }

    /**
     * @param label a string that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    public final VarPattern label(String label) {
        return label(grakn.core.graql.concept.Label.of(label));
    }

    /**
     * @param label a type label that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    public final VarPattern label(grakn.core.graql.concept.Label label) {
        return addProperty(LabelProperty.of(label));
    }

    /**
     * @param value a value that this variable's value must exactly match
     * @return this
     */
    @CheckReturnValue
    public final VarPattern val(Object value) {
        return val(Graql.eq(value));
    }

    /**
     * @param predicate a atom this variable's value must match
     * @return this
     */
    @CheckReturnValue
    public final VarPattern val(ValuePredicate predicate) {
        return addProperty(ValueProperty.of(predicate));
    }

    /**
     * the variable must have a resource of the given type with an exact matching value
     *
     * @param type  a resource type in the schema
     * @param value a value of a resource
     * @return this
     */
    @CheckReturnValue
    public final VarPattern has(String type, Object value) {
        return has(type, Graql.eq(value));
    }

    /**
     * the variable must have a resource of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param predicate a atom on the value of a resource
     * @return this
     */
    @CheckReturnValue
    public final VarPattern has(String type, ValuePredicate predicate) {
        return has(type, Graql.var().val(predicate));
    }

    /**
     * the variable must have an {@link Attribute} of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param attribute a variable pattern representing an {@link Attribute}
     * @return this
     */
    @CheckReturnValue
    public final VarPattern has(String type, VarPattern attribute) {
        return has(grakn.core.graql.concept.Label.of(type), attribute);
    }

    /**
     * the variable must have an {@link Attribute} of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param attribute a variable pattern representing an {@link Attribute}
     * @return this
     */
    @CheckReturnValue
    public final VarPattern has(grakn.core.graql.concept.Label type, VarPattern attribute) {
        return has(type, attribute, Graql.var());
    }

    /**
     * the variable must have an {@link Attribute} of the given type that matches {@code resource}.
     * The {@link grakn.core.graql.concept.Relationship} associating the two must match {@code relation}.
     *
     * @param type         a resource type in the ontology
     * @param attribute    a variable pattern representing an {@link Attribute}
     * @param relationship a variable pattern representing a {@link grakn.core.graql.concept.Relationship}
     * @return this
     */
    @CheckReturnValue
    public final VarPattern has(grakn.core.graql.concept.Label type, VarPattern attribute, VarPattern relationship) {
        return addProperty(HasAttributeProperty.of(type, attribute, relationship));
    }

    /**
     * @param type a concept type id that the variable must be of this type directly
     * @return this
     */
    @CheckReturnValue
    public final VarPattern isaExplicit(String type) {
        return isaExplicit(Graql.label(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of directly
     * @return this
     */
    @CheckReturnValue
    public final VarPattern isaExplicit(VarPattern type) {
        return addProperty(IsaExplicitProperty.of(type));
    }

    /**
     * @param type a concept type id that the variable must be of this type directly or indirectly
     * @return this
     */
    @CheckReturnValue
    public final VarPattern isa(String type) {
        return isa(Graql.label(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of directly or indirectly
     * @return this
     */
    @CheckReturnValue
    public final VarPattern isa(VarPattern type) {
        return addProperty(IsaProperty.of(type));
    }

    /**
     * @param type a concept type id that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    public final VarPattern sub(String type) {
        return sub(Graql.label(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    public final VarPattern sub(VarPattern type) {
        return addProperty(SubProperty.of(type));
    }

    /**
     * @param type a concept type id that this variable must be a kind of, without looking at parent types
     * @return this
     */
    @CheckReturnValue
    public final VarPattern subExplicit(String type) {
        return subExplicit(Graql.label(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of, without looking at parent type
     * @return this
     */
    @CheckReturnValue
    public final VarPattern subExplicit(VarPattern type) {
        return addProperty(SubExplicitProperty.of(type));
    }

    /**
     * @param type a {@link Role} id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public final VarPattern relates(String type) {
        return relates(type, null);
    }

    /**
     * @param type a {@link Role} that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public final VarPattern relates(VarPattern type) {
        return relates(type, null);
    }

    /**
     * @param roleType a {@link Role} id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public VarPattern relates(String roleType, @javax.annotation.Nullable String superRoleType) {
        return relates(Graql.label(roleType), superRoleType == null ? null : Graql.label(superRoleType));
    }

    /**
     * @param roleType a {@link Role} that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public VarPattern relates(VarPattern roleType, @javax.annotation.Nullable VarPattern superRoleType) {
        return addProperty(RelatesProperty.of(roleType, superRoleType));
    }

    /**
     * @param type a {@link Role} id that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    public final VarPattern plays(String type) {
        return plays(Graql.label(type));
    }

    /**
     * @param type a {@link Role} that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    public final VarPattern plays(VarPattern type) {
        return addProperty(PlaysProperty.of(type, false));
    }

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    public final VarPattern has(String type) {
        return has(Graql.label(type));
    }

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    public final VarPattern has(VarPattern type) {
        return addProperty(HasAttributeTypeProperty.of(type, false));
    }

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    public final VarPattern key(String type) {
        return key(Graql.var().label(type));
    }

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    public final VarPattern key(VarPattern type) {
        return addProperty(HasAttributeTypeProperty.of(type, true));
    }

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final VarPattern rel(String roleplayer) {
        return rel(Graql.var(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final VarPattern rel(VarPattern roleplayer) {
        return addCasting(RelationPlayer.of(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given {@link Role}
     *
     * @param role       a {@link Role} in the schema
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final VarPattern rel(String role, String roleplayer) {
        return rel(Graql.label(role), Graql.var(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given {@link Role}
     *
     * @param role       a variable pattern representing a {@link Role}
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final VarPattern rel(VarPattern role, String roleplayer) {
        return rel(role, Graql.var(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given {@link Role}
     *
     * @param role       a {@link Role} in the schema
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final VarPattern rel(String role, VarPattern roleplayer) {
        return rel(Graql.label(role), roleplayer);
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given {@link Role}
     *
     * @param role       a variable pattern representing a {@link Role}
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final VarPattern rel(VarPattern role, VarPattern roleplayer) {
        return addCasting(RelationPlayer.of(role, roleplayer));
    }

    /**
     * set this concept type variable as abstract, meaning it cannot have direct instances
     *
     * @return this
     */
    @CheckReturnValue
    public final VarPattern isAbstract() {
        return addProperty(IsAbstractProperty.get());
    }

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    public final VarPattern datatype(AttributeType.DataType<?> datatype) {
        return addProperty(DataTypeProperty.of(datatype));
    }

    /**
     * Specify the regular expression instances of this resource type must match
     *
     * @param regex the regex to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    public final VarPattern regex(String regex) {
        return addProperty(RegexProperty.of(regex));
    }

    /**
     * @param when the left-hand side of this rule
     * @return this
     */
    @CheckReturnValue
    public final VarPattern when(Pattern when) {
        return addProperty(WhenProperty.of(when));
    }

    /**
     * @param then the right-hand side of this rule
     * @return this
     */
    @CheckReturnValue
    public final VarPattern then(Pattern then) {
        return addProperty(ThenProperty.of(then));
    }

    /**
     * Specify that the variable is different to another variable
     *
     * @param var the variable that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    public final VarPattern neq(String var) {
        return neq(Graql.var(var));
    }

    /**
     * Specify that the variable is different to another variable
     *
     * @param varPattern the variable pattern that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    public final VarPattern neq(VarPattern varPattern) {
        return addProperty(NeqProperty.of(varPattern));
    }

    /**
     * @return the name of this variable, as it would be referenced in a native Graql query (e.g. '$x', 'movie')
     */
    @CheckReturnValue
    public final String getPrintableName() {
        if (properties().size() == 0) {
            // If there are no properties, we display the variable name
            return var().toString();
        } else if (properties().size() == 1) {
            // If there is only a label, we display that
            Optional<grakn.core.graql.concept.Label> label = getTypeLabel();
            if (label.isPresent()) {
                return StringConverter.typeLabelToString(label.get());
            }
        }

        // Otherwise, we print the entire pattern
        return "`" + toString() + "`";
    }

    /**
     * Get a stream of all properties on this variable
     */
    @CheckReturnValue
    public final Stream<VarProperty> getProperties() {
        return properties().stream();
    }

    private VarPattern addCasting(RelationPlayer relationPlayer) {
        Optional<RelationshipProperty> relationProperty = getProperty(RelationshipProperty.class);

        ImmutableMultiset<RelationPlayer> oldCastings = relationProperty
                .map(RelationshipProperty::relationPlayers)
                .orElse(ImmutableMultiset.of());

        ImmutableMultiset<RelationPlayer> relationPlayers =
                Stream.concat(oldCastings.stream(), Stream.of(relationPlayer)).collect(CommonUtil.toImmutableMultiset());

        RelationshipProperty newProperty = RelationshipProperty.of(relationPlayers);

        return relationProperty.map(this::removeProperty).orElse(this).addProperty(newProperty);
    }

    private VarPattern addProperty(VarProperty property) {
        if (property.isUnique()) {
            testUniqueProperty((UniqueVarProperty) property);
        }
        return new VarPatternImpl(var(), Sets.union(properties(), ImmutableSet.of(property)));
    }

    private VarPattern removeProperty(VarProperty property) {
        return new VarPatternImpl(var(), Sets.difference(properties(), ImmutableSet.of(property)));
    }

    /**
     * Fail if there is already an equal property of this type
     */
    private void testUniqueProperty(UniqueVarProperty property) {
        getProperty(property.getClass()).filter(other -> !other.equals(property)).ifPresent(other -> {
            throw GraqlQueryException.conflictingProperties(this, property, other);
        });
    }
}
