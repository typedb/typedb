/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.pattern;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.ValuePredicate;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.*;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.VarTraversals;
import io.mindmaps.graql.internal.pattern.property.*;
import io.mindmaps.graql.internal.util.CommonUtil;
import io.mindmaps.graql.internal.util.StringConverter;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.mindmaps.graql.Graql.eq;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.util.ErrorMessage.*;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of Var interface
 */
class VarImpl implements VarInternal {

    private Set<VarProperty> properties = new HashSet<>();

    private String name;
    private final boolean userDefinedName;

    private Optional<VarTraversals> varPattern = Optional.empty();

    /**
     * Create a variable with a random variable name
     */
    VarImpl() {
        this.name = UUID.randomUUID().toString();
        this.userDefinedName = false;
    }

    /**
     * @param name the variable name of the variable
     */
    VarImpl(String name) {
        this.name = name;
        this.userDefinedName = true;
    }

    /**
     * Create a variable by combining a collection of other variables
     * @param vars a collection of variables to combine
     */
    VarImpl(Collection<VarAdmin> vars) {
        VarAdmin first = vars.iterator().next();
        this.name = first.getName();
        this.userDefinedName = first.isUserDefinedName();

        for (VarAdmin var : vars) {
            if (var.isUserDefinedName()) {
                this.name = var.getName();
            }

            var.getProperties().forEach(this::addProperty);
        }
    }

    @Override
    public Var id(String id) {
        return addProperty(new IdProperty(id));
    }

    @Override
    public Var value() {
        return addProperty(new ValueFlagProperty());
    }

    @Override
    public Var value(Object value) {
        return value(eq(value));
    }

    @Override
    public Var value(ValuePredicate predicate) {
        return addProperty(new ValueProperty(predicate.admin()));
    }

    @Override
    public Var has(String type) {
        return has(type, var());
    }

    @Override
    public Var has(String type, Object value) {
        return has(type, eq(value));
    }

    @Override
    public Var has(String type, ValuePredicate predicate) {
        return has(type, var().value(predicate));
    }

    @Override
    public Var has(String type, Var var) {
        return addProperty(new HasResourceProperty(type, var.admin()));
    }

    @Override
    public Var isa(String type) {
        return isa(var().id(type));
    }

    @Override
    public Var isa(Var type) {
        return addProperty(new IsaProperty(type.admin()));
    }

    @Override
    public Var ako(String type) {
        return ako(var().id(type));
    }

    @Override
    public Var ako(Var type) {
        return addProperty(new AkoProperty(type.admin()));
    }

    @Override
    public Var hasRole(String type) {
        return hasRole(var().id(type));
    }

    @Override
    public Var hasRole(Var type) {
        return addProperty(new HasRoleProperty(type.admin()));
    }

    @Override
    public Var playsRole(String type) {
        return playsRole(var().id(type));
    }

    @Override
    public Var playsRole(Var type) {
        return addProperty(new PlaysRoleProperty(type.admin()));
    }

    @Override
    public Var hasScope(Var type) {
        return addProperty(new HasScopeProperty(type.admin()));
    }

    @Override
    public Var hasResource(String type) {
        return hasResource(var().id(type));
    }

    @Override
    public Var hasResource(Var type) {
        return addProperty(new HasResourceTypeProperty(type.admin()));
    }

    @Override
    public Var rel(String roleplayer) {
        return rel(var(roleplayer));
    }

    @Override
    public Var rel(Var roleplayer) {
        putRelationProperty().addCasting(new Casting(roleplayer.admin()), properties);
        return this;
    }

    @Override
    public Var rel(String roletype, String roleplayer) {
        return rel(var().id(roletype), var(roleplayer));
    }

    @Override
    public Var rel(Var roletype, String roleplayer) {
        return rel(roletype, var(roleplayer));
    }

    @Override
    public Var rel(String roletype, Var roleplayer) {
        return rel(var().id(roletype), roleplayer);
    }

    @Override
    public Var rel(Var roletype, Var roleplayer) {
        putRelationProperty().addCasting(new Casting(roletype.admin(), roleplayer.admin()), properties);
        return this;
    }

    @Override
    public Var isAbstract() {
        return addProperty(new IsAbstractProperty());
    }

    @Override
    public Var datatype(ResourceType.DataType<?> datatype) {
        return addProperty(new DataTypeProperty(datatype));
    }

    @Override
    public Var regex(String regex) {
        return addProperty(new RegexProperty(regex));
    }

    @Override
    public Var lhs(String lhs) {
        return addProperty(new LhsProperty(lhs));
    }

    @Override
    public Var rhs(String rhs) {
        return addProperty(new RhsProperty(rhs));
    }

    @Override
    public VarInternal admin() {
        return this;
    }

    @Override
    public Optional<VarAdmin> getType() {
        return getProperty(IsaProperty.class).map(IsaProperty::getType);
    }

    @Override
    public boolean isRelation() {
        return getProperty(RelationProperty.class).isPresent();
    }

    @Override
    public boolean isUserDefinedName() {
        return userDefinedName;
    }

    @Override
    public boolean usesNonEqualPredicate() {
        Stream<ValuePredicateAdmin> predicates = getInnerVars().stream().flatMap(v -> v.getValuePredicates().stream());
        return predicates.anyMatch(id -> !id.equalsValue().isPresent());
    }

    @Override
    public boolean getAbstract() {
        return getProperty(IsAbstractProperty.class).isPresent();
    }

    @Override
    public Optional<ResourceType.DataType<?>> getDatatype() {
        return getProperty(DataTypeProperty.class).map(DataTypeProperty::getDatatype);
    }

    @Override
    public Optional<String> getRegex() {
        return getProperty(RegexProperty.class).map(RegexProperty::getRegex);
    }

    @Override
    public boolean hasValue() {
        return getProperty(ValueFlagProperty.class).isPresent();
    }

    @Override
    public Optional<VarAdmin> getAko() {
        return getProperty(AkoProperty.class).map(AkoProperty::getSuperType);
    }

    @Override
    public Set<VarAdmin> getHasRoles() {
        return getProperties(HasRoleProperty.class).map(HasRoleProperty::getRole).collect(toSet());
    }

    @Override
    public Set<VarAdmin> getPlaysRoles() {
        return getProperties(PlaysRoleProperty.class).map(PlaysRoleProperty::getRole).collect(toSet());
    }

    @Override
    public Set<VarAdmin> getScopes() {
        return getProperties(HasScopeProperty.class).map(HasScopeProperty::getScope).collect(toSet());
    }

    @Override
    public Set<VarAdmin> getHasResourceTypes() {
        return getProperties(HasResourceTypeProperty.class)
                .map(HasResourceTypeProperty::getResourceType).collect(toSet());
    }

    @Override
    public Set<String> getRoleTypes() {
        return getIdNames(getCastings().stream().map(VarAdmin.Casting::getRoleType).flatMap(CommonUtil::optionalToStream));
    }

    @Override
    public Optional<String> getId() {
        return getProperty(IdProperty.class).map(IdProperty::getId);
    }

    @Override
    public Set<String> getResourceTypes() {
        return getProperties(HasResourceProperty.class).map(HasResourceProperty::getType).collect(toSet());
    }

    @Override
    public boolean hasNoProperties() {
        // return true if this variable has any properties set
        return properties.isEmpty();
    }

    @Override
    public Optional<String> getIdOnly() {

        if (getId().isPresent() && properties.size() == 1 && !userDefinedName) {
            return getId();
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        if (!userDefinedName) throw new RuntimeException(SET_GENERATED_VARIABLE_NAME.getMessage(name));
        this.name = name;
    }

    @Override
    public String getPrintableName() {
        if (userDefinedName) {
            return "$" + name;
        } else {
            return getId().map(StringConverter::idToString).orElse("$" + name);
        }
    }

    @Override
    public Set<?> getValueEqualsPredicates() {
        return getValuePredicates().stream()
                .map(ValuePredicateAdmin::equalsValue)
                .flatMap(CommonUtil::optionalToStream)
                .collect(toSet());
    }

    @Override
    public Set<ValuePredicateAdmin> getValuePredicates() {
        return getProperties(ValueProperty.class).map(ValueProperty::getPredicate).collect(toSet());
    }

    @Override
    public Optional<String> getLhs() {
        return getProperty(LhsProperty.class).map(LhsProperty::getLhs);
    }

    @Override
    public Optional<String> getRhs() {
        return getProperty(RhsProperty.class).map(RhsProperty::getRhs);
    }

    @Override
    public Set<VarAdmin> getResources() {
        return getProperties(HasResourceProperty.class).map(HasResourceProperty::getResource).collect(toSet());
    }

    @Override
    public Map<VarAdmin, Set<ValuePredicateAdmin>> getResourcePredicates() {
        // Type of the resource is guaranteed to exist
        //noinspection OptionalGetWithoutIsPresent
        Function<VarAdmin, VarAdmin> type = v -> v.getType().get();

        Function<VarAdmin, Stream<ValuePredicateAdmin>> predicates = resource -> resource.getValuePredicates().stream();

        Map<VarAdmin, List<VarAdmin>> groupedByType =
                getProperties(HasResourceProperty.class).map(HasResourceProperty::getResource).collect(groupingBy(type));

        return Maps.transformValues(groupedByType, vars -> vars.stream().flatMap(predicates).collect(toSet()));
    }

    public Set<VarAdmin.Casting> getCastings() {
        return getProperties(RelationProperty.class).flatMap(RelationProperty::getCastings).collect(toSet());
    }

    @Override
    public Set<MultiTraversal> getMultiTraversals() {
        return getVarTraversals().getTraversals().collect(toSet());
    }

    @Override
    public Stream<VarProperty> getProperties() {
        return properties.stream();
    }

    @Override
    public <T extends VarProperty> Stream<T> getProperties(Class<T> type) {
        return getProperties().filter(type::isInstance).map(type::cast);
    }

    @Override
    public <T extends UniqueVarProperty> Optional<T> getProperty(Class<T> type) {
        return getProperties().filter(type::isInstance).map(type::cast).findAny();
    }

    @Override
    public Set<VarAdmin> getInnerVars() {
        Stack<VarAdmin> newVars = new Stack<>();
        Set<VarAdmin> vars = new HashSet<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarAdmin var = newVars.pop();
            vars.add(var);

            var.getType().ifPresent(newVars::add);
            var.getAko().ifPresent(newVars::add);
            var.getHasRoles().forEach(newVars::add);
            var.getPlaysRoles().forEach(newVars::add);
            var.getScopes().forEach(newVars::add);
            var.getHasResourceTypes().forEach(newVars::add);
            var.getResources().forEach(newVars::add);

            var.getCastings().forEach(casting -> {
                casting.getRoleType().ifPresent(newVars::add);
                newVars.add(casting.getRolePlayer());
            });
        }

        return vars;
    }

    @Override
    public Set<String> getTypeIds() {
        Set<String> results = new HashSet<>();
        getId().ifPresent(results::add);
        getResourceTypes().forEach(results::add);
        getId().ifPresent(results::add);
        return results;
    }

    @Override
    public String toString() {
        Set<VarAdmin> innerVars = getInnerVars();
        innerVars.remove(this);
        innerVars.removeAll(getResources());

        if (!innerVars.stream().allMatch(v -> v.getIdOnly().isPresent() || v.hasNoProperties())) {
            throw new UnsupportedOperationException("Graql strings cannot represent a query with inner variables");
        }

        StringBuilder builder = new StringBuilder();

        String name = isUserDefinedName() ? getPrintableName() + " " : "";

        builder.append(name);

        boolean first = true;

        for (VarProperty property : properties) {
            if (!first) {
                builder.append(" ");
            }
            first = false;
            property.buildString(builder);
        }

        return builder.toString();
    }

    /**
     * @param vars a stream of variables
     * @return the IDs of all variables that refer to things by id in the graph
     */
    private Set<String> getIdNames(Stream<VarAdmin> vars) {
        return vars.map(VarAdmin::getId).flatMap(CommonUtil::optionalToStream).collect(toSet());
    }

    /**
     * @return the VarTraversals object representing this Var as gremlin traversals
     */
    private VarTraversals getVarTraversals() {
        VarTraversals varTraversals = this.varPattern.orElseGet(() -> new VarTraversals(this));
        this.varPattern = Optional.of(varTraversals);
        return varTraversals;
    }

    private RelationProperty putRelationProperty() {
        Optional<RelationProperty> maybeProperty = getProperty(RelationProperty.class);

        return maybeProperty.orElseGet(() -> {
            RelationProperty property = new RelationProperty();
            addProperty(property);
            return property;
        });
    }

    /**
     * Add a non-unique property
     */
    private Var addProperty(VarProperty property) {
        if (property.isUnique()) {
            testUniqueProperty((UniqueVarProperty) property);
        }
        properties.add(property);
        return this;
    }

    /**
     * Fail if there is already an equal property of this type
     */
    private void testUniqueProperty(UniqueVarProperty property) {
        getProperty(property.getClass()).filter(other -> !other.equals(property)).ifPresent(other -> {
            String message = CONFLICTING_PROPERTIES.getMessage(
                    getPrintableName(), property.graqlString(), other.graqlString()
            );
            throw new IllegalStateException(message);
        });
    }

    @Override
    public Disjunction<Conjunction<VarAdmin>> getDisjunctiveNormalForm() {
        // a disjunction containing only one option
        Conjunction<VarAdmin> conjunction = Patterns.conjunction(Collections.singleton(this));
        return Patterns.disjunction(Collections.singleton(conjunction));
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, excludeEqualityFields());
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj, excludeEqualityFields());
    }

    /**
     * Fields to exclude when testing object equality
     */
    private Collection<String> excludeEqualityFields() {
        Collection<String> excludeFields = Sets.newHashSet("varPattern");

        if (!userDefinedName) {
            excludeFields.add("name");
        }

        return excludeFields;
    }

    /**
     * A casting is the pairing of roletype and roleplayer in a relation, where the roletype may be unknown
     */
    public class Casting implements VarAdmin.Casting {
        private final Optional<VarAdmin> roleType;
        private final VarAdmin rolePlayer;

        /**
         * A casting without a role type specified
         * @param rolePlayer the role player of the casting
         */
        Casting(VarAdmin rolePlayer) {
            this.roleType = Optional.empty();
            this.rolePlayer = rolePlayer;
        }

        /**
         * @param roletype the role type of the casting
         * @param rolePlayer the role player of the casting
         */
        Casting(VarAdmin roletype, VarAdmin rolePlayer) {
            this.roleType = Optional.of(roletype);
            this.rolePlayer = rolePlayer;
        }

        @Override
        public Optional<VarAdmin> getRoleType() {
            return roleType;
        }

        @Override
        public VarAdmin getRolePlayer() {
            return rolePlayer;
        }

        @Override
        public String toString() {
            return getRoleType().map(r -> r.getPrintableName() + ": ").orElse("") + getRolePlayer().getPrintableName();
        }

        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }

        @Override
        public boolean equals(Object obj) {
            return EqualsBuilder.reflectionEquals(this, obj);
        }
    }
}
