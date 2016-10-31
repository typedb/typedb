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

package io.grakn.graql.internal.pattern;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import io.grakn.concept.ResourceType;
import io.grakn.graql.ValuePredicate;
import io.grakn.graql.Var;
import io.grakn.graql.admin.Conjunction;
import io.grakn.graql.admin.Disjunction;
import io.grakn.graql.admin.UniqueVarProperty;
import io.grakn.graql.admin.ValuePredicateAdmin;
import io.grakn.graql.admin.VarAdmin;
import io.grakn.graql.admin.VarProperty;
import io.grakn.graql.internal.pattern.property.SubProperty;
import io.grakn.graql.internal.pattern.property.DataTypeProperty;
import io.grakn.graql.internal.pattern.property.HasResourceProperty;
import io.grakn.graql.internal.pattern.property.HasResourceTypeProperty;
import io.grakn.graql.internal.pattern.property.HasRoleProperty;
import io.grakn.graql.internal.pattern.property.HasScopeProperty;
import io.grakn.graql.internal.pattern.property.IdProperty;
import io.grakn.graql.internal.pattern.property.IsAbstractProperty;
import io.grakn.graql.internal.pattern.property.IsaProperty;
import io.grakn.graql.internal.pattern.property.LhsProperty;
import io.grakn.graql.internal.pattern.property.PlaysRoleProperty;
import io.grakn.graql.internal.pattern.property.RegexProperty;
import io.grakn.graql.internal.pattern.property.RelationProperty;
import io.grakn.graql.internal.pattern.property.RhsProperty;
import io.grakn.graql.internal.pattern.property.ValueFlagProperty;
import io.grakn.graql.internal.pattern.property.ValueProperty;
import io.grakn.graql.internal.util.CommonUtil;
import io.grakn.graql.internal.util.StringConverter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.grakn.graql.Graql.eq;
import static io.grakn.graql.Graql.var;
import static io.grakn.graql.internal.util.CommonUtil.toImmutableMultiset;
import static io.grakn.util.ErrorMessage.CONFLICTING_PROPERTIES;
import static io.grakn.util.ErrorMessage.SET_GENERATED_VARIABLE_NAME;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of Var interface
 */
class VarImpl implements VarAdmin {

    private Set<VarProperty> properties = new HashSet<>();

    private String name;
    private final boolean userDefinedName;

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
    public Var sub(String type) {
        return sub(var().id(type));
    }

    @Override
    public Var sub(Var type) {
        return addProperty(new SubProperty(type.admin()));
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
        return addCasting(new Casting(roleplayer.admin()));
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
        return addCasting(new Casting(roletype.admin(), roleplayer.admin()));
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
    public VarAdmin admin() {
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
    public Optional<String> getId() {
        return getProperty(IdProperty.class).map(IdProperty::getId);
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
            return getId().map(StringConverter::idToString).orElse("'" + toString() + "'");
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
            var.getProperties().flatMap(VarProperty::getInnerVars).forEach(newVars::add);
        }

        return vars;
    }

    @Override
    public Set<VarAdmin> getImplicitInnerVars() {
        Stack<VarAdmin> newVars = new Stack<>();
        Set<VarAdmin> vars = new HashSet<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarAdmin var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::getImplicitInnerVars).forEach(newVars::add);
        }

        return vars;
    }

    @Override
    public Set<String> getTypeIds() {
        return getProperties()
                .flatMap(VarProperty::getTypes)
                .map(VarAdmin::getId).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());
    }

    @Override
    public String toString() {
        Set<VarAdmin> innerVars = getInnerVars();
        innerVars.remove(this);
        getProperties(HasResourceProperty.class).map(HasResourceProperty::getResource).forEach(innerVars::remove);

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

    private Var addCasting(VarAdmin.Casting casting) {
        Optional<RelationProperty> relationProperty = getProperty(RelationProperty.class);

        Stream<VarAdmin.Casting> oldCastings = relationProperty
                .map(RelationProperty::getCastings)
                .orElse(Stream.empty());

        ImmutableMultiset<VarAdmin.Casting> castings =
                Stream.concat(oldCastings, Stream.of(casting)).collect(toImmutableMultiset());

        relationProperty.ifPresent(properties::remove);

        properties.add(new RelationProperty(castings));

        return this;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarImpl var = (VarImpl) o;

        if (userDefinedName != var.userDefinedName) return false;
        if (!properties.equals(var.properties)) return false;
        return !userDefinedName || name.equals(var.name);

    }

    @Override
    public int hashCode() {
        int result = properties.hashCode();
        if (userDefinedName) result = 31 * result + name.hashCode();
        result = 31 * result + (userDefinedName ? 1 : 0);
        return result;
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Casting casting = (Casting) o;

            if (!roleType.equals(casting.roleType)) return false;
            return rolePlayer.equals(casting.rolePlayer);

        }

        @Override
        public int hashCode() {
            int result = roleType.hashCode();
            result = 31 * result + rolePlayer.hashCode();
            return result;
        }
    }
}
