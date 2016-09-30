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
import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.ValuePredicate;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.*;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.VarTraversals;
import io.mindmaps.graql.internal.pattern.property.*;
import io.mindmaps.graql.internal.util.CommonUtil;
import io.mindmaps.graql.internal.util.StringConverter;

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

            if (var.hasValue()) value();
            if (var.getAbstract()) isAbstract();

            var.getDatatype().ifPresent(this::datatype);
            var.getRegex().ifPresent(this::regex);
            var.getType().ifPresent(this::isa);
            var.getAko().ifPresent(this::ako);

            var.getId().ifPresent(this::id);
            var.getLhs().ifPresent(this::lhs);
            var.getRhs().ifPresent(this::rhs);
            var.getValuePredicates().forEach(this::value);

            var.getHasRoles().forEach(this::hasRole);
            var.getPlaysRoles().forEach(this::playsRole);
            var.getScopes().forEach(this::hasScope);

            // Currently it is guaranteed that resource types are specified with an ID
            //noinspection OptionalGetWithoutIsPresent
            ((VarInternal) var).getProperties(HasResourceProperty.class).forEach(properties::add);

            if (var.isRelation()) {
                RelationProperty relationProperty = putRelationProperty();
                var.getCastings().forEach(relationProperty::addCasting);
            }
        }
    }

    @Override
    public Var id(String id) {
        getId().ifPresent(
                prevId -> {
                    if (!prevId.equals(id)) throw new IllegalStateException(MULTIPLE_IDS.getMessage(id, prevId));
                }
        );
        properties.add(new IdProperty(id));
        return this;
    }

    @Override
    public Var value() {
        properties.add(new ValueFlagProperty());
        return this;
    }

    @Override
    public Var value(Object value) {
        return value(eq(value));
    }

    @Override
    public Var value(ValuePredicate predicate) {
        properties.add(new ValueProperty(predicate.admin()));
        return this;
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
        properties.add(new HasResourceProperty(type, var.admin()));
        return this;
    }

    @Override
    public Var isa(String type) {
        return isa(var().id(type));
    }

    @Override
    public Var isa(Var type) {
        VarAdmin var = type.admin();

        getType().ifPresent(
                other -> {
                    if (!var.getName().equals(other.getName()) && !var.getIdOnly().equals(other.getIdOnly())) {
                        throw new IllegalStateException(
                                MULTIPLE_TYPES.getMessage(
                                        getPrintableName(), var.getPrintableName(), other.getPrintableName()
                                )
                        );
                    }
                }
        );
        properties.add(new IsaProperty(var));
        return this;
    }

    @Override
    public Var ako(String type) {
        return ako(var().id(type));
    }

    @Override
    public Var ako(Var type) {
        properties.add(new AkoProperty(type.admin()));
        return this;
    }

    @Override
    public Var hasRole(String type) {
        return hasRole(var().id(type));
    }

    @Override
    public Var hasRole(Var type) {
        properties.add(new HasRoleProperty(type.admin()));
        return this;
    }

    @Override
    public Var playsRole(String type) {
        return playsRole(var().id(type));
    }

    @Override
    public Var playsRole(Var type) {
        properties.add(new PlaysRoleProperty(type.admin()));
        return this;
    }

    @Override
    public Var hasScope(Var type) {
        properties.add(new HasScopeProperty(type.admin()));
        return this;
    }

    @Override
    public Var hasResource(String type) {
        return hasResource(var().id(type));
    }

    @Override
    public Var hasResource(Var type) {
        properties.add(new HasResourceTypeProperty(type.admin()));
        return this;
    }

    @Override
    public Var rel(String roleplayer) {
        return rel(var(roleplayer));
    }

    @Override
    public Var rel(Var roleplayer) {
        putRelationProperty().addCasting(new Casting(roleplayer.admin()));
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
        putRelationProperty().addCasting(new Casting(roletype.admin(), roleplayer.admin()));
        return this;
    }

    @Override
    public Var isAbstract() {
        properties.add(new IsAbstractProperty());
        return this;
    }

    @Override
    public Var datatype(ResourceType.DataType<?> datatype) {
        properties.add(new DataTypeProperty(datatype));
        return this;
    }

    @Override
    public Var regex(String regex) {
        properties.add(new RegexProperty(regex));
        return this;
    }

    @Override
    public Var lhs(String lhs) {
        properties.add(new LhsProperty(lhs));
        return this;
    }

    @Override
    public Var rhs(String rhs) {
        properties.add(new RhsProperty(rhs));
        return this;
    }

    @Override
    public VarInternal admin() {
        return this;
    }


    @Override
    public Optional<VarAdmin> getType() {
        return getProperties(IsaProperty.class).findAny().map(IsaProperty::getType);
    }

    @Override
    public boolean isRelation() {
        return getProperties(RelationProperty.class).findAny().isPresent();
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
        return getProperties(IsAbstractProperty.class).findAny().isPresent();
    }

    @Override
    public Optional<ResourceType.DataType<?>> getDatatype() {
        return getProperties(DataTypeProperty.class).findAny().map(DataTypeProperty::getDatatype);
    }

    @Override
    public Optional<String> getRegex() {
        return getProperties(RegexProperty.class).findAny().map(RegexProperty::getRegex);
    }

    @Override
    public boolean hasValue() {
        return getProperties(ValueFlagProperty.class).findAny().isPresent();
    }

    @Override
    public Optional<VarAdmin> getAko() {
        return getProperties(AkoProperty.class).findAny().map(AkoProperty::getSuperType);
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
        return getProperties(IdProperty.class).findAny().map(IdProperty::getId);
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
        return getProperties(LhsProperty.class).findAny().map(LhsProperty::getLhs);
    }

    @Override
    public Optional<String> getRhs() {
        return getProperties(RhsProperty.class).findAny().map(RhsProperty::getRhs);
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
        Optional<RelationProperty> maybeProperty = getProperties(RelationProperty.class).findAny();

        return maybeProperty.orElseGet(() -> {
            RelationProperty property = new RelationProperty();
            properties.add(property);
            return property;
        });
    }

    @Override
    public Disjunction<Conjunction<VarAdmin>> getDisjunctiveNormalForm() {
        // a disjunction containing only one option
        Conjunction<VarAdmin> conjunction = Patterns.conjunction(Collections.singleton(this));
        return Patterns.disjunction(Collections.singleton(conjunction));
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
    }
}
