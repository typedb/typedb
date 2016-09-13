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

package io.mindmaps.graql.internal.query;

import com.google.common.collect.Maps;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.ValuePredicate;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.Disjunction;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.VarTraversals;
import io.mindmaps.graql.internal.util.StringConverter;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.mindmaps.graql.Graql.eq;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.util.ErrorMessage.*;
import static java.util.stream.Collectors.*;

/**
 * Implementation of Var interface
 */
class VarImpl implements VarInternal {

    private String name;
    private final boolean userDefinedName;

    private boolean abstractFlag = false;
    private Optional<ResourceType.DataType<?>> datatype = Optional.empty();

    private Optional<String> id = Optional.empty();

    private boolean valueFlag = false;
    private final Set<ValuePredicateAdmin> values = new HashSet<>();

    private Optional<String> lhs = Optional.empty();
    private Optional<String> rhs = Optional.empty();

    private Optional<VarAdmin> isa = Optional.empty();
    private Optional<VarAdmin> ako = Optional.empty();

    private final Set<VarAdmin> hasRole = new HashSet<>();
    private final Set<VarAdmin> playsRole = new HashSet<>();
    private final Set<VarAdmin> hasScope = new HashSet<>();
    private final Set<VarAdmin> hasResourceTypes = new HashSet<>();

    private final Set<VarAdmin> resources = new HashSet<>();

    private final Set<VarAdmin.Casting> castings = new HashSet<>();

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

        valueFlag = false;

        for (VarAdmin var : vars) {
            if (var.isUserDefinedName()) {
                this.name = var.getName();
            }

            valueFlag |= var.hasValue();
            abstractFlag |= var.getAbstract();

            var.getDatatype().ifPresent(this::datatype);
            var.getType().ifPresent(this::isa);
            var.getAko().ifPresent(this::ako);

            var.getId().ifPresent(this::id);
            var.getLhs().ifPresent(this::lhs);
            var.getRhs().ifPresent(this::rhs);
            values.addAll(var.getValuePredicates());

            hasRole.addAll(var.getHasRoles());
            playsRole.addAll(var.getPlaysRoles());
            hasScope.addAll(var.getScopes());

            // Currently it is guaranteed that resource types are specified with an ID
            //noinspection OptionalGetWithoutIsPresent
            var.getResources().forEach(resources::add);

            castings.addAll(var.getCastings());
        }
    }

    @Override
    public Var id(String id) {
        this.id.ifPresent(
                prevId -> {
                    if (!prevId.equals(id)) throw new IllegalStateException(MULTIPLE_IDS.getMessage(id, prevId));
                }
        );
        this.id = Optional.of(id);
        return this;
    }

    @Override
    public Var value() {
        valueFlag = true;
        return this;
    }

    @Override
    public Var value(Object value) {
        return value(eq(value));
    }

    @Override
    public Var value(ValuePredicate predicate) {
        values.add(predicate.admin());
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
        resources.add(var.isa(type).admin());
        return this;
    }

    @Override
    public Var isa(String type) {
        return isa(var().id(type));
    }

    @Override
    public Var isa(Var type) {
        VarAdmin var = type.admin();

        isa.ifPresent(
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
        isa = Optional.of(var);
        return this;
    }

    @Override
    public Var ako(String type) {
        return ako(var().id(type));
    }

    @Override
    public Var ako(Var type) {
        ako = Optional.of(type.admin());
        return this;
    }

    @Override
    public Var hasRole(String type) {
        return hasRole(var().id(type));
    }

    @Override
    public Var hasRole(Var type) {
        hasRole.add(type.admin());
        return this;
    }

    @Override
    public Var playsRole(String type) {
        return playsRole(var().id(type));
    }

    @Override
    public Var playsRole(Var type) {
        playsRole.add(type.admin());
        return this;
    }

    @Override
    public Var hasScope(Var type) {
        hasScope.add(type.admin());
        return this;
    }

    @Override
    public Var hasResource(String type) {
        return hasResource(var().id(type));
    }

    @Override
    public Var hasResource(Var type) {
        hasResourceTypes.add(type.admin());
        return this;
    }

    @Override
    public Var rel(String roleplayer) {
        return rel(var(roleplayer));
    }

    @Override
    public Var rel(Var roleplayer) {
        castings.add(new Casting(roleplayer.admin()));
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
        castings.add(new Casting(roletype.admin(), roleplayer.admin()));
        return this;
    }

    @Override
    public Var isAbstract() {
        abstractFlag = true;
        return this;
    }

    @Override
    public Var datatype(ResourceType.DataType<?> datatype) {
        this.datatype = Optional.of(datatype);
        return this;
    }

    @Override
    public Var lhs(String lhs) {
        this.lhs = Optional.of(lhs);
        return this;
    }

    @Override
    public Var rhs(String rhs) {
        this.rhs = Optional.of(rhs);
        return this;
    }

    @Override
    public VarInternal admin() {
        return this;
    }


    @Override
    public Optional<VarAdmin> getType() {
        return isa;
    }

    @Override
    public boolean isRelation() {
        return !castings.isEmpty();
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
        return abstractFlag;
    }

    @Override
    public Optional<ResourceType.DataType<?>> getDatatype() {
        return datatype;
    }

    @Override
    public boolean hasValue() {
        return valueFlag;
    }

    @Override
    public Optional<VarAdmin> getAko() {
        return ako;
    }

    @Override
    public Set<VarAdmin> getHasRoles() {
        return hasRole;
    }

    @Override
    public Set<VarAdmin> getPlaysRoles() {
        return playsRole;
    }

    @Override
    public Set<VarAdmin> getScopes() {
        return hasScope;
    }

    @Override
    public Set<VarAdmin> getHasResourceTypes() {
        return hasResourceTypes;
    }

    @Override
    public Set<String> getRoleTypes() {
        return getIdNames(castings.stream().map(VarAdmin.Casting::getRoleType).flatMap(this::optionalToStream));
    }

    @Override
    public Optional<String> getId() {
        return id;
    }

    @Override
    public Set<String> getResourceTypes() {
        // Currently it is guaranteed that resources have a type with an ID
        //noinspection OptionalGetWithoutIsPresent
        return resources.stream().map(resource -> resource.getType().get().getId().get()).collect(toSet());
    }

    @Override
    public boolean hasNoProperties() {
        // return true if this variable has any properties set
        return !id.isPresent() && !valueFlag && values.isEmpty() && !isa.isPresent() && !ako.isPresent() &&
                hasRole.isEmpty() && playsRole.isEmpty() && hasScope.isEmpty() && resources.isEmpty() &&
                castings.isEmpty();
    }

    @Override
    public Optional<String> getIdOnly() {
        if (id.isPresent() && !valueFlag && values.isEmpty() && !isa.isPresent() && !ako.isPresent() &&
                hasRole.isEmpty() && playsRole.isEmpty() && hasScope.isEmpty() && resources.isEmpty() &&
                castings.isEmpty() && !userDefinedName) {
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
        return values.stream()
                .map(ValuePredicateAdmin::equalsValue)
                .flatMap(this::optionalToStream)
                .collect(toSet());
    }

    @Override
    public Set<ValuePredicateAdmin> getValuePredicates() {
        return values;
    }

    @Override
    public Optional<String> getLhs() {
        return lhs;
    }

    @Override
    public Optional<String> getRhs() {
        return rhs;
    }

    @Override
    public Set<VarAdmin> getResources() {
        return resources;
    }

    @Override
    public Map<VarAdmin, Set<ValuePredicateAdmin>> getResourcePredicates() {
        // The type of the resource is guaranteed to exist
        //noinspection OptionalGetWithoutIsPresent
        Function<VarAdmin, VarAdmin> type = v -> v.getType().get();

        Function<VarAdmin, Stream<ValuePredicateAdmin>> predicates =
                resource -> resource.getValuePredicates().stream();

        Map<VarAdmin, List<VarAdmin>> groupedByType = resources.stream().collect(groupingBy(type));

        return Maps.transformValues(groupedByType, vars -> vars.stream().flatMap(predicates).collect(toSet()));
    }

    public Set<VarAdmin.Casting> getCastings() {
        return castings;
    }

    @Override
    public Set<MultiTraversal> getMultiTraversals() {
        return getVarTraversals().getTraversals().collect(toSet());
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
        id.ifPresent(results::add);
        return results;
    }

    @Override
    public String toString() {
        Set<String> properties = new HashSet<>();

        Set<VarAdmin> innerVars = getInnerVars();
        innerVars.remove(this);
        innerVars.removeAll(resources);

        if (!innerVars.stream().allMatch(v -> v.getIdOnly().isPresent() || v.hasNoProperties())) {
            throw new UnsupportedOperationException("Graql strings cannot represent a query with inner variables");
        }

        id.ifPresent(i -> properties.add("id " + StringConverter.valueToString(i)));

        if (isRelation()) {
            properties.add("(" + castings.stream().map(Object::toString).collect(joining(", ")) + ")");
        }

        isa.ifPresent(v -> properties.add("isa " + v.getPrintableName()));
        ako.ifPresent(v -> properties.add("ako " + v.getPrintableName()));
        playsRole.forEach(v -> properties.add("plays-role " + v.getPrintableName()));
        hasRole.forEach(v -> properties.add("has-role " + v.getPrintableName()));
        hasScope.forEach(v -> properties.add("has-scope " + v.getPrintableName()));
        hasResourceTypes.forEach(v -> properties.add("has-resource " + v.getPrintableName()));

        getDatatypeName().ifPresent(d -> properties.add("datatype " + d));

        if (getAbstract()) properties.add("is-abstract");

        values.forEach(v -> properties.add("value " + v));

        resources.forEach(
                resource -> {
                    // Currently it is guaranteed that resources have a type specified
                    //noinspection OptionalGetWithoutIsPresent
                    String type = resource.getType().get().getId().get();

                    String resourceRepr;
                    if (resource.isUserDefinedName()) {
                        resourceRepr = " " + resource.getName();
                    } else if (resource.hasNoProperties()) {
                        resourceRepr = "";
                    } else {
                        resourceRepr = " " + resource.getValuePredicates().iterator().next().toString();
                    }
                    properties.add("has " + type + resourceRepr);
                }
        );

        lhs.ifPresent(s -> properties.add("lhs {" + s + "}"));
        rhs.ifPresent(s -> properties.add("rhs {" + s + "}"));

        String name = isUserDefinedName() ? getPrintableName() + " " : "";

        return name + properties.stream().collect(joining(", "));
    }

    /**
     * @return the datatype's name (as referred to in native Graql), if one is specified
     */
    private Optional<String> getDatatypeName() {
        return datatype.map(
                d -> {
                    if (d == ResourceType.DataType.BOOLEAN) {
                        return "boolean";
                    } else if (d == ResourceType.DataType.DOUBLE) {
                        return "double";
                    } else if (d == ResourceType.DataType.LONG) {
                        return "long";
                    } else if (d == ResourceType.DataType.STRING) {
                        return "string";
                    } else {
                        throw new RuntimeException("Unknown data type: " + d.getName());
                    }
                }
        );
    }

    /**
     * @param vars a stream of variables
     * @return the IDs of all variables that refer to things by id in the graph
     */
    private Set<String> getIdNames(Stream<VarAdmin> vars) {
        return vars.map(VarAdmin::getId).flatMap(this::optionalToStream).collect(toSet());
    }

    /**
     * @param optional the optional to change into a stream
     * @param <T> the type in the optional
     * @return a stream of one item if the optional has an element, else an empty stream
     */
    private <T> Stream<T> optionalToStream(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }

    /**
     * @return the VarTraversals object representing this Var as gremlin traversals
     */
    private VarTraversals getVarTraversals() {
        VarTraversals varTraversals = this.varPattern.orElseGet(() -> new VarTraversals(this));
        this.varPattern = Optional.of(varTraversals);
        return varTraversals;
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
            return getRoleType().map(r -> r.getPrintableName() + " ").orElse("") + getRolePlayer().getPrintableName();
        }
    }
}
