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
import io.mindmaps.core.implementation.Data;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.ValuePredicate;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.StringConverter;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.VarTraversals;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.validation.ErrorMessage.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of Var interface
 */
public class VarImpl implements Var.Admin {

    private String name;
    private final boolean userDefinedName;

    private boolean abstractFlag = false;
    private Optional<Data<?>> datatype = Optional.empty();

    private Optional<String> id = Optional.empty();

    private boolean valueFlag = false;
    private final Set<ValuePredicate.Admin> values = new HashSet<>();

    private Optional<String> lhs = Optional.empty();
    private Optional<String> rhs = Optional.empty();

    private Optional<Var.Admin> isa = Optional.empty();
    private Optional<Var.Admin> ako = Optional.empty();

    private final Set<Var.Admin> hasRole = new HashSet<>();
    private final Set<Var.Admin> playsRole = new HashSet<>();
    private final Set<Var.Admin> hasScope = new HashSet<>();
    private final Set<Var.Admin> hasResourceTypes = new HashSet<>();

    private final Map<Var.Admin, Set<ValuePredicate.Admin>> resources = new HashMap<>();

    private final Set<Var.Casting> castings = new HashSet<>();

    private Optional<VarTraversals> varPattern = Optional.empty();

    /**
     * Create a variable with a random variable name
     */
    public VarImpl() {
        this.name = UUID.randomUUID().toString();
        this.userDefinedName = false;
    }

    /**
     * @param name the variable name of the variable
     */
    public VarImpl(String name) {
        this.name = name;
        this.userDefinedName = true;
    }

    /**
     * Create a variable by combining a collection of other variables
     * @param vars a collection of variables to combine
     */
    VarImpl(Collection<Var.Admin> vars) {
        Var.Admin first = vars.iterator().next();
        this.name = first.getName();
        this.userDefinedName = first.isUserDefinedName();

        valueFlag = false;

        for (Var.Admin var : vars) {
            if (var.isUserDefinedName()) {
                this.name = var.getName();
            }

            valueFlag |= var.hasValue();
            abstractFlag |= var.getAbstract();

            var.getDatatype().ifPresent(this::datatype);
            var.getType().ifPresent(this::isa);
            var.getAko().ifPresent(this::ako);

            var.getId().ifPresent(this::id);
            values.addAll(var.getValuePredicates());

            hasRole.addAll(var.getHasRoles());
            playsRole.addAll(var.getPlaysRoles());
            hasScope.addAll(var.getScopes());

            var.getResourcePredicates().forEach(
                    (type, values) -> values.forEach(value -> has(type.getId().get(), value))
            );

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
    public Var value(ValuePredicate predicate) {
        values.add(predicate.admin());
        return this;
    }

    @Override
    public Var has(String type) {
        Var.Admin resourceVar = QueryBuilder.id(Objects.requireNonNull(type)).admin();
        resources.putIfAbsent(resourceVar, new HashSet<>());
        return this;
    }

    @Override
    public Var has(String type, ValuePredicate predicate) {
        Var.Admin resourceVar = QueryBuilder.id(Objects.requireNonNull(type)).admin();
        resources.computeIfAbsent(resourceVar, k -> new HashSet<>()).add(predicate.admin());
        return this;
    }

    @Override
    public Var isa(Var type) {
        Var.Admin var = type.admin();

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
    public Var ako(Var type) {
        ako = Optional.of(type.admin());
        return this;
    }

    @Override
    public Var hasRole(Var type) {
        hasRole.add(type.admin());
        return this;
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
        hasResourceTypes.add(QueryBuilder.id(type).admin());
        return this;
    }

    @Override
    public Var rel(Var roleplayer) {
        castings.add(new Casting(roleplayer.admin()));
        return this;
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
    public Var datatype(Data<?> datatype) {
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
    public Var.Admin admin() {
        return this;
    }


    @Override
    public Optional<Var.Admin> getType() {
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
        Stream<ValuePredicate.Admin> predicates = Stream.of(
                values.stream(),
                resources.values().stream().flatMap(Collection::stream)
        ).flatMap(Function.identity());

        return predicates.anyMatch(id -> !id.equalsValue().isPresent());
    }

    @Override
    public boolean getAbstract() {
        return abstractFlag;
    }

    @Override
    public Optional<Data<?>> getDatatype() {
        return datatype;
    }

    @Override
    public boolean hasValue() {
        return valueFlag;
    }

    @Override
    public Optional<Var.Admin> getAko() {
        return ako;
    }

    @Override
    public Set<Var.Admin> getHasRoles() {
        return hasRole;
    }

    @Override
    public Set<Var.Admin> getPlaysRoles() {
        return playsRole;
    }

    @Override
    public Set<Var.Admin> getScopes() {
        return hasScope;
    }

    @Override
    public Set<Var.Admin> getHasResourceTypes() {
        return hasResourceTypes;
    }

    @Override
    public Set<String> getRoleTypes() {
        return getIdNames(castings.stream().map(Var.Casting::getRoleType).flatMap(this::optionalToStream));
    }

    @Override
    public Optional<String> getId() {
        return id;
    }

    @Override
    public Set<String> getResourceTypes() {
        return resources.keySet().stream().map(var -> var.getId().get()).collect(toSet());
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
            return getId().orElse("$" + name);
        }
    }

    @Override
    public Set<?> getValueEqualsPredicates() {
        return getEqualsPredicatesUnknownType(values);
    }

    @Override
    public Set<ValuePredicate.Admin> getValuePredicates() {
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
    public Map<Var.Admin, Set<?>> getResourceEqualsPredicates() {
        return Maps.transformValues(resources, this::getEqualsPredicatesUnknownType);
    }

    @Override
    public Map<Var.Admin, Set<ValuePredicate.Admin>> getResourcePredicates() {
        return resources;
    }

    public Set<Var.Casting> getCastings() {
        return castings;
    }

    @Override
    public Set<MultiTraversal> getMultiTraversals() {
        return getVarTraversals().getTraversals().collect(toSet());
    }

    @Override
    public Set<Var.Admin> getInnerVars() {
        Stack<Var.Admin> newVars = new Stack<>();
        Set<Var.Admin> vars = new HashSet<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            Var.Admin var = newVars.pop();
            vars.add(var);

            var.getType().ifPresent(newVars::add);
            var.getAko().ifPresent(newVars::add);
            var.getHasRoles().forEach(newVars::add);
            var.getPlaysRoles().forEach(newVars::add);
            var.getScopes().forEach(newVars::add);
            var.getHasResourceTypes().forEach(newVars::add);
            var.getResourcePredicates().keySet().forEach(newVars::add);

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

        Set<Var.Admin> innerVars = getInnerVars();
        innerVars.remove(this);

        if (!innerVars.stream().allMatch(v -> v.getIdOnly().isPresent() || v.hasNoProperties())) {
            throw new UnsupportedOperationException("Graql strings cannot represent a query with inner variables");
        }

        isa.ifPresent(v -> properties.add("isa " + v.getPrintableName()));
        ako.ifPresent(v -> properties.add("ako " + v.getPrintableName()));
        playsRole.forEach(v -> properties.add("plays-role " + v.getPrintableName()));
        hasRole.forEach(v -> properties.add("has-role " + v.getPrintableName()));
        hasScope.forEach(v -> properties.add("has-scope " + v.getPrintableName()));

        id.ifPresent(i -> properties.add("id " + StringConverter.valueToString(i)));
        values.forEach(v -> properties.add("value " + v));

        resources.forEach(
                (type, predicates) -> predicates.forEach(p -> properties.add("has " + type.getId().get() + " " + p))
        );

        getDatatypeName().ifPresent(d -> properties.add("datatype " + d));

        if (getAbstract()) properties.add("is-abstract");

        lhs.ifPresent(s -> properties.add("lhs {" + s + "}"));
        rhs.ifPresent(s -> properties.add("rhs {" + s + "}"));

        if (isRelation()) {
            properties.add("(" + castings.stream().map(Object::toString).collect(joining(", ")) + ")");
        }

        String name = isUserDefinedName() ? getPrintableName() + " " : "";

        return name + properties.stream().collect(joining(", "));
    }

    /**
     * @return the datatype's name (as referred to in native Graql), if one is specified
     */
    private Optional<String> getDatatypeName() {
        return datatype.map(
                d -> {
                    if (d == Data.BOOLEAN) {
                        return "boolean";
                    } else if (d == Data.DOUBLE) {
                        return "double";
                    } else if (d == Data.LONG) {
                        return "long";
                    } else if (d == Data.STRING) {
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
    private Set<String> getIdNames(Stream<Var.Admin> vars) {
        return vars.map(Var.Admin::getId).flatMap(this::optionalToStream).collect(toSet());
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
     * @param predicates a collection of predicates of an unknown type
     * @return all values of predicates in the collection which are simple 'equals' predicates
     */
    private Set<?> getEqualsPredicatesUnknownType(Collection<ValuePredicate.Admin> predicates) {
        return predicates.stream()
                .map(ValuePredicate.Admin::equalsValue)
                .flatMap(this::optionalToStream)
                .collect(toSet());
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
    public Disjunction<Conjunction<Var.Admin>> getDisjunctiveNormalForm() {
        // a disjunction containing only one option
        Conjunction<Var.Admin> conjunction = Pattern.Admin.conjunction(Collections.singleton(this));
        return Pattern.Admin.disjunction(Collections.singleton(conjunction));
    }

    /**
     * A casting is the pairing of roletype and roleplayer in a relation, where the roletype may be unknown
     */
    public class Casting implements Var.Casting {
        private final Optional<Var.Admin> roleType;
        private final Var.Admin rolePlayer;

        /**
         * A casting without a role type specified
         * @param rolePlayer the role player of the casting
         */
        public Casting(Var.Admin rolePlayer) {
            this.roleType = Optional.empty();
            this.rolePlayer = rolePlayer;
        }

        /**
         * @param roletype the role type of the casting
         * @param rolePlayer the role player of the casting
         */
        public Casting(Var.Admin roletype, Var.Admin rolePlayer) {
            this.roleType = Optional.of(roletype);
            this.rolePlayer = rolePlayer;
        }

        @Override
        public Optional<Var.Admin> getRoleType() {
            return roleType;
        }

        @Override
        public Var.Admin getRolePlayer() {
            return rolePlayer;
        }

        @Override
        public String toString() {
            return getRoleType().map(r -> r.getPrintableName() + " ").orElse("") + getRolePlayer().getPrintableName();
        }
    }
}
