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

package ai.grakn.graql.internal.pattern;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceTypeProperty;
import ai.grakn.graql.internal.pattern.property.RelatesProperty;
import ai.grakn.graql.internal.pattern.property.HasScopeProperty;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.IsAbstractProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.LhsProperty;
import ai.grakn.graql.internal.pattern.property.LabelProperty;
import ai.grakn.graql.internal.pattern.property.NeqProperty;
import ai.grakn.graql.internal.pattern.property.PlaysProperty;
import ai.grakn.graql.internal.pattern.property.RegexProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.pattern.property.RhsProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of Var interface
 */
class VarImpl implements VarAdmin {

    private final VarName name;
    private final boolean userDefinedName;

    private final Set<VarProperty> properties;

    private VarImpl(VarName name, boolean userDefinedName, Set<VarProperty> properties) {
        this.name = name;
        this.userDefinedName = userDefinedName;
        this.properties = properties;
    }

    /**
     * Create a variable with a random variable name
     */
    static VarImpl anon() {
        return new VarImpl(VarName.anon(), false, ImmutableSet.of());
    }

    /**
     * Create a variable with a specified name
     * @param name the name of the variable
     */
    static VarImpl named(VarName name) {
        return new VarImpl(name, true, ImmutableSet.of());
    }

    /**
     * Create a variable by combining a collection of other variables
     * @param vars a collection of variables to combine
     */
    static VarImpl merge(Collection<VarAdmin> vars) {
        VarAdmin first = vars.iterator().next();
        VarName name = first.getVarName();
        boolean userDefinedName = first.isUserDefinedName();
        ImmutableSet.Builder<VarProperty> properties = ImmutableSet.builder();

        for (VarAdmin var : vars) {
            if (var.isUserDefinedName()) {
                name = var.getVarName();
            }

            properties.addAll(var.getProperties().iterator());
        }

        return new VarImpl(name, userDefinedName, properties.build());
    }

    @Override
    public Var id(ConceptId id) {
        return addProperty(new IdProperty(id));
    }

    @Override
    public Var label(String label) {
        return label(TypeLabel.of(label));
    }

    @Override
    public Var label(TypeLabel label) {
        return addProperty(new LabelProperty(label));
    }

    @Override
    public Var val(Object value) {
        return val(Graql.eq(value));
    }

    @Override
    public Var val(ValuePredicate predicate) {
        return addProperty(new ValueProperty(predicate.admin()));
    }

    @Override
    public Var has(String type, Object value) {
        return has(type, Graql.eq(value));
    }

    @Override
    public Var has(String type, ValuePredicate predicate) {
        return has(type, Graql.var().val(predicate));
    }

    @Override
    public Var has(String type, Var var) {
        return has(TypeLabel.of(type), var);
    }

    @Override
    public Var has(TypeLabel type, Var var) {
        return addProperty(HasResourceProperty.of(type, var.admin()));
    }

    @Override
    public Var isa(String type) {
        return isa(Graql.label(type));
    }

    @Override
    public Var isa(Var type) {
        return addProperty(new IsaProperty(type.admin()));
    }

    @Override
    public Var sub(String type) {
        return sub(Graql.label(type));
    }

    @Override
    public Var sub(Var type) {
        return addProperty(new SubProperty(type.admin()));
    }

    @Override
    public Var relates(String type) {
        return relates(Graql.label(type));
    }

    @Override
    public Var relates(Var type) {
        return addProperty(new RelatesProperty(type.admin()));
    }

    @Override
    public Var plays(String type) {
        return plays(Graql.label(type));
    }

    @Override
    public Var plays(Var type) {
        return addProperty(new PlaysProperty(type.admin(), false));
    }

    @Override
    public Var hasScope(Var type) {
        return addProperty(new HasScopeProperty(type.admin()));
    }

    @Override
    public Var has(String type) {
        return has(Graql.label(type));
    }

    @Override
    public Var has(Var type) {
        return addProperty(new HasResourceTypeProperty(type.admin(), false));
    }

    @Override
    public Var key(String type) {
        return key(Graql.var().label(type));
    }

    @Override
    public Var key(Var type) {
        return addProperty(new HasResourceTypeProperty(type.admin(), true));
    }

    @Override
    public Var rel(String roleplayer) {
        return rel(Graql.var(roleplayer));
    }

    @Override
    public Var rel(Var roleplayer) {
        return addCasting(RelationPlayerImpl.of(roleplayer.admin()));
    }

    @Override
    public Var rel(String roletype, String roleplayer) {
        return rel(Graql.label(roletype), Graql.var(roleplayer));
    }

    @Override
    public Var rel(Var roletype, String roleplayer) {
        return rel(roletype, Graql.var(roleplayer));
    }

    @Override
    public Var rel(String roletype, Var roleplayer) {
        return rel(Graql.label(roletype), roleplayer);
    }

    @Override
    public Var rel(Var roletype, Var roleplayer) {
        return addCasting(RelationPlayerImpl.of(roletype.admin(), roleplayer.admin()));
    }

    @Override
    public Var isAbstract() {
        return addProperty(new IsAbstractProperty());
    }

    @Override
    public Var datatype(ResourceType.DataType<?> datatype) {
        return addProperty(new DataTypeProperty(requireNonNull(datatype)));
    }

    @Override
    public Var regex(String regex) {
        return addProperty(new RegexProperty(regex));
    }

    @Override
    public Var lhs(Pattern lhs) {
        return addProperty(new LhsProperty(lhs));
    }

    @Override
    public Var rhs(Pattern rhs) {
        return addProperty(new RhsProperty(rhs));
    }

    @Override
    public Var neq(String varName) {
        return neq(Graql.var(varName));
    }

    @Override
    public Var neq(Var var) {
        return addProperty(new NeqProperty(var.admin()));
    }

    @Override
    public VarAdmin admin() {
        return this;
    }

    @Override
    public boolean isUserDefinedName() {
        return userDefinedName;
    }

    @Override
    public Optional<ConceptId> getId() {
        return getProperty(IdProperty.class).map(IdProperty::getId);
    }

    @Override
    public Optional<TypeLabel> getTypeLabel() {
        return getProperty(LabelProperty.class).map(LabelProperty::getLabelValue);
    }

    @Override
    public VarName getVarName() {
        return name;
    }

    @Override
    public VarAdmin setVarName(VarName name) {
        if (!userDefinedName) throw new RuntimeException(ErrorMessage.SET_GENERATED_VARIABLE_NAME.getMessage(name));
        return new VarImpl(name, true, properties);
    }

    @Override
    public String getPrintableName() {
        if (userDefinedName) {
            return name.toString();
        } else {
            return getTypeLabel().map(StringConverter::typeLabelToString).orElse("'" + toString() + "'");
        }
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
    public <T extends VarProperty> boolean hasProperty(Class<T> type) {
        return getProperties(type).findAny().isPresent();
    }

    @Override
    public <T extends VarProperty> VarAdmin mapProperty(Class<T> type, UnaryOperator<T> mapper) {
        ImmutableSet<VarProperty> newProperties = getProperties().map(property -> {
            if (type.isInstance(property)) {
                return mapper.apply(type.cast(property));
            } else {
                return property;
            }
        }).collect(toImmutableSet());

        return new VarImpl(name, userDefinedName, newProperties);
    }

    @Override
    public Collection<VarAdmin> getInnerVars() {
        Stack<VarAdmin> newVars = new Stack<>();
        List<VarAdmin> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarAdmin var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::getInnerVars).forEach(newVars::add);
        }

        return vars;
    }

    @Override
    public Collection<VarAdmin> getImplicitInnerVars() {
        Stack<VarAdmin> newVars = new Stack<>();
        List<VarAdmin> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarAdmin var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::getImplicitInnerVars).forEach(newVars::add);
        }

        return vars;
    }

    @Override
    public Set<TypeLabel> getTypeLabels() {
        return getProperties()
                .flatMap(VarProperty::getTypes)
                .map(VarAdmin::getTypeLabel).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());
    }

    @Override
    public String toString() {
        Collection<VarAdmin> innerVars = getInnerVars();
        innerVars.remove(this);
        getProperties(HasResourceProperty.class)
                .map(HasResourceProperty::getResource)
                .flatMap(r -> r.getInnerVars().stream())
                .forEach(innerVars::remove);

        if (innerVars.stream().anyMatch(VarImpl::invalidInnerVariable)) {
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

    private Var addCasting(RelationPlayer relationPlayer) {
        Optional<RelationProperty> relationProperty = getProperty(RelationProperty.class);

        Stream<RelationPlayer> oldCastings = relationProperty
                .map(RelationProperty::getRelationPlayers)
                .orElse(Stream.empty());

        ImmutableMultiset<RelationPlayer> relationPlayers =
                Stream.concat(oldCastings, Stream.of(relationPlayer)).collect(CommonUtil.toImmutableMultiset());

        RelationProperty newProperty = new RelationProperty(relationPlayers);

        return relationProperty.map(this::removeProperty).orElse(this).addProperty(newProperty);
    }

    private static boolean invalidInnerVariable(VarAdmin var) {
        return var.getProperties().anyMatch(p -> !(p instanceof LabelProperty));
    }

    private VarImpl addProperty(VarProperty property) {
        if (property.isUnique()) {
            testUniqueProperty((UniqueVarProperty) property);
        }
        return new VarImpl(name, userDefinedName, Sets.union(properties, ImmutableSet.of(property)));
    }

    private VarImpl removeProperty(VarProperty property) {
        return new VarImpl(name, userDefinedName, Sets.difference(properties, ImmutableSet.of(property)));
    }

    /**
     * Fail if there is already an equal property of this type
     */
    private void testUniqueProperty(UniqueVarProperty property) {
        getProperty(property.getClass()).filter(other -> !other.equals(property)).ifPresent(other -> {
            String message = ErrorMessage.CONFLICTING_PROPERTIES.getMessage(
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
    public Set<VarName> commonVarNames() {
        return getInnerVars().stream().filter(VarAdmin::isUserDefinedName).map(VarAdmin::getVarName).collect(toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarImpl var = (VarImpl) o;

        if (userDefinedName != var.userDefinedName) return false;

        // "simplifying" this makes it harder to read
        //noinspection SimplifiableIfStatement
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

}
