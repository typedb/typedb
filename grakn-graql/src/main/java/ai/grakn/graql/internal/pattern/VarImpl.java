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
import ai.grakn.concept.TypeName;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceTypeProperty;
import ai.grakn.graql.internal.pattern.property.HasRoleProperty;
import ai.grakn.graql.internal.pattern.property.HasScopeProperty;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.IsAbstractProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.LhsProperty;
import ai.grakn.graql.internal.pattern.property.NameProperty;
import ai.grakn.graql.internal.pattern.property.NeqProperty;
import ai.grakn.graql.internal.pattern.property.PlaysRoleProperty;
import ai.grakn.graql.internal.pattern.property.RegexProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.pattern.property.RhsProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueFlagProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMultiset;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of Var interface
 */
class VarImpl implements VarAdmin {

    private final Set<VarProperty> properties = new HashSet<>();

    private VarName name;
    private final boolean userDefinedName;

    /**
     * Create a variable with a random variable name
     */
    VarImpl() {
        this.name = new VarNameImpl();
        this.userDefinedName = false;
    }

    /**
     * @param name the variable name of the variable
     */
    VarImpl(VarName name) {
        this.name = name;
        this.userDefinedName = true;
    }

    /**
     * Create a variable by combining a collection of other variables
     * @param vars a collection of variables to combine
     */
    VarImpl(Collection<VarAdmin> vars) {
        VarAdmin first = vars.iterator().next();
        this.name = first.getVarName();
        this.userDefinedName = first.isUserDefinedName();

        for (VarAdmin var : vars) {
            if (var.isUserDefinedName()) {
                this.name = var.getVarName();
            }

            var.getProperties().forEach(this::addProperty);
        }
    }

    /**
     * Create a variable by cloning an existing variable
     * @param var a variable to clone
     */
    private VarImpl(VarAdmin var) {
        this.name = var.getVarName();
        this.userDefinedName = var.isUserDefinedName();
        var.getProperties().forEach(this::addProperty);
    }

    @Override
    public Var id(ConceptId id) {
        return addProperty(new IdProperty(id));
    }

    @Override
    public Var name(String name) {
        return name(TypeName.of(name));
    }

    @Override
    public Var name(TypeName name) {
        return addProperty(new NameProperty(name));
    }

    @Override
    public Var value() {
        return addProperty(new ValueFlagProperty());
    }

    @Override
    public Var value(Object value) {
        return value(Graql.eq(value));
    }

    @Override
    public Var value(ValuePredicate predicate) {
        return addProperty(new ValueProperty(predicate.admin()));
    }

    @Override
    public Var has(String type) {
        return has(type, Graql.var());
    }

    @Override
    public Var has(Var var) {
        return addProperty(new HasResourceProperty(var.admin()));
    }

    @Override
    public Var has(String type, Object value) {
        return has(type, Graql.eq(value));
    }

    @Override
    public Var has(String type, ValuePredicate predicate) {
        return has(type, Graql.var().value(predicate));
    }

    @Override
    public Var has(String type, Var var) {
        return has(TypeName.of(type), var);
    }

    @Override
    public Var has(TypeName type, Var var) {
        return addProperty(new HasResourceProperty(type, var.admin()));
    }

    @Override
    public Var isa(String type) {
        return isa(Graql.name(type));
    }

    @Override
    public Var isa(Var type) {
        return addProperty(new IsaProperty(type.admin()));
    }

    @Override
    public Var sub(String type) {
        return sub(Graql.name(type));
    }

    @Override
    public Var sub(Var type) {
        return addProperty(new SubProperty(type.admin()));
    }

    @Override
    public Var hasRole(String type) {
        return hasRole(Graql.name(type));
    }

    @Override
    public Var hasRole(Var type) {
        return addProperty(new HasRoleProperty(type.admin()));
    }

    @Override
    public Var playsRole(String type) {
        return playsRole(Graql.name(type));
    }

    @Override
    public Var playsRole(Var type) {
        return addProperty(new PlaysRoleProperty(type.admin(), false));
    }

    @Override
    public Var hasScope(Var type) {
        return addProperty(new HasScopeProperty(type.admin()));
    }

    @Override
    public Var hasResource(String type) {
        return hasResource(Graql.name(type));
    }

    @Override
    public Var hasResource(Var type) {
        return addProperty(new HasResourceTypeProperty(type.admin(), false));
    }

    @Override
    public Var hasKey(String type) {
        return hasKey(Graql.var().name(type));
    }

    @Override
    public Var hasKey(Var type) {
        return addProperty(new HasResourceTypeProperty(type.admin(), true));
    }

    @Override
    public Var rel(String roleplayer) {
        return rel(Graql.var(roleplayer));
    }

    @Override
    public Var rel(Var roleplayer) {
        return addCasting(new RelationPlayerImpl(roleplayer.admin()));
    }

    @Override
    public Var rel(String roletype, String roleplayer) {
        return rel(Graql.name(roletype), Graql.var(roleplayer));
    }

    @Override
    public Var rel(Var roletype, String roleplayer) {
        return rel(roletype, Graql.var(roleplayer));
    }

    @Override
    public Var rel(String roletype, Var roleplayer) {
        return rel(Graql.name(roletype), roleplayer);
    }

    @Override
    public Var rel(Var roletype, Var roleplayer) {
        return addCasting(new RelationPlayerImpl(roletype.admin(), roleplayer.admin()));
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
    public Optional<TypeName> getTypeName() {
        return getProperty(NameProperty.class).map(NameProperty::getNameValue);
    }

    @Override
    public VarName getVarName() {
        return name;
    }

    @Override
    public void setVarName(VarName name) {
        if (!userDefinedName) throw new RuntimeException(ErrorMessage.SET_GENERATED_VARIABLE_NAME.getMessage(name));
        this.name = name;
    }

    @Override
    public String getPrintableName() {
        if (userDefinedName) {
            return name.toString();
        } else {
            return getTypeName().map(StringConverter::typeNameToString).orElse("'" + toString() + "'");
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
    public Set<TypeName> getTypeNames() {
        return getProperties()
                .flatMap(VarProperty::getTypes)
                .map(VarAdmin::getTypeName).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());
    }

    @Override
    public String toString() {
        Set<VarAdmin> innerVars = getInnerVars();
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

    private Var addCasting(ai.grakn.graql.admin.RelationPlayer relationPlayer) {
        Optional<RelationProperty> relationProperty = getProperty(RelationProperty.class);

        Stream<ai.grakn.graql.admin.RelationPlayer> oldCastings = relationProperty
                .map(RelationProperty::getRelationPlayers)
                .orElse(Stream.empty());

        ImmutableMultiset<ai.grakn.graql.admin.RelationPlayer> relationPlayers =
                Stream.concat(oldCastings, Stream.of(relationPlayer)).collect(CommonUtil.toImmutableMultiset());

        relationProperty.ifPresent(properties::remove);

        properties.add(new RelationProperty(relationPlayers));

        return this;
    }

    private static boolean invalidInnerVariable(VarAdmin var) {
        return var.getProperties().anyMatch(p -> !(p instanceof NameProperty));
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
    public PatternAdmin cloneMe() {
        return new VarImpl(this);
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
