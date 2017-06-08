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
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.VarPatternBuilder;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceTypeProperty;
import ai.grakn.graql.internal.pattern.property.HasScopeProperty;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.IsAbstractProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.LabelProperty;
import ai.grakn.graql.internal.pattern.property.LhsProperty;
import ai.grakn.graql.internal.pattern.property.NeqProperty;
import ai.grakn.graql.internal.pattern.property.PlaysProperty;
import ai.grakn.graql.internal.pattern.property.RegexProperty;
import ai.grakn.graql.internal.pattern.property.RelatesProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.pattern.property.RhsProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of {@link VarPattern} interface
 */
class VarPatternImpl implements VarPatternAdmin {

    private final Var name;

    private final Set<VarProperty> properties;

    VarPatternImpl(Var name, Set<VarProperty> properties) {
        this.name = name;
        this.properties = properties;
    }

    @Override
    public VarPattern id(ConceptId id) {
        return addProperty(new IdProperty(id));
    }

    @Override
    public VarPattern label(String label) {
        return label(TypeLabel.of(label));
    }

    @Override
    public VarPattern label(TypeLabel label) {
        return addProperty(new LabelProperty(label));
    }

    @Override
    public VarPattern val(Object value) {
        return val(Graql.eq(value));
    }

    @Override
    public VarPattern val(ValuePredicate predicate) {
        return addProperty(new ValueProperty(predicate.admin()));
    }

    @Override
    public VarPattern has(String type, Object value) {
        return has(type, Graql.eq(value));
    }

    @Override
    public VarPattern has(String type, ValuePredicate predicate) {
        return has(type, Graql.var().val(predicate));
    }

    @Override
    public VarPattern has(String type, VarPatternBuilder varPattern) {
        return has(TypeLabel.of(type), varPattern);
    }

    @Override
    public VarPattern has(TypeLabel type, VarPatternBuilder varPattern) {
        return addProperty(HasResourceProperty.of(type, varPattern.pattern().admin()));
    }

    @Override
    public VarPattern isa(String type) {
        return isa(Graql.label(type));
    }

    @Override
    public VarPattern isa(VarPatternBuilder type) {
        return addProperty(new IsaProperty(type.pattern().admin()));
    }

    @Override
    public VarPattern sub(String type) {
        return sub(Graql.label(type));
    }

    @Override
    public VarPattern sub(VarPatternBuilder type) {
        return addProperty(new SubProperty(type.pattern().admin()));
    }

    @Override
    public VarPattern relates(String type) {
        return relates(Graql.label(type));
    }

    @Override
    public VarPattern relates(VarPatternBuilder type) {
        return addProperty(new RelatesProperty(type.pattern().admin()));
    }

    @Override
    public VarPattern plays(String type) {
        return plays(Graql.label(type));
    }

    @Override
    public VarPattern plays(VarPatternBuilder type) {
        return addProperty(new PlaysProperty(type.pattern().admin(), false));
    }

    @Override
    public VarPattern hasScope(VarPatternBuilder type) {
        return addProperty(new HasScopeProperty(type.pattern().admin()));
    }

    @Override
    public VarPattern has(String type) {
        return has(Graql.label(type));
    }

    @Override
    public VarPattern has(VarPatternBuilder type) {
        return addProperty(new HasResourceTypeProperty(type.pattern().admin(), false));
    }

    @Override
    public VarPattern key(String type) {
        return key(Graql.var().label(type));
    }

    @Override
    public VarPattern key(VarPatternBuilder type) {
        return addProperty(new HasResourceTypeProperty(type.pattern().admin(), true));
    }

    @Override
    public VarPattern rel(String roleplayer) {
        return rel(Graql.var(roleplayer));
    }

    @Override
    public VarPattern rel(VarPatternBuilder roleplayer) {
        return addCasting(RelationPlayerImpl.of(roleplayer.pattern().admin()));
    }

    @Override
    public VarPattern rel(String roletype, String roleplayer) {
        return rel(Graql.label(roletype), Graql.var(roleplayer));
    }

    @Override
    public VarPattern rel(VarPatternBuilder roletype, String roleplayer) {
        return rel(roletype, Graql.var(roleplayer));
    }

    @Override
    public VarPattern rel(String roletype, VarPatternBuilder roleplayer) {
        return rel(Graql.label(roletype), roleplayer);
    }

    @Override
    public VarPattern rel(VarPatternBuilder roletype, VarPatternBuilder roleplayer) {
        return addCasting(RelationPlayerImpl.of(roletype.pattern().admin(), roleplayer.pattern().admin()));
    }

    @Override
    public VarPattern isAbstract() {
        return addProperty(new IsAbstractProperty());
    }

    @Override
    public VarPattern datatype(ResourceType.DataType<?> datatype) {
        return addProperty(new DataTypeProperty(requireNonNull(datatype)));
    }

    @Override
    public VarPattern regex(String regex) {
        return addProperty(new RegexProperty(regex));
    }

    @Override
    public VarPattern lhs(Pattern lhs) {
        return addProperty(new LhsProperty(lhs));
    }

    @Override
    public VarPattern rhs(Pattern rhs) {
        return addProperty(new RhsProperty(rhs));
    }

    @Override
    public VarPattern neq(String var) {
        return neq(Graql.var(var));
    }

    @Override
    public VarPattern neq(VarPatternBuilder varPattern) {
        return addProperty(new NeqProperty(varPattern.pattern().admin()));
    }

    @Override
    public VarPattern pattern() {
        return this;
    }

    @Override
    public VarPatternAdmin admin() {
        return this;
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
    public Var getVarName() {
        return name;
    }

    @Override
    public VarPatternAdmin setVarName(Var name) {
        if (!this.name.isUserDefinedName()) throw new RuntimeException(ErrorMessage.SET_GENERATED_VARIABLE_NAME.getMessage(name));
        return new VarPatternImpl(name, properties);
    }

    @Override
    public String getPrintableName() {
        if (name.isUserDefinedName()) {
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
    public <T extends VarProperty> VarPatternAdmin mapProperty(Class<T> type, UnaryOperator<T> mapper) {
        ImmutableSet<VarProperty> newProperties = getProperties().map(property -> {
            if (type.isInstance(property)) {
                return mapper.apply(type.cast(property));
            } else {
                return property;
            }
        }).collect(toImmutableSet());

        return new VarPatternImpl(name, newProperties);
    }

    @Override
    public Collection<VarPatternAdmin> getInnerVars() {
        Stack<VarPatternAdmin> newVars = new Stack<>();
        List<VarPatternAdmin> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarPatternAdmin var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::getInnerVars).forEach(newVars::add);
        }

        return vars;
    }

    @Override
    public Collection<VarPatternAdmin> getImplicitInnerVars() {
        Stack<VarPatternAdmin> newVars = new Stack<>();
        List<VarPatternAdmin> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarPatternAdmin var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::getImplicitInnerVars).forEach(newVars::add);
        }

        return vars;
    }

    @Override
    public Set<TypeLabel> getTypeLabels() {
        return getProperties()
                .flatMap(VarProperty::getTypes)
                .map(VarPatternAdmin::getTypeLabel).flatMap(Streams::stream)
                .collect(toSet());
    }

    @Override
    public String toString() {
        Collection<VarPatternAdmin> innerVars = getInnerVars();
        innerVars.remove(this);
        getProperties(HasResourceProperty.class)
                .map(HasResourceProperty::getResource)
                .flatMap(r -> r.getInnerVars().stream())
                .forEach(innerVars::remove);

        if (innerVars.stream().anyMatch(VarPatternImpl::invalidInnerVariable)) {
            throw new UnsupportedOperationException("Graql strings cannot represent a query with inner variables");
        }

        StringBuilder builder = new StringBuilder();

        String name = this.name.isUserDefinedName() ? getPrintableName() : "";

        builder.append(name);

        if (this.name.isUserDefinedName() && !properties.isEmpty()) {
            // Add a space after the var name
            builder.append(" ");
        }

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

    private VarPattern addCasting(RelationPlayer relationPlayer) {
        Optional<RelationProperty> relationProperty = getProperty(RelationProperty.class);

        Stream<RelationPlayer> oldCastings = relationProperty
                .map(RelationProperty::getRelationPlayers)
                .orElse(Stream.empty());

        ImmutableMultiset<RelationPlayer> relationPlayers =
                Stream.concat(oldCastings, Stream.of(relationPlayer)).collect(toImmutableMultiset());

        RelationProperty newProperty = new RelationProperty(relationPlayers);

        return relationProperty.map(this::removeProperty).orElse(this).addProperty(newProperty);
    }

    private static boolean invalidInnerVariable(VarPatternAdmin var) {
        return var.getProperties().anyMatch(p -> !(p instanceof LabelProperty));
    }

    private VarPatternImpl addProperty(VarProperty property) {
        if (property.isUnique()) {
            testUniqueProperty((UniqueVarProperty) property);
        }
        return new VarPatternImpl(name, Sets.union(properties, ImmutableSet.of(property)));
    }

    private VarPatternImpl removeProperty(VarProperty property) {
        return new VarPatternImpl(name, Sets.difference(properties, ImmutableSet.of(property)));
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
    public Disjunction<Conjunction<VarPatternAdmin>> getDisjunctiveNormalForm() {
        // a disjunction containing only one option
        Conjunction<VarPatternAdmin> conjunction = Patterns.conjunction(Collections.singleton(this));
        return Patterns.disjunction(Collections.singleton(conjunction));
    }

    @Override
    public Set<Var> commonVarNames() {
        return getInnerVars().stream()
                .filter(v -> v.getVarName().isUserDefinedName())
                .map(VarPatternAdmin::getVarName)
                .collect(toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarPatternImpl var = (VarPatternImpl) o;

        if (name.isUserDefinedName() != var.name.isUserDefinedName()) return false;

        // "simplifying" this makes it harder to read
        //noinspection SimplifiableIfStatement
        if (!properties.equals(var.properties)) return false;

        return !name.isUserDefinedName() || name.equals(var.name);

    }

    @Override
    public int hashCode() {
        int result = properties.hashCode();
        if (name.isUserDefinedName()) result = 31 * result + name.hashCode();
        result = 31 * result + (name.isUserDefinedName() ? 1 : 0);
        return result;
    }

}
