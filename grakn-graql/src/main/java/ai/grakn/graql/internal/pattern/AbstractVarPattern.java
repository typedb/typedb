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
 *
 */

package ai.grakn.graql.internal.pattern;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
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
import ai.grakn.graql.internal.pattern.property.WhenProperty;
import ai.grakn.graql.internal.pattern.property.NeqProperty;
import ai.grakn.graql.internal.pattern.property.PlaysProperty;
import ai.grakn.graql.internal.pattern.property.RegexProperty;
import ai.grakn.graql.internal.pattern.property.RelatesProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.pattern.property.ThenProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.CommonUtil;
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
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * Abstract implementation of {@link VarPatternAdmin}.
 *
 * @author Felix Chapman
 */
public abstract class AbstractVarPattern extends AbstractPattern implements VarPatternAdmin {

    @Override
    public abstract Var getVarName();

    protected abstract Set<VarProperty> properties();

    @Override
    public final VarPatternAdmin admin() {
        return this;
    }

    @Override
    public final Optional<Label> getTypeLabel() {
        return getProperty(LabelProperty.class).map(LabelProperty::getLabelValue);
    }

    @Override
    public final <T extends VarProperty> Stream<T> getProperties(Class<T> type) {
        return getProperties().filter(type::isInstance).map(type::cast);
    }

    @Override
    public final <T extends UniqueVarProperty> Optional<T> getProperty(Class<T> type) {
        return getProperties().filter(type::isInstance).map(type::cast).findAny();
    }

    @Override
    public final <T extends VarProperty> boolean hasProperty(Class<T> type) {
        return getProperties(type).findAny().isPresent();
    }

    @Override
    public final Collection<VarPatternAdmin> getInnerVars() {
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
    public final Collection<VarPatternAdmin> getImplicitInnerVars() {
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
    public final Set<Label> getTypeLabels() {
        return getProperties()
                .flatMap(VarProperty::getTypes)
                .map(VarPatternAdmin::getTypeLabel).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());
    }

    @Override
    public final Disjunction<Conjunction<VarPatternAdmin>> getDisjunctiveNormalForm() {
        // a disjunction containing only one option
        Conjunction<VarPatternAdmin> conjunction = Patterns.conjunction(Collections.singleton(this));
        return Patterns.disjunction(Collections.singleton(conjunction));
    }

    @Override
    public final Set<Var> commonVarNames() {
        return getInnerVars().stream()
                .filter(v -> v.getVarName().isUserDefinedName())
                .map(VarPatternAdmin::getVarName)
                .collect(toSet());
    }

    @Override
    public final VarPattern id(ConceptId id) {
        return addProperty(new IdProperty(id));
    }

    @Override
    public final VarPattern label(String label) {
        return label(Label.of(label));
    }

    @Override
    public final VarPattern label(Label label) {
        return addProperty(new LabelProperty(label));
    }

    @Override
    public final VarPattern val(Object value) {
        return val(Graql.eq(value));
    }

    @Override
    public final VarPattern val(ValuePredicate predicate) {
        return addProperty(new ValueProperty(predicate.admin()));
    }

    @Override
    public final VarPattern has(String type, Object value) {
        return has(type, Graql.eq(value));
    }

    @Override
    public final VarPattern has(String type, ValuePredicate predicate) {
        return has(type, Graql.var().val(predicate));
    }

    @Override
    public final VarPattern has(String type, VarPattern varPattern) {
        return has(Label.of(type), varPattern);
    }

    @Override
    public final VarPattern has(Label type, VarPattern varPattern) {
        return addProperty(HasResourceProperty.of(type, varPattern.admin()));
    }

    @Override
    public final VarPattern isa(String type) {
        return isa(Graql.label(type));
    }

    @Override
    public final VarPattern isa(VarPattern type) {
        return addProperty(new IsaProperty(type.admin()));
    }

    @Override
    public final VarPattern sub(String type) {
        return sub(Graql.label(type));
    }

    @Override
    public final VarPattern sub(VarPattern type) {
        return addProperty(new SubProperty(type.admin()));
    }

    @Override
    public final VarPattern relates(String type) {
        return relates(Graql.label(type));
    }

    @Override
    public final VarPattern relates(VarPattern type) {
        return addProperty(new RelatesProperty(type.admin()));
    }

    @Override
    public final VarPattern plays(String type) {
        return plays(Graql.label(type));
    }

    @Override
    public final VarPattern plays(VarPattern type) {
        return addProperty(new PlaysProperty(type.admin(), false));
    }

    @Override
    public final VarPattern hasScope(VarPattern type) {
        return addProperty(new HasScopeProperty(type.admin()));
    }

    @Override
    public final VarPattern has(String type) {
        return has(Graql.label(type));
    }

    @Override
    public final VarPattern has(VarPattern type) {
        return addProperty(new HasResourceTypeProperty(type.admin(), false));
    }

    @Override
    public final VarPattern key(String type) {
        return key(Graql.var().label(type));
    }

    @Override
    public final VarPattern key(VarPattern type) {
        return addProperty(new HasResourceTypeProperty(type.admin(), true));
    }

    @Override
    public final VarPattern rel(String roleplayer) {
        return rel(Graql.var(roleplayer));
    }

    @Override
    public final VarPattern rel(VarPattern roleplayer) {
        return addCasting(RelationPlayer.of(roleplayer.admin()));
    }

    @Override
    public final VarPattern rel(String role, String roleplayer) {
        return rel(Graql.label(role), Graql.var(roleplayer));
    }

    @Override
    public final VarPattern rel(VarPattern role, String roleplayer) {
        return rel(role, Graql.var(roleplayer));
    }

    @Override
    public final VarPattern rel(String role, VarPattern roleplayer) {
        return rel(Graql.label(role), roleplayer);
    }

    @Override
    public final VarPattern rel(VarPattern role, VarPattern roleplayer) {
        return addCasting(RelationPlayer.of(role.admin(), roleplayer.admin()));
    }

    @Override
    public final VarPattern isAbstract() {
        return addProperty(new IsAbstractProperty());
    }

    @Override
    public final VarPattern datatype(ResourceType.DataType<?> datatype) {
        return addProperty(new DataTypeProperty(requireNonNull(datatype)));
    }

    @Override
    public final VarPattern regex(String regex) {
        return addProperty(new RegexProperty(regex));
    }

    @Override
    public final VarPattern when(Pattern when) {
        return addProperty(new WhenProperty(when));
    }

    @Override
    public final VarPattern then(Pattern then) {
        return addProperty(new ThenProperty(then));
    }

    @Override
    public final VarPattern neq(String var) {
        return neq(Graql.var(var));
    }

    @Override
    public final VarPattern neq(VarPattern varPattern) {
        return addProperty(new NeqProperty(varPattern.admin()));
    }

    @Override
    public final String getPrintableName() {
        if (properties().size() == 0) {
            // If there are no properties, we display the variable name
            return getVarName().toString();
        } else if (properties().size() == 1) {
            // If there is only a label, we display that
            Optional<Label> label = getTypeLabel();
            if (label.isPresent()) {
                return StringConverter.typeLabelToString(label.get());
            }
        }

        // Otherwise, we print the entire pattern
        return "`" + toString() + "`";
    }

    @Override
    public final Stream<VarProperty> getProperties() {
        return properties().stream();
    }

    private VarPattern addCasting(RelationPlayer relationPlayer) {
        Optional<RelationProperty> relationProperty = getProperty(RelationProperty.class);

        Stream<RelationPlayer> oldCastings = relationProperty
                .map(RelationProperty::getRelationPlayers)
                .orElse(Stream.empty());

        ImmutableMultiset<RelationPlayer> relationPlayers =
                Stream.concat(oldCastings, Stream.of(relationPlayer)).collect(CommonUtil.toImmutableMultiset());

        RelationProperty newProperty = new RelationProperty(relationPlayers);

        return relationProperty.map(this::removeProperty).orElse(this).addProperty(newProperty);
    }

    private VarPatternAdmin addProperty(VarProperty property) {
        if (property.isUnique()) {
            testUniqueProperty((UniqueVarProperty) property);
        }
        return Patterns.varPattern(getVarName(), Sets.union(properties(), ImmutableSet.of(property)));
    }

    private AbstractVarPattern removeProperty(VarProperty property) {
        return (AbstractVarPattern) Patterns.varPattern(getVarName(), Sets.difference(properties(), ImmutableSet.of(property)));
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
