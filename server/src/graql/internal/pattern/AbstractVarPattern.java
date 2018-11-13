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

package grakn.core.graql.internal.pattern;

import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.Graql;
import grakn.core.graql.Pattern;
import grakn.core.graql.ValuePredicate;
import grakn.core.graql.Var;
import grakn.core.graql.VarPattern;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.Disjunction;
import grakn.core.graql.admin.RelationPlayer;
import grakn.core.graql.admin.UniqueVarProperty;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.admin.VarProperty;
import grakn.core.graql.internal.pattern.property.DataTypeProperty;
import grakn.core.graql.internal.pattern.property.IsaExplicitProperty;
import grakn.core.graql.internal.pattern.property.HasAttributeProperty;
import grakn.core.graql.internal.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.internal.pattern.property.IdProperty;
import grakn.core.graql.internal.pattern.property.IsAbstractProperty;
import grakn.core.graql.internal.pattern.property.IsaProperty;
import grakn.core.graql.internal.pattern.property.LabelProperty;
import grakn.core.graql.internal.pattern.property.NeqProperty;
import grakn.core.graql.internal.pattern.property.PlaysProperty;
import grakn.core.graql.internal.pattern.property.RegexProperty;
import grakn.core.graql.internal.pattern.property.RelatesProperty;
import grakn.core.graql.internal.pattern.property.RelationshipProperty;
import grakn.core.graql.internal.pattern.property.SubExplicitProperty;
import grakn.core.graql.internal.pattern.property.SubProperty;
import grakn.core.graql.internal.pattern.property.ThenProperty;
import grakn.core.graql.internal.pattern.property.ValueProperty;
import grakn.core.graql.internal.pattern.property.WhenProperty;
import grakn.core.graql.internal.util.StringConverter;
import grakn.core.util.CommonUtil;
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

import static java.util.stream.Collectors.toSet;

/**
 * Abstract implementation of {@link VarPatternAdmin}.
 *
 */
public abstract class AbstractVarPattern extends AbstractPattern implements VarPatternAdmin {

    @Override
    public abstract Var var();

    protected abstract Set<VarProperty> properties();

    @Override
    public final VarPatternAdmin admin() {
        return this;
    }

    @Override
    public final Optional<Label> getTypeLabel() {
        return getProperty(LabelProperty.class).map(LabelProperty::label);
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
    public final Collection<VarPatternAdmin> innerVarPatterns() {
        Stack<VarPatternAdmin> newVars = new Stack<>();
        List<VarPatternAdmin> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarPatternAdmin var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::innerVarPatterns).forEach(newVars::add);
        }

        return vars;
    }

    @Override
    public final Collection<VarPatternAdmin> implicitInnerVarPatterns() {
        Stack<VarPatternAdmin> newVars = new Stack<>();
        List<VarPatternAdmin> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            VarPatternAdmin var = newVars.pop();
            vars.add(var);
            var.getProperties().flatMap(VarProperty::implicitInnerVarPatterns).forEach(newVars::add);
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
    public final Set<Var> commonVars() {
        return innerVarPatterns().stream()
                .filter(v -> v.var().isUserDefinedName())
                .map(VarPatternAdmin::var)
                .collect(toSet());
    }

    @Override
    public final VarPattern id(ConceptId id) {
        return addProperty(IdProperty.of(id));
    }

    @Override
    public final VarPattern label(String label) {
        return label(Label.of(label));
    }

    @Override
    public final VarPattern label(Label label) {
        return addProperty(LabelProperty.of(label));
    }

    @Override
    public final VarPattern val(Object value) {
        return val(Graql.eq(value));
    }

    @Override
    public final VarPattern val(ValuePredicate predicate) {
        return addProperty(ValueProperty.of(predicate));
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
    public final VarPattern has(String type, VarPattern attribute) {
        return has(Label.of(type), attribute);
    }

    @Override
    public final VarPattern has(Label type, VarPattern attribute) {
        return has(type, attribute, Graql.var());
    }

    @Override
    public final VarPattern has(Label type, VarPattern attribute, VarPattern relationship) {
        return addProperty(HasAttributeProperty.of(type, attribute.admin(), relationship.admin()));
    }

    @Override
    public final VarPattern isaExplicit(String type) {
        return isaExplicit(Graql.label(type));
    }

    @Override
    public final VarPattern isaExplicit(VarPattern type) {
        return addProperty(IsaExplicitProperty.of(type.admin()));
    }

    @Override
    public final VarPattern isa(String type) {
        return isa(Graql.label(type));
    }

    @Override
    public final VarPattern isa(VarPattern type) {
        return addProperty(IsaProperty.of(type.admin()));
    }

    @Override
    public final VarPattern sub(String type) {
        return sub(Graql.label(type));
    }

    @Override
    public final VarPattern sub(VarPattern type) {
        return addProperty(SubProperty.of(type.admin()));
    }

    @Override
    public final VarPattern subExplicit(String type) {
        return subExplicit(Graql.label(type));
    }

    @Override
    public final VarPattern subExplicit(VarPattern type) {
        return addProperty(SubExplicitProperty.of(type.admin()));
    }

    @Override
    public final VarPattern relates(String type) {
        return relates(type, null);
    }

    @Override
    public final VarPattern relates(VarPattern type) {
        return relates(type, null);
    }

    @Override
    public VarPattern relates(String roleType, String superRoleType) {
        return relates(Graql.label(roleType), superRoleType == null ? null : Graql.label(superRoleType));
    }

    @Override
    public VarPattern relates(VarPattern roleType, VarPattern superRoleType) {
        return addProperty(RelatesProperty.of(roleType.admin(), superRoleType == null ? null : superRoleType.admin()));
    }

    @Override
    public final VarPattern plays(String type) {
        return plays(Graql.label(type));
    }

    @Override
    public final VarPattern plays(VarPattern type) {
        return addProperty(PlaysProperty.of(type.admin(), false));
    }

    @Override
    public final VarPattern has(String type) {
        return has(Graql.label(type));
    }

    @Override
    public final VarPattern has(VarPattern type) {
        return addProperty(HasAttributeTypeProperty.of(type.admin(), false));
    }

    @Override
    public final VarPattern key(String type) {
        return key(Graql.var().label(type));
    }

    @Override
    public final VarPattern key(VarPattern type) {
        return addProperty(HasAttributeTypeProperty.of(type.admin(), true));
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
        return addProperty(IsAbstractProperty.get());
    }

    @Override
    public final VarPattern datatype(AttributeType.DataType<?> datatype) {
        return addProperty(DataTypeProperty.of(datatype));
    }

    @Override
    public final VarPattern regex(String regex) {
        return addProperty(RegexProperty.of(regex));
    }

    @Override
    public final VarPattern when(Pattern when) {
        return addProperty(WhenProperty.of(when));
    }

    @Override
    public final VarPattern then(Pattern then) {
        return addProperty(ThenProperty.of(then));
    }

    @Override
    public final VarPattern neq(String var) {
        return neq(Graql.var(var));
    }

    @Override
    public final VarPattern neq(VarPattern varPattern) {
        return addProperty(NeqProperty.of(varPattern.admin()));
    }

    @Override
    public final String getPrintableName() {
        if (properties().size() == 0) {
            // If there are no properties, we display the variable name
            return var().toString();
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
        Optional<RelationshipProperty> relationProperty = getProperty(RelationshipProperty.class);

        ImmutableMultiset<RelationPlayer> oldCastings = relationProperty
                .map(RelationshipProperty::relationPlayers)
                .orElse(ImmutableMultiset.of());

        ImmutableMultiset<RelationPlayer> relationPlayers =
                Stream.concat(oldCastings.stream(), Stream.of(relationPlayer)).collect(CommonUtil.toImmutableMultiset());

        RelationshipProperty newProperty = RelationshipProperty.of(relationPlayers);

        return relationProperty.map(this::removeProperty).orElse(this).addProperty(newProperty);
    }

    private VarPatternAdmin addProperty(VarProperty property) {
        if (property.isUnique()) {
            testUniqueProperty((UniqueVarProperty) property);
        }
        return Patterns.varPattern(var(), Sets.union(properties(), ImmutableSet.of(property)));
    }

    private AbstractVarPattern removeProperty(VarProperty property) {
        return (AbstractVarPattern) Patterns.varPattern(var(), Sets.difference(properties(), ImmutableSet.of(property)));
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
