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
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * Default implementation of {@link Var}.
 *
 * @author Felix Chapman
 */
final class VarImpl implements Var, VarPatternAdmin {

    private final String value;

    private final boolean userDefinedName;

    VarImpl(String value, boolean userDefinedName) {
        this.value = value;
        this.userDefinedName = userDefinedName;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Var map(Function<String, String> mapper) {
        return Graql.var(mapper.apply(value));
    }

    @Override
    public boolean isUserDefinedName() {
        return userDefinedName;
    }

    @Override
    public Var asUserDefined() {
        if (userDefinedName) {
            return this;
        } else {
            return new VarImpl(value, true);
        }
    }

    @Override
    public VarPattern pattern() {
        return new VarPatternImpl(this, ImmutableSet.of());
    }

    @Override
    public String shortName() {
        return "$" + StringUtils.left(value, 3);
    }

    @Override
    public String toString() {
        return "$" + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarImpl varName = (VarImpl) o;

        return value.equals(varName.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public VarPattern id(ConceptId id) {
        return pattern().id(id);
    }

    @Override
    public VarPattern label(String label) {
        return pattern().label(label);
    }

    @Override
    public VarPattern label(TypeLabel label) {
        return pattern().label(label);
    }

    @Override
    public VarPattern val(Object value) {
        return pattern().val(value);
    }

    @Override
    public VarPattern val(ValuePredicate predicate) {
        return pattern().val(predicate);
    }

    @Override
    public VarPattern has(String type, Object value) {
        return pattern().has(type, value);
    }

    @Override
    public VarPattern has(String type, ValuePredicate predicate) {
        return pattern().has(type, predicate);
    }

    @Override
    public VarPattern has(String type, VarPatternBuilder varPattern) {
        return pattern().has(type, varPattern);
    }

    @Override
    public VarPattern has(TypeLabel type, VarPatternBuilder varPattern) {
        return pattern().has(type, varPattern);
    }

    @Override
    public VarPattern isa(String type) {
        return pattern().isa(type);
    }

    @Override
    public VarPattern isa(VarPatternBuilder type) {
        return pattern().isa(type);
    }

    @Override
    public VarPattern sub(String type) {
        return pattern().sub(type);
    }

    @Override
    public VarPattern sub(VarPatternBuilder type) {
        return pattern().sub(type);
    }

    @Override
    public VarPattern relates(String type) {
        return pattern().relates(type);
    }

    @Override
    public VarPattern relates(VarPatternBuilder type) {
        return pattern().relates(type);
    }

    @Override
    public VarPattern plays(String type) {
        return pattern().plays(type);
    }

    @Override
    public VarPattern plays(VarPatternBuilder type) {
        return pattern().plays(type);
    }

    @Override
    public VarPattern hasScope(VarPatternBuilder type) {
        return pattern().hasScope(type);
    }

    @Override
    public VarPattern has(String type) {
        return pattern().has(type);
    }

    @Override
    public VarPattern has(VarPatternBuilder type) {
        return pattern().has(type);
    }

    @Override
    public VarPattern key(String type) {
        return pattern().key(type);
    }

    @Override
    public VarPattern key(VarPatternBuilder type) {
        return pattern().key(type);
    }

    @Override
    public VarPattern rel(String roleplayer) {
        return pattern().rel(roleplayer);
    }

    @Override
    public VarPattern rel(VarPatternBuilder roleplayer) {
        return pattern().rel(roleplayer);
    }

    @Override
    public VarPattern rel(String roletype, String roleplayer) {
        return pattern().rel(roletype, roleplayer);
    }

    @Override
    public VarPattern rel(VarPatternBuilder roletype, String roleplayer) {
        return pattern().rel(roletype, roleplayer);
    }

    @Override
    public VarPattern rel(String roletype, VarPatternBuilder roleplayer) {
        return pattern().rel(roletype, roleplayer);
    }

    @Override
    public VarPattern rel(VarPatternBuilder roletype, VarPatternBuilder roleplayer) {
        return pattern().rel(roletype, roleplayer);
    }

    @Override
    public VarPattern isAbstract() {
        return pattern().isAbstract();
    }

    @Override
    public VarPattern datatype(ResourceType.DataType<?> datatype) {
        return pattern().datatype(datatype);
    }

    @Override
    public VarPattern regex(String regex) {
        return pattern().regex(regex);
    }

    @Override
    public VarPattern lhs(Pattern lhs) {
        return pattern().lhs(lhs);
    }

    @Override
    public VarPattern rhs(Pattern rhs) {
        return pattern().rhs(rhs);
    }

    @Override
    public VarPattern neq(String var) {
        return pattern().neq(var);
    }

    @Override
    public VarPattern neq(VarPatternBuilder varPattern) {
        return pattern().neq(varPattern);
    }

    @Override
    public Var getVarName() {
        return this;
    }

    @Override
    public VarPatternAdmin setVarName(Var name) {
        return (VarPatternAdmin) name;
    }

    @Override
    public Stream<VarProperty> getProperties() {
        return Stream.of();
    }

    @Override
    public <T extends VarProperty> Stream<T> getProperties(Class<T> type) {
        return Stream.of();
    }

    @Override
    public <T extends UniqueVarProperty> Optional<T> getProperty(Class<T> type) {
        return Optional.empty();
    }

    @Override
    public <T extends VarProperty> boolean hasProperty(Class<T> type) {
        return false;
    }

    @Override
    public <T extends VarProperty> VarPatternAdmin mapProperty(Class<T> type, UnaryOperator<T> mapper) {
        return this;
    }

    @Override
    public Optional<ConceptId> getId() {
        return Optional.empty();
    }

    @Override
    public Optional<TypeLabel> getTypeLabel() {
        return Optional.empty();
    }

    @Override
    public Collection<VarPatternAdmin> getInnerVars() {
        return ImmutableSet.of(this);
    }

    @Override
    public Collection<VarPatternAdmin> getImplicitInnerVars() {
        return ImmutableSet.of(this);
    }

    @Override
    public Set<TypeLabel> getTypeLabels() {
        return ImmutableSet.of();
    }

    @Override
    public String getPrintableName() {
        return toString();
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
    public VarPatternAdmin admin() {
        return this;
    }
}
