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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.reasoner.atom.property.RegexAtom;
import ai.grakn.util.StringUtil;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code regex} property on a {@link AttributeType}.
 *
 * This property can be queried and inserted.
 *
 * This property introduces a validation constraint on instances of this {@link AttributeType}, stating that their
 * values must conform to the given regular expression.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class RegexProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    public static RegexProperty of(String regex) {
        return new AutoValue_RegexProperty(regex);
    }

    public abstract String regex();

    @Override
    public String getName() {
        return "regex";
    }

    @Override
    public String getProperty() {
        return StringUtil.valueToString(regex());
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.regex(this, start, regex()));
    }

    @Override
    public PropertyExecutor define(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.get(var).asAttributeType().setRegex(regex());
        };

        return PropertyExecutor.builder(method).requires(var).build();
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return new RegexAtom(var.var(), this, parent);
    }
}
