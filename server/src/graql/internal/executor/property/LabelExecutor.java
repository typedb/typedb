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

package grakn.core.graql.internal.executor.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Set;

public class LabelExecutor implements PropertyExecutor.Definable, PropertyExecutor.Insertable, PropertyExecutor.Matchable {

    private final Variable var;
    private final LabelProperty property;

    public LabelExecutor(Variable var, LabelProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new LookupLabel());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new LookupLabel());
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new LookupLabel());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return ImmutableSet.of(EquivalentFragmentSets.label(property, var, ImmutableSet.of(property.label())));
    }

    // The WriteExecutor for IdExecutor works for Define, Undefine and Insert queries,
    // because it is used for look-ups of a concept
    private class LookupLabel implements PropertyExecutor.Writer {

        @Override
        public Variable var() {
            return var;
        }

        @Override
        public VarProperty property() {
            return property;
        }

        @Override
        public Set<Variable> requiredVars() {
            return ImmutableSet.of();
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of(var);
        }

        @Override
        public void execute(WriteExecutor executor) {
            executor.getBuilder(var).label(property.label());
        }
    }
}
