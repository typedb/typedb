/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.executor.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.planning.gremlin.value.ValueAssignment;
import grakn.core.graql.planning.gremlin.value.ValueOperation;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Set;

public class ValueExecutor  implements PropertyExecutor.Insertable {

    private final Variable var;
    private final ValueProperty property;
    private final ValueOperation<?, ?> operation;

    ValueExecutor(Variable var, ValueProperty property) {
        this.var = var;
        this.property = property;
        this.operation = ValueOperation.of(property.operation());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return ImmutableSet.of(
                EquivalentFragmentSets.notInternalFragmentSet(property, var),
                EquivalentFragmentSets.value(property, var, operation)
        );
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertValue());
    }

    class InsertValue implements PropertyExecutor.Writer {

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
            if (!(operation instanceof ValueAssignment)) {
                throw GraqlSemanticException.insertPredicate();
            } else {
                executor.getBuilder(var).value(operation.value());
            }
        }
    }
}
