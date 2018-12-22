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
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;

import java.util.Set;

public class WhenExecutor implements PropertyExecutor.Definable {

    private final Variable var;
    private final WhenProperty property;

    public WhenExecutor(Variable var, WhenProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineWhen());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        // TODO: Fix this so that Undefining WhenProperty behaves symmetrically to Defining WhenProperty
        throw GraqlQueryException.defineUnsupportedProperty(property.name());
    }

    private class DefineWhen implements PropertyExecutor.Writer {

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
            // This allows users to skip stating `$ruleVar sub rule` when they say `$ruleVar when { ... }`
            executor.getBuilder(var).isRule().when(property.pattern());
        }
    }
}
