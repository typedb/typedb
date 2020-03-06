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
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.property.VarProperty;
import graql.lang.property.WhenProperty;
import graql.lang.statement.Variable;

import java.util.Set;

public class WhenExecutor  implements PropertyExecutor.Definable {

    private final Variable var;
    private final WhenProperty property;

    WhenExecutor(Variable var, WhenProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return null;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineWhen());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        // TODO: Fix this so that Undefining WhenProperty behaves symmetrically to Defining WhenProperty
        throw GraqlSemanticException.defineUnsupportedProperty(property.keyword());
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
