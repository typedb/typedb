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

import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.Set;

public class IdExecutor implements PropertyExecutor.Definable, PropertyExecutor.Insertable {

    private final Variable var;
    private final IdProperty property;

    public IdExecutor(Variable var, IdProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new LookupId()));
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new LookupId()));
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new LookupId()));
    }

    // The WriteExecutor for IdExecutor works for Define, Undefine and Insert queries,
    // because it is used for look-ups of a concept
    private class LookupId implements PropertyExecutor.Writer {

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
            return Collections.unmodifiableSet(Collections.emptySet());
        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.singleton(var));
        }

        @Override
        public void execute(WriteExecutor executor) {
            executor.getBuilder(var).id(property.id());
        }
    }
}
