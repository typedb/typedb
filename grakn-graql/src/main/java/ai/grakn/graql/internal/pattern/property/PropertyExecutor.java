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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.Var;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.function.Consumer;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class PropertyExecutor {

    public static PropertyExecutor.Builder builder(Consumer<InsertQueryExecutor> executeMethod) {
        return new AutoValue_PropertyExecutor.Builder().executeMethod(executeMethod);
    }

    abstract Consumer<InsertQueryExecutor> executeMethod();

    public void execute(InsertQueryExecutor executor) {
        executeMethod().accept(executor);
    }

    abstract ImmutableSet<Var> requiredVars();

    abstract ImmutableSet<Var> producedVars();

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder executeMethod(Consumer<InsertQueryExecutor> value);

        abstract ImmutableSet.Builder<Var> requiredVarsBuilder();

        public Builder requires(Var... values) {
            requiredVarsBuilder().add(values);
            return this;
        }

        public Builder requires(Iterable<Var> values) {
            requiredVarsBuilder().addAll(values);
            return this;
        }

        abstract ImmutableSet.Builder<Var> producedVarsBuilder();

        public Builder produces(Var... values) {
            producedVarsBuilder().add(values);
            return this;
        }

        public Builder produces(Iterable<Var> values) {
            producedVarsBuilder().addAll(values);
            return this;
        }

        abstract PropertyExecutor build();
    }
}
