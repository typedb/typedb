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

package grakn.core.graql.internal.pattern.property;

import grakn.core.concept.Concept;
import grakn.core.graql.Var;
import grakn.core.graql.admin.VarProperty;
import grakn.core.graql.internal.query.executor.QueryOperationExecutor;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

// TODO: Add an example of 'undefine' to this description
/**
 * A class describing an operation to perform using a {@link VarProperty}.
 *
 * <p>
 *     The behaviour is executed via a {@link QueryOperationExecutor} using {@link #execute}. The class also
 *     report its {@link #requiredVars} before it can run and its {@link #producedVars()}, that will be available to
 *     other {@link PropertyExecutor}s after it has run.
 * </p>
 * <p>
 *     For example:
 *     <pre>
 *         SubProperty property = SubProperty.of(y);
 *         PropertyExecutor executor = property.define(x);
 *         executor.requiredVars(); // returns `{y}`
 *         executor.producedVars(); // returns `{x}`
 *
 *         // apply the `sub` property between `x` and `y`
 *         // because it requires `y`, it will call `queryOperationExecutor.get(y)`
 *         // because it produces `x`, it will call `queryOperationExecutor.builder(x)`
 *         executor.execute(queryOperationExecutor);
 *     </pre>
 * </p>
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class PropertyExecutor {

    public static PropertyExecutor.Builder builder(Method executeMethod) {
        return builder().executeMethod(executeMethod);
    }

    private static PropertyExecutor.Builder builder() {
        return new AutoValue_PropertyExecutor.Builder();
    }

    /**
     * Apply the given property, if possible.
     *
     * @param executor a class providing a map of concepts that are accessible and methods to build new concepts.
     *                 <p>
     *                 This method can expect any key to be here that is returned from
     *                 {@link #requiredVars()}. The method may also build a concept provided that key is returned
     *                 from {@link #producedVars()}.
     *                 </p>
     */
    public final void execute(QueryOperationExecutor executor) {
        executeMethod().execute(executor);
    }

    /**
     * Get all {@link Var}s whose {@link Concept} must exist for the subject {@link Var} to be applied.
     * For example, for {@link IsaProperty} the type must already be present before an instance can be created.
     *
     * <p>
     *     When calling {@link #execute}, the method can expect any {@link Var} returned here to be available by calling
     *     {@link QueryOperationExecutor#get}.
     * </p>
     */
    public abstract ImmutableSet<Var> requiredVars();

    /**
     * Get all {@link Var}s whose {@link Concept} can only be created after this property is applied.
     *
     * <p>
     *     When calling {@link #execute}, the method must help build a {@link Concept} for every {@link Var} returned
     *     from this method, using {@link QueryOperationExecutor#builder}.
     * </p>
     */
    public abstract ImmutableSet<Var> producedVars();

    abstract Method executeMethod();

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder executeMethod(Method value);

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

        abstract PropertyExecutor build();
    }

    @FunctionalInterface
    interface Method {
        void execute(QueryOperationExecutor queryOperationExecutor);
    }
}
