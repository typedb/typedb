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

package grakn.core.graql.query.pattern.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.query.pattern.Variable;

import java.util.Arrays;

// TODO: Add an example of 'undefine' to this description

/**
 * A class describing an operation to perform using a VarProperty.
 * The behaviour is executed via a WriteExecutor using #execute. The class also
 * report its #requiredVars before it can run and its #producedVars(), that will be available to
 * other PropertyExecutors after it has run.
 * For example:
 * SubProperty property = SubProperty.of(y);
 * PropertyExecutor executor = property.define(x);
 * executor.requiredVars(); // returns `{y}`
 * executor.producedVars(); // returns `{x}`
 * // apply the `sub` property between `x` and `y`
 * // because it requires `y`, it will call `writeExecutor.get(y)`
 * // because it produces `x`, it will call `writeExecutor.builder(x)`
 * executor.execute(writeExecutor);
 */
public class PropertyExecutor {

    private final ImmutableSet<Variable> requiredVars;
    private final ImmutableSet<Variable> producedVars;
    private final PropertyExecutor.Method executeMethod;

    private PropertyExecutor(
            ImmutableSet<Variable> requiredVars,
            ImmutableSet<Variable> producedVars,
            PropertyExecutor.Method executeMethod) {
        this.requiredVars = requiredVars;
        this.producedVars = producedVars;
        this.executeMethod = executeMethod;
    }

    public static PropertyExecutor.Builder builder(Method executeMethod) {
        return builder().executeMethod(executeMethod);
    }

    private static PropertyExecutor.Builder builder() {
        return new PropertyExecutor.Builder();
    }

    /**
     * Apply the given property, if possible.
     *
     * @param executor a class providing a map of concepts that are accessible and methods to build new concepts.
     *                 This method can expect any key to be here that is returned from
     *                 #requiredVars(). The method may also build a concept provided that key is returned
     *                 from #producedVars().
     */
    public final void execute(WriteExecutor executor) {
        executeMethod().execute(executor);
    }

    /**
     * Get all Variables whose Concept must exist for the subject Variable to be applied.
     * For example, for IsaProperty the type must already be present before an instance can be created.
     * When calling #execute, the method can expect any Variable returned here to be available by calling
     * WriteExecutor#get.
     */
    public ImmutableSet<Variable> requiredVars() {
        return requiredVars;
    }

    /**
     * Get all Variables whose Concept can only be created after this property is applied.
     * When calling #execute, the method must help build a Concept for every Variable returned
     * from this method, using WriteExecutor#builder.
     */
    public ImmutableSet<Variable> producedVars() {
        return producedVars;
    }

    private PropertyExecutor.Method executeMethod() {
        return executeMethod;
    }

    @Override
    public String toString() {
        return "PropertyExecutor{"
                + "requiredVars=" + requiredVars + ", "
                + "producedVars=" + producedVars + ", "
                + "executeMethod=" + executeMethod
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof PropertyExecutor) {
            PropertyExecutor that = (PropertyExecutor) o;
            return (this.requiredVars.equals(that.requiredVars()))
                    && (this.producedVars.equals(that.producedVars()))
                    && (this.executeMethod.equals(that.executeMethod()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.requiredVars.hashCode();
        h *= 1000003;
        h ^= this.producedVars.hashCode();
        h *= 1000003;
        h ^= this.executeMethod.hashCode();
        return h;
    }

    static class Builder {

        private ImmutableSet.Builder<Variable> requiredVarsBuilder;
        private ImmutableSet<Variable> requiredVars;
        private ImmutableSet.Builder<Variable> producedVarsBuilder;
        private ImmutableSet<Variable> producedVars;
        private PropertyExecutor.Method executeMethod;

        Builder() {}

        public Builder requires(Variable... values) {
            return requires(Arrays.asList(values));
        }

        public Builder requires(Iterable<Variable> values) {
            if (requiredVarsBuilder == null) {
                requiredVarsBuilder = ImmutableSet.builder();
            }
            requiredVarsBuilder.addAll(values);
            return this;
        }

        public Builder produces(Variable... values) {
            if (producedVarsBuilder == null) {
                producedVarsBuilder = ImmutableSet.builder();
            }
            producedVarsBuilder.add(values);
            return this;
        }

        PropertyExecutor.Builder executeMethod(PropertyExecutor.Method executeMethod) {
            if (executeMethod == null) {
                throw new NullPointerException("Null executeMethod");
            }
            this.executeMethod = executeMethod;
            return this;
        }

        PropertyExecutor build() {
            if (requiredVarsBuilder != null) {
                this.requiredVars = requiredVarsBuilder.build();
            } else if (this.requiredVars == null) {
                this.requiredVars = ImmutableSet.of();
            }
            if (producedVarsBuilder != null) {
                this.producedVars = producedVarsBuilder.build();
            } else if (this.producedVars == null) {
                this.producedVars = ImmutableSet.of();
            }
            String missing = "";
            if (this.executeMethod == null) {
                missing += " executeMethod";
            }
            if (!missing.isEmpty()) {
                throw new IllegalStateException("Missing required properties:" + missing);
            }
            return new PropertyExecutor(
                    this.requiredVars,
                    this.producedVars,
                    this.executeMethod);
        }
    }

    @FunctionalInterface
    interface Method {
        void execute(WriteExecutor writeExecutor);
    }
}
