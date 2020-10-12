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

package grakn.core.pattern.variable;

import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.core.common.exception.GraknException;
import grakn.core.pattern.Pattern;
import grakn.core.pattern.constraint.Constraint;
import graql.lang.pattern.variable.Reference;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Query.INVALID_CASTING;

public abstract class Variable implements Pattern {

    private static final String TRACE_PREFIX = "variable.";
    final Identifier identifier;

    Variable(final Identifier identifier) {
        this.identifier = identifier;
    }

    public static Set<TypeVariable> createFromTypes(final List<graql.lang.pattern.variable.TypeVariable> variables) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "types")) {
            final VariableRegistry registry = new VariableRegistry();
            variables.forEach(registry::register);
            return registry.types();
        }
    }

    public static Set<Variable> createFromThings(final List<graql.lang.pattern.variable.ThingVariable<?>> variables) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "things")) {
            return createFromVariables(variables);
        }
    }

    public static Set<Variable> createFromVariables(
            final List<? extends graql.lang.pattern.variable.BoundVariable> variables) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "variables")) {
            final VariableRegistry registry = new VariableRegistry();
            variables.forEach(registry::register);
            final Set<Variable> output = new HashSet<>();
            output.addAll(registry.types());
            output.addAll(registry.things());
            return output;
        }
    }

    public Identifier identifier() {
        return identifier;
    }

    public Reference reference() {
        return identifier.reference();
    }

    public boolean isType() {
        return false;
    }

    public boolean isThing() {
        return false;
    }

    public TypeVariable asType() {
        throw new GraknException(INVALID_CASTING.message(className(this.getClass()), className(TypeVariable.class)));
    }

    public ThingVariable asThing() {
        throw new GraknException(INVALID_CASTING.message(className(this.getClass()), className(ThingVariable.class)));
    }

    public abstract Set<? extends Constraint> constraints();

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();
}
