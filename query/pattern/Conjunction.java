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

package grakn.core.query.pattern;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.query.pattern.variable.TypeVariable;
import grakn.core.query.pattern.variable.Variable;
import grakn.core.query.pattern.variable.VariableRegistry;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;

public class Conjunction<PATTERN extends Pattern> extends Pattern {

    private static final String TRACE_PREFIX = "conjunction.";
    private final Set<PATTERN> patterns;

    private Conjunction(Set<PATTERN> patterns) {
        this.patterns = patterns;
    }

    public static Conjunction<TypeVariable> fromTypes(final List<graql.lang.pattern.variable.TypeVariable> variables) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "fromtypes")) {
            VariableRegistry registry = new VariableRegistry();
            variables.forEach(registry::register);
            return new Conjunction<>(registry.types());
        }
    }

    public static Conjunction<Variable> fromThings(final List<graql.lang.pattern.variable.ThingVariable<?>> variables) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "fromthings")) {
            VariableRegistry registry = new VariableRegistry();
            variables.forEach(registry::register);
            Set<Variable> output = new HashSet<>();
            output.addAll(registry.types());
            output.addAll(registry.things());
            return new Conjunction<>(output);
        }
    }

    public Set<PATTERN> patterns() {
        return patterns;
    }
}
