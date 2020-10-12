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

package grakn.core.pattern;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import graql.lang.pattern.Conjunctable;

import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static java.util.stream.Collectors.toSet;

public class Disjunction implements Pattern {

    private static final String TRACE_PREFIX = "disjunction.";
    private final Set<Conjunction> conjunctions;

    public Disjunction(final Set<Conjunction> conjunctions) {
        this.conjunctions = conjunctions;
    }

    public static Disjunction create(
            final graql.lang.pattern.Disjunction<graql.lang.pattern.Conjunction<Conjunctable>> graql) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            return new Disjunction(graql.patterns().stream().map(Conjunction::create).collect(toSet()));
        }
    }

    public Set<Conjunction> conjunctions() {
        return conjunctions;
    }
}
