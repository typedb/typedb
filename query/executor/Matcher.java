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

package grakn.core.query.executor;

import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.core.common.parameters.Context;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graph.Graphs;
import grakn.core.query.pattern.Disjunction;
import grakn.core.query.pattern.Pattern;

import java.util.stream.Stream;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;

public class Matcher {

    private static final String TRACE_PREFIX = "matcher.";
    private final Graphs graphMgr;
    private final Disjunction disjunction;
    private final Context.Query context;

    public Matcher(Graphs graphMgr, graql.lang.pattern.Conjunction<? extends graql.lang.pattern.Pattern> conjunction, Context.Query context) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "constructor")) {
            this.graphMgr = graphMgr;
            this.disjunction = Pattern.fromGraqlConjunction(conjunction);
            this.context = context;
        }
    }

    public Stream<ConceptMap> execute() {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            return Stream.empty(); // TODO
        }
    }
}
