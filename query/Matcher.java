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

package grakn.core.query;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Context;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.Reasoner;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;

public class Matcher {

    private static final String TRACE_PREFIX = "matcher.";
    private final Reasoner reasoner;
    private final Disjunction disjunction;
    private final Context.Query context;

    private Matcher(final TraversalEngine traversalEng, final ConceptManager conceptMgr,
                    final Disjunction disjunction, final Context.Query context) {
        this.reasoner = new Reasoner(traversalEng, conceptMgr);
        this.disjunction = disjunction;
        this.context = context;
    }

    public static Matcher create(final TraversalEngine traversalEng, final ConceptManager conceptMgr,
                                 final Conjunction<? extends Pattern> conjunction, final Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            return new Matcher(traversalEng, conceptMgr, Disjunction.create(conjunction.normalise()), context);
        }
    }

    public ResourceIterator<ConceptMap> execute() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            return reasoner.execute(disjunction);
        }
    }
}
