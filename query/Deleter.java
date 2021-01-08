/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.common.parameters.Context;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Thing;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;

public class Deleter {

    private static final String TRACE_PREFIX = "deleter.";

    private final ConceptManager conceptMgr;
    private final Context.Query context;
    private final ConceptMap existing;
    private final Set<ThingVariable> variables;
    private final Map<Reference, Thing> deleted;

    private Deleter(ConceptManager conceptMgr, Set<ThingVariable> variables,
                    ConceptMap existing, Context.Query context) {
        this.conceptMgr = conceptMgr;
        this.context = context;
        this.existing = existing;
        this.variables = variables;
        this.deleted = new HashMap<>();
    }

    public static Deleter create(ConceptManager conceptMgr,
                                 List<graql.lang.pattern.variable.ThingVariable<?>> variables,
                                 ConceptMap existing, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            return new Deleter(conceptMgr, VariableRegistry.createFromThings(variables).things(), existing, context);
        }
    }

    public void execute() {
//        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
//            TODO
//        }
    }
}
