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
import grakn.core.common.iterator.ComposableIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.ThingType;
import grakn.core.traversal.TraversalEngine;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;

import java.util.List;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static grakn.core.common.iterator.Iterators.iterate;

public class QueryManager {

    private static final String TRACE_PREFIX = "query.";
    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final Context.Transaction transactionCtx;

    public QueryManager(final TraversalEngine traversalEng,
                        final ConceptManager conceptMgr,
                        final Context.Transaction transactionCtx) {
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
        this.transactionCtx = transactionCtx;
    }

    public ComposableIterator<ConceptMap> match(final GraqlMatch query) {
        return match(query, new Options.Query());
    }

    public ComposableIterator<ConceptMap> match(final GraqlMatch query, final Options.Query options) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            return Matcher.create(traversalEng, query.conjunction(), context).execute();
        }
    }

    public ComposableIterator<ConceptMap> insert(final GraqlInsert query) {
        return insert(query, new Options.Query());
    }

    public ComposableIterator<ConceptMap> insert(final GraqlInsert query, final Options.Query options) {
        if (transactionCtx.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION.message());
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            if (query.match().isPresent()) {
                final List<ConceptMap> matched = match(query.match().get()).toList();
                return iterate(matched).map(answer -> Inserter.create(conceptMgr, query.variables(), answer, context).execute());
            } else {
                return iterate(Inserter.create(conceptMgr, query.variables(), context).execute());
            }
        }
    }

    public void delete(final GraqlDelete query) {
        delete(query, new Options.Query());
    }

    public void delete(final GraqlDelete query, final Options.Query options) {
        if (transactionCtx.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION.message());
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            final List<ConceptMap> matched = match(query.match()).toList();
            matched.forEach(existing -> Deleter.create(conceptMgr, query.variables(), existing, context).execute());
        }
    }

    public List<ThingType> define(final GraqlDefine query) {
        return define(query, new Options.Query());
    }

    public List<ThingType> define(final GraqlDefine query, final Options.Query options) {
        if (transactionCtx.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION.message());
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            return Definer.create(conceptMgr, query.variables(), context).execute();
        }
    }

    public void undefine(final GraqlUndefine query) {
        undefine(query, new Options.Query());
    }

    public void undefine(final GraqlUndefine query, final Options.Query options) {
        if (transactionCtx.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION.message());
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            Undefiner.create(conceptMgr, query.variables(), context).execute();
        }
    }
}
