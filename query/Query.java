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
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.Concepts;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.ThingType;
import grakn.core.query.writer.Definer;
import grakn.core.query.writer.Deleter;
import grakn.core.query.writer.Inserter;
import grakn.core.query.writer.Undefiner;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;

import java.util.List;
import java.util.stream.Stream;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static java.util.stream.Collectors.toList;

public class Query {

    private static final String TRACE_PREFIX = "query.";
    private final Concepts conceptMgr;
    private final Context.Transaction transactionContext;

    public Query(Concepts conceptMgr, Context.Transaction transactionContext) {
        this.conceptMgr = conceptMgr;
        this.transactionContext = transactionContext;
    }

    public Stream<ConceptMap> match(GraqlMatch query) {
        return match(query, new Options.Query());
    }

    public Stream<ConceptMap> match(GraqlMatch query, Options.Query options) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match")) {
            Context.Query context = new Context.Query(transactionContext, options);
            return null;
        }
    }

    public Stream<ConceptMap> insert(GraqlInsert query) {
        return insert(query, new Options.Query());
    }

    public Stream<ConceptMap> insert(GraqlInsert query, Options.Query options) {
        if (transactionContext.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION.message());
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            Context.Query context = new Context.Query(transactionContext, options);
            if (query.match().isPresent()) {
                List<ConceptMap> matched = match(query.match().get()).collect(toList());
                return matched.stream().map(answers -> new Inserter(conceptMgr, query.variables(), answers, context).execute());
            } else {
                return Stream.of(new Inserter(conceptMgr, query.variables(), context).execute());
            }
        }
    }

    public void delete(GraqlDelete query) {
        delete(query, new Options.Query());
    }

    public void delete(GraqlDelete query, Options.Query options) {
        if (transactionContext.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION.message());
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
            Context.Query context = new Context.Query(transactionContext, options);
            List<ConceptMap> matched = match(query.match()).collect(toList());
            matched.forEach(existing -> new Deleter(conceptMgr, query.variables(), context, existing).execute());
        }
    }

    public List<ThingType> define(GraqlDefine query) {
        return define(query, new Options.Query());
    }

    public List<ThingType> define(GraqlDefine query, Options.Query options) {
        if (transactionContext.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION.message());
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            Context.Query context = new Context.Query(transactionContext, options);
            return new Definer(conceptMgr, query.variables(), context).execute();
        }
    }

    public void undefine(GraqlUndefine query) {
        undefine(query, new Options.Query());
    }

    public void undefine(GraqlUndefine query, Options.Query options) {
        if (transactionContext.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION.message());
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine")) {
            Context.Query context = new Context.Query(transactionContext, options);
            new Undefiner(conceptMgr, query.variables(), context).execute();
        }
    }
}
