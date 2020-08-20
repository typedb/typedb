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
import grakn.core.concept.type.Type;
import grakn.core.query.writer.DefineWriter;
import grakn.core.query.writer.UndefineWriter;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlUndefine;

import java.util.List;
import java.util.stream.Stream;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;

public class Query {

    private static final String TRACE_STREAM_DEFINE = "stream.define";
    private static final String TRACE_STREAM_UNDEFINE = "stream.undefine";
    private static final String TRACE_STREAM_INSERT = "stream.insert";
    private static final String TRACE_STREAM_DELETE = "stream.delete";
    private final Concepts concepts;
    private final Context.Transaction transactionContext;

    public Query(Concepts concepts, Context.Transaction transactionContext) {
        this.concepts = concepts;
        this.transactionContext = transactionContext;
    }

    public Stream<ConceptMap> insert(GraqlInsert query) {
        return insert(query, new Options.Query());
    }

    public Stream<ConceptMap> insert(GraqlInsert query, Options.Query options) {
        try (ThreadTrace ignored = traceOnThread(TRACE_STREAM_INSERT)) {
            Context.Query context = new Context.Query(transactionContext, options);
            return null; // TODO
        }
    }

    public void delete(GraqlDelete query) {
        delete(query, new Options.Query());
    }

    public void delete(GraqlDelete query, Options.Query options) {
        try (ThreadTrace ignored = traceOnThread(TRACE_STREAM_DELETE)) {
            Context.Query context = new Context.Query(transactionContext, options);
        }
    }

    public List<Type> define(GraqlDefine query) {
        return define(query, new Options.Query());
    }

    public List<Type> define(GraqlDefine query, Options.Query options) {
        try (ThreadTrace ignored = traceOnThread(TRACE_STREAM_DEFINE)) {
            Context.Query context = new Context.Query(transactionContext, options);
            DefineWriter writer = new DefineWriter(concepts, query, context);
            return writer.write();
        }
    }

    public void undefine(GraqlUndefine query) {
        undefine(query, new Options.Query());
    }

    public void undefine(GraqlUndefine query, Options.Query options) {
        try (ThreadTrace ignored = traceOnThread(TRACE_STREAM_UNDEFINE)) {
            Context.Query context = new Context.Query(transactionContext, options);
            UndefineWriter writer = new UndefineWriter(concepts, query, context);
            writer.write();
        }
    }
}
