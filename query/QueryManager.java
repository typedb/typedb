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
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptMapGroup;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.NumericGroup;
import grakn.core.logic.LogicManager;
import grakn.core.reasoner.Reasoner;
import graql.lang.pattern.variable.UnboundVariable;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_DATA_READ_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_SCHEMA_READ_VIOLATION;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.single;

public class QueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);

    private static final String TRACE_PREFIX = "query.";
    private final LogicManager logicMgr;
    private final Reasoner reasoner;
    private final ConceptManager conceptMgr;
    private final Context.Transaction context;
    private final Context.Query defaultOptions;

    public QueryManager(ConceptManager conceptMgr, LogicManager logicMgr, Reasoner reasoner, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.reasoner = reasoner;
        this.context = context;
        this.defaultOptions = new Context.Query(context, new Options.Query());
    }

    public ResourceIterator<ConceptMap> match(GraqlMatch query) {
        return match(query, defaultOptions);
    }

    public ResourceIterator<ConceptMap> match(GraqlMatch query, Context.Query queryContext) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match")) {

            return Matcher.create(reasoner, query, queryContext).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public Numeric match(GraqlMatch.Aggregate query) {
        return match(query, defaultOptions);
    }

    public Numeric match(GraqlMatch.Aggregate query, Context.Query queryContext) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match_aggregate")) {
            return Matcher.create(reasoner, query, queryContext).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public ResourceIterator<ConceptMapGroup> match(GraqlMatch.Group query) {
        return match(query, defaultOptions);
    }

    public ResourceIterator<ConceptMapGroup> match(GraqlMatch.Group query, Context.Query queryContext) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match_group")) {
            return Matcher.create(reasoner, query, queryContext).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public ResourceIterator<NumericGroup> match(GraqlMatch.Group.Aggregate query) {
        return match(query, defaultOptions);
    }

    public ResourceIterator<NumericGroup> match(GraqlMatch.Group.Aggregate query, Context.Query queryContext) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match_group_aggregate")) {
            return Matcher.create(reasoner, query, queryContext).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public ResourceIterator<ConceptMap> insert(GraqlInsert query) {
        return insert(query, defaultOptions);
    }

    public ResourceIterator<ConceptMap> insert(GraqlInsert query, Context.Query queryContext) {
        if (this.context.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        if (this.context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_DATA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            if (query.match().isPresent()) {
                GraqlMatch.Unfiltered match = query.match().get();
                List<UnboundVariable> filterVars = new ArrayList<>(match.namedVariablesUnbound());
                filterVars.retainAll(query.namedVariablesUnbound());
                assert !filterVars.isEmpty();
                List<ConceptMap> matched = match(match.get(filterVars), queryContext).toList();
                return iterate(iterate(matched).map(answer -> Inserter.create(
                        conceptMgr, query.variables(), answer, queryContext
                ).execute()).toList());
            } else {
                return single(Inserter.create(conceptMgr, query.variables(), queryContext).execute());
            }
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void delete(GraqlDelete query) {
        delete(query, defaultOptions);
    }

    public void delete(GraqlDelete query, Context.Query queryContext) {
        if (this.context.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        if (this.context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_DATA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
            List<UnboundVariable> filterVars = new ArrayList<>(query.match().namedVariablesUnbound());
            filterVars.retainAll(query.namedVariablesUnbound());
            assert !filterVars.isEmpty();
            List<ConceptMap> matched = match(query.match().get(filterVars), queryContext).toList();
            matched.forEach(existing -> Deleter.create(query.variables(), existing, queryContext).execute());
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void define(GraqlDefine query) {
        define(query, defaultOptions);
    }

    public void define(GraqlDefine query, Context.Query queryContext) {
        if (this.context.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION);
        if (this.context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_SCHEMA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            Definer.create(conceptMgr, logicMgr, query.variables(), query.rules(), queryContext).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void undefine(GraqlUndefine query) {
        undefine(query, defaultOptions);
    }

    public void undefine(GraqlUndefine query, Context.Query queryContext) {
        if (this.context.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION);
        if (this.context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_SCHEMA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine")) {
            Undefiner.create(conceptMgr, logicMgr, query.variables(), query.rules(), queryContext).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }
}
