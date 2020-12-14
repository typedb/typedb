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
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.Reasoner;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;

import java.util.List;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static grakn.core.common.iterator.Iterators.iterate;

public class QueryManager {

    private static final String TRACE_PREFIX = "query.";
    private final LogicManager logicMgr;
    private final Reasoner reasoner;
    private final ConceptManager conceptMgr;
    private final Context.Transaction transactionCtx;

    public QueryManager(ConceptManager conceptMgr, LogicManager logicMgr, Reasoner reasoner, Context.Transaction transactionCtx) {
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.reasoner = reasoner;
        this.transactionCtx = transactionCtx;
    }

    public ResourceIterator<ConceptMap> match(GraqlMatch query) {
        return match(query, new Options.Query());
    }

    public ResourceIterator<ConceptMap> match(GraqlMatch query, Options.Query options) {
        // TODO: Note that Query Options are not yet utilised during match query
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match")) {
            Disjunction disjunction = Disjunction.create(query.conjunction().normalise());
            return reasoner.executeSync(disjunction);
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public ResourceIterator<ConceptMap> insert(GraqlInsert query) {
        return insert(query, new Options.Query());
    }

    public ResourceIterator<ConceptMap> insert(GraqlInsert query, Options.Query options) {
        if (transactionCtx.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            if (query.match().isPresent()) {
                List<ConceptMap> matched = match(query.match().get()).toList();
                return iterate(iterate(matched).map(answer -> Inserter.create(
                        conceptMgr, query.variables(), answer, context
                ).execute()).toList());
            } else {
                return iterate(list(Inserter.create(conceptMgr, query.variables(), context).execute()));
            }
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void delete(GraqlDelete query) {
        delete(query, new Options.Query());
    }

    public void delete(GraqlDelete query, Options.Query options) {
        if (transactionCtx.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
            final Context.Query context = new Context.Query(transactionCtx, options);
            final List<ConceptMap> matched = match(query.match()).toList();
            matched.forEach(existing -> Deleter.create(conceptMgr, query.variables(), existing, context).execute());
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void define(GraqlDefine query) {
        if (transactionCtx.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            Definer.create(conceptMgr, logicMgr, query.variables(), query.rules()).execute();
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void undefine(GraqlUndefine query) {
        if (transactionCtx.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine")) {
            Undefiner.create(conceptMgr, logicMgr, query.variables(), query.rules()).execute();
        } catch (GraknException exception) {
            throw conceptMgr.exception(exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public Numeric match(GraqlMatch.Aggregate matchAggregate) {
        // TODO
        return null;
    }

    public ResourceIterator<AnswerGroup<ConceptMap>> match(GraqlMatch.Group matchGroup) {
        // TODO
        return null;
    }

    public ResourceIterator<AnswerGroup<Numeric>> match(GraqlMatch.Group.Aggregate matchGroupAggregate) {
        return null;
    }
}
