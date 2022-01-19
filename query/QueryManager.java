/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.query;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.ThreadTrace;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMapGroup;
import com.vaticle.typedb.core.concept.answer.Numeric;
import com.vaticle.typedb.core.concept.answer.NumericGroup;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLDelete;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLUndefine;
import com.vaticle.typeql.lang.query.TypeQLUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_DATA_READ_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_SCHEMA_READ_VIOLATION;

public class QueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);
    static final int PARALLELISATION_SPLIT_MIN = 8;

    private static final String TRACE_PREFIX = "query.";
    private final LogicManager logicMgr;
    private final Reasoner reasoner;
    private final ConceptManager conceptMgr;
    private final Context.Query defaultContext;

    public QueryManager(ConceptManager conceptMgr, LogicManager logicMgr, Reasoner reasoner, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.reasoner = reasoner;
        this.defaultContext = new Context.Query(context, new Options.Query());
    }

    public FunctionalIterator<ConceptMap> match(TypeQLMatch query) {
        return match(query, defaultContext);
    }

    public FunctionalIterator<ConceptMap> match(TypeQLMatch query, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match")) {
            return Matcher.create(reasoner, query, context).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public FunctionalIterator<Explanation> explain(long explainableId) {
        return reasoner.explain(explainableId, defaultContext);
    }

    public Numeric match(TypeQLMatch.Aggregate query) {
        return match(query, defaultContext);
    }

    public Numeric match(TypeQLMatch.Aggregate query, Context.Query queryContext) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match_aggregate")) {
            return Matcher.create(reasoner, query, queryContext).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public FunctionalIterator<ConceptMapGroup> match(TypeQLMatch.Group query) {
        return match(query, defaultContext);
    }

    public FunctionalIterator<ConceptMapGroup> match(TypeQLMatch.Group query, Context.Query queryContext) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match_group")) {
            return Matcher.create(reasoner, query, queryContext).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public FunctionalIterator<NumericGroup> match(TypeQLMatch.Group.Aggregate query) {
        return match(query, defaultContext);
    }

    public FunctionalIterator<NumericGroup> match(TypeQLMatch.Group.Aggregate query, Context.Query queryContext) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "match_group_aggregate")) {
            return Matcher.create(reasoner, query, queryContext).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public FunctionalIterator<ConceptMap> insert(TypeQLInsert query) {
        return insert(query, defaultContext);
    }

    public FunctionalIterator<ConceptMap> insert(TypeQLInsert query, Context.Query context) {
        if (context.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        if (context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_DATA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            return Inserter.create(reasoner, conceptMgr, query, context).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void delete(TypeQLDelete query) {
        delete(query, defaultContext);
    }

    public void delete(TypeQLDelete query, Context.Query context) {
        if (context.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        if (context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_DATA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
            Deleter.create(reasoner, query, context).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void update(TypeQLUpdate query) {
        update(query, defaultContext);
    }

    public FunctionalIterator<ConceptMap> update(TypeQLUpdate query, Context.Query context) {
        if (context.sessionType().isSchema()) throw conceptMgr.exception(SESSION_SCHEMA_VIOLATION);
        if (context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_DATA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "update")) {
            return Updater.create(reasoner, conceptMgr, query, context).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void define(TypeQLDefine query) {
        define(query, defaultContext);
    }

    public void define(TypeQLDefine query, Context.Query context) {
        if (context.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION);
        if (context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_SCHEMA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "define")) {
            Definer.create(conceptMgr, logicMgr, query, context).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public void undefine(TypeQLUndefine query) {
        undefine(query, defaultContext);
    }

    public void undefine(TypeQLUndefine query, Context.Query context) {
        if (context.sessionType().isData()) throw conceptMgr.exception(SESSION_DATA_VIOLATION);
        if (context.transactionType().isRead()) throw conceptMgr.exception(TRANSACTION_SCHEMA_READ_VIOLATION);
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "undefine")) {
            Undefiner.create(conceptMgr, logicMgr, query, context).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }
}
