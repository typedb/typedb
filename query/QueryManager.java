/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.query;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMapGroup;
import com.vaticle.typedb.core.concept.answer.ReadableConceptTree;
import com.vaticle.typedb.core.concept.answer.ValueGroup;
import com.vaticle.typedb.core.concept.value.Value;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLDelete;
import com.vaticle.typeql.lang.query.TypeQLFetch;
import com.vaticle.typeql.lang.query.TypeQLGet;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLUndefine;
import com.vaticle.typeql.lang.query.TypeQLUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_DATA_READ_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_SCHEMA_READ_VIOLATION;

public class QueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);
    static final int PARALLELISATION_SPLIT_MIN = 8;

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

    public FunctionalIterator<? extends ConceptMap> get(TypeQLGet query) {
        return get(query, defaultContext);
    }

    public FunctionalIterator<? extends ConceptMap> get(TypeQLGet query, Context.Query context) {
        try {
            return Getter.create(reasoner, conceptMgr, query, context).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public FunctionalIterator<Explanation> explain(long explainableId) {
        return reasoner.explain(explainableId, defaultContext);
    }

    public Optional<Value<?>> get(TypeQLGet.Aggregate query) {
        return get(query, defaultContext);
    }

    public Optional<Value<?>> get(TypeQLGet.Aggregate query, Context.Query queryContext) {
        try {
            return Getter.create(reasoner, conceptMgr, query, queryContext).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public FunctionalIterator<ConceptMapGroup> get(TypeQLGet.Group query) {
        return get(query, defaultContext);
    }

    public FunctionalIterator<ConceptMapGroup> get(TypeQLGet.Group query, Context.Query queryContext) {
        try {
            return Getter.create(reasoner, conceptMgr, query, queryContext).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public FunctionalIterator<ValueGroup> get(TypeQLGet.Group.Aggregate query) {
        return get(query, defaultContext);
    }

    public FunctionalIterator<ValueGroup> get(TypeQLGet.Group.Aggregate query, Context.Query queryContext) {
        try {
            return Getter.create(reasoner, conceptMgr, query, queryContext).execute().onError(conceptMgr::exception);
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }

    public FunctionalIterator<ReadableConceptTree> fetch(TypeQLFetch query) {
        return fetch(query, defaultContext);
    }

    public FunctionalIterator<ReadableConceptTree> fetch(TypeQLFetch query, Context.Query queryContext) {
        try {
            return Fetcher.create(reasoner, conceptMgr, query, queryContext).execute().onError(conceptMgr::exception);
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
        try {
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
        try {
            Deleter.create(reasoner, conceptMgr, query, context).execute();
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
        try {
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
        try {
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
        try {
            Undefiner.create(conceptMgr, logicMgr, query, context).execute();
        } catch (Exception exception) {
            throw conceptMgr.exception(exception);
        }
    }
}
