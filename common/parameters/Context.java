/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.parameters;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typeql.lang.query.TypeQLQuery;
import io.sentry.ITransaction;

import javax.annotation.Nullable;

import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;

public class Context<PARENT extends Context<?, ?>, OPTIONS extends Options<?, ?>> {

    Arguments.Session.Type sessionType;
    Arguments.Transaction.Type transactionType;
    long transactionId;
    private final PARENT parent;
    private final OPTIONS options;

    private Context(@Nullable PARENT parent, OPTIONS options) {
        this.parent = parent;
        this.options = options;
        if (parent != null) {
            this.sessionType = parent.sessionType();
            this.transactionType = parent.transactionType();
            this.transactionId = parent.transactionId();
        } else {
            this.sessionType = null;
            this.transactionType = null;
        }
    }

    public OPTIONS options() {
        return options;
    }

    public PARENT parent() {
        return parent;
    }

    public Arguments.Session.Type sessionType() {
        return sessionType;
    }

    public Arguments.Transaction.Type transactionType() {
        return transactionType;
    }

    public long transactionId() {
        return transactionId;
    }

    public static class Session extends Context<Context<?, ?>, Options.Session> {

        public Session(Options.Database databaseOptions, Options.Session sessionOptions) {
            super(null, sessionOptions.parent(databaseOptions));
        }

        public Session type(Arguments.Session.Type sessionType) {
            this.sessionType = sessionType;
            return this;
        }
    }

    public static class Transaction extends Context<Context.Session, Options.Transaction> {

        private ITransaction diagnosticTxn;

        public Transaction(Context.Session context, Options.Transaction options) {
            super(context, options.parent(context.options()));
        }

        public Transaction type(Arguments.Transaction.Type transactionType) {
            this.transactionType = transactionType;
            return this;
        }

        public Transaction id(long transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public long id() {
            return transactionId;
        }

        public Transaction diagnosticTxn(ITransaction txn) {
            this.diagnosticTxn = txn;
            return this;
        }

        public ITransaction diagnosticTxn() {
            return this.diagnosticTxn;
        }
    }

    public static class Query extends Context<Context.Transaction, Options.Query> {

        private Either<Arguments.Query.Producer, Long> producerCtx;
        private static final Either<Arguments.Query.Producer, Long> DEFAULT_PRODUCER = Either.first(INCREMENTAL);

        public Query(Transaction context, Options.Query options) {
            super(context, options.parent(context.options()));
        }

        public Query(Transaction context, Options.Query options, TypeQLQuery query) {
            super(context, options.parent(context.options()));
            options.query(query);
        }

        public Either<Arguments.Query.Producer, Long> producer() {
            if (producerCtx != null) return producerCtx;
            else return DEFAULT_PRODUCER;
        }

        public Query producer(Either<Arguments.Query.Producer, Long> producerCtx) {
            this.producerCtx = producerCtx;
            return this;
        }
    }
}
