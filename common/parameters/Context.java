/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.common.parameters;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import javax.annotation.Nullable;

import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;

public class Context<PARENT extends Context<?, ?>, OPTIONS extends Options<?, ?>> {

    Arguments.Session.Type sessionType;
    Arguments.Transaction.Type transactionType;
    long transactionId;
    private final OPTIONS options;

    private Context(@Nullable PARENT parent, OPTIONS options) {
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
