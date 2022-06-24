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
 */

package com.vaticle.typedb.core.common.parameters;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.nio.file.Path;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_CANNOT_BE_TOGGLED_PER_QUERY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONING_CANNOT_BE_TOGGLED_PER_QUERY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_IDLE_TIMEOUT_NOT_CONFIGURABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_TIMEOUT_NOT_CONFIGURABLE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class Options<PARENT extends Options<?, ?>, SELF extends Options<?, ?>> {

    public static final int DEFAULT_PREFETCH_SIZE = 50;
    public static final long DEFAULT_SESSION_IDLE_TIMEOUT_MILLIS = SECONDS.toMillis(30);
    public static final long DEFAULT_TRANSACTION_TIMEOUT_MILLIS = MINUTES.toMillis(5);
    public static final long DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS = SECONDS.toMillis(10);
    public static final boolean DEFAULT_INFER = false;
    public static final boolean DEFAULT_TRACE_INFERENCE = false;
    public static final boolean DEFAULT_EXPLAIN = false;
    public static final boolean DEFAULT_PARALLEL = true;
    public static final boolean DEFAULT_QUERY_READ_PREFETCH = true;
    public static final boolean DEFAULT_QUERY_WRITE_PREFETCH = false;
    public static final boolean DEFAULT_READ_ANY_REPLICA = false;

    private PARENT parent;
    private Boolean infer = null;
    private Boolean traceInference = null;
    private Boolean explain = null;
    private Boolean parallel = null;
    private Integer prefetchSize = null;
    private Long sessionIdleTimeoutMillis = null;
    private Long transactionTimeoutMillis = null;
    private Long schemaLockAcquireTimeoutMillis = null;
    private Boolean readAnyReplica = null;
    protected Boolean prefetch = null;
    protected Path typeDBDir = null;
    protected Path dataDir = null;
    protected Path reasonerDebuggerDir = null;
    protected Long storageDataCacheSize = null;
    protected Long storageIndexCacheSize = null;

    abstract SELF getThis();

    public SELF parent(PARENT parent) {
        this.parent = parent;
        return getThis();
    }

    public boolean infer() {
        if (infer != null) return infer;
        else if (parent != null) return parent.infer();
        else return DEFAULT_INFER;
    }

    public SELF infer(boolean infer) {
        this.infer = infer;
        return getThis();
    }

    public boolean traceInference() {
        if (traceInference != null) return traceInference;
        else if (parent != null) return parent.traceInference();
        else return DEFAULT_TRACE_INFERENCE;
    }

    public SELF traceInference(boolean traceInference) {
        this.traceInference = traceInference;
        return getThis();
    }

    public boolean explain() {
        if (explain != null) return explain;
        else if (parent != null) return parent.explain();
        else return DEFAULT_EXPLAIN;
    }

    public SELF explain(boolean explain) {
        this.explain = explain;
        return getThis();
    }

    public int prefetchSize() {
        if (prefetchSize != null) return prefetchSize;
        else if (parent != null) return parent.prefetchSize();
        else return DEFAULT_PREFETCH_SIZE;
    }

    public SELF prefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
        return getThis();
    }

    public boolean parallel() {
        if (parallel != null) return parallel;
        else if (parent != null) return parent.parallel();
        else return DEFAULT_PARALLEL;
    }

    public SELF parallel(boolean parallel) {
        this.parallel = parallel;
        return getThis();
    }

    public long sessionIdleTimeoutMillis() {
        if (sessionIdleTimeoutMillis != null) return sessionIdleTimeoutMillis;
        else if (parent != null) return parent.sessionIdleTimeoutMillis();
        else return DEFAULT_SESSION_IDLE_TIMEOUT_MILLIS;
    }

    public SELF sessionIdleTimeoutMillis(long idleTimeoutMillis) {
        this.sessionIdleTimeoutMillis = idleTimeoutMillis;
        return getThis();
    }

    public long transactionTimeoutMillis() {
        if (transactionTimeoutMillis != null) return transactionTimeoutMillis;
        else if (parent != null) return parent.transactionTimeoutMillis();
        else return DEFAULT_TRANSACTION_TIMEOUT_MILLIS;
    }

    public SELF transactionTimeoutMillis(long timeoutMillis) {
        this.transactionTimeoutMillis = timeoutMillis;
        return getThis();
    }

    public long schemaLockTimeoutMillis() {
        if (schemaLockAcquireTimeoutMillis != null) return schemaLockAcquireTimeoutMillis;
        else if (parent != null) return parent.schemaLockTimeoutMillis();
        else return DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS;
    }

    public SELF schemaLockTimeoutMillis(long acquireSchemaLockTimeoutMillis) {
        this.schemaLockAcquireTimeoutMillis = acquireSchemaLockTimeoutMillis;
        return getThis();
    }

    public boolean readAnyReplica() {
        if (readAnyReplica != null) return readAnyReplica;
        else if (parent != null) return parent.readAnyReplica();
        else return DEFAULT_READ_ANY_REPLICA;
    }

    public SELF readAnyReplica(boolean readAnyReplica) {
        this.readAnyReplica = readAnyReplica;
        return getThis();
    }

    public Path typeDBDir() {
        if (typeDBDir != null) return typeDBDir;
        else if (parent != null) return parent.typeDBDir();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public Path dataDir() {
        if (dataDir != null) return dataDir;
        else if (parent != null) return parent.dataDir();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public Path reasonerDebuggerDir() {
        if (reasonerDebuggerDir != null) return reasonerDebuggerDir;
        else if (parent != null) return parent.reasonerDebuggerDir();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public long storageDataCacheSize() {
        if (storageDataCacheSize != null) return storageDataCacheSize;
        else if (parent != null) return parent.storageDataCacheSize();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public long storageIndexCacheSize() {
        if (storageIndexCacheSize != null) return storageIndexCacheSize;
        else if (parent != null) return parent.storageIndexCacheSize();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static class Database extends Options<Options<?, ?>, Database> {

        @Override
        Database getThis() {
            return this;
        }

        public Database parent(Options<?, ?> parent) {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        public Database typeDBDir(Path typeDBDir) {
            this.typeDBDir = typeDBDir;
            return this;
        }

        public Database dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Database reasonerDebuggerDir(Path debuggerDir) {
            this.reasonerDebuggerDir = debuggerDir;
            return this;
        }

        public Database storageDataCacheSize(long size) {
            this.storageDataCacheSize = size;
            return this;
        }

        public Database storageIndexCacheSize(long size) {
            this.storageIndexCacheSize = size;
            return this;
        }
    }

    public static class Session extends Options<Database, Session> {

        @Override
        Session getThis() {
            return this;
        }
    }

    public static class Transaction extends Options<Session, Transaction> {

        @Override
        Transaction getThis() {
            return this;
        }

        @Override
        public Transaction sessionIdleTimeoutMillis(long idleTimeoutMillis) {
            throw TypeDBException.of(SESSION_IDLE_TIMEOUT_NOT_CONFIGURABLE, className(getClass()));
        }
    }

    public static class Query extends Options<Transaction, Query> {

        private TypeQLQuery query = null;

        @Override
        Query getThis() {
            return this;
        }

        public Query query(TypeQLQuery query) {
            this.query = query;
            return this;
        }

        public boolean prefetch() {
            if (prefetch != null) {
                return prefetch;
            } else if (query != null) {
                return query.type().isRead() ? DEFAULT_QUERY_READ_PREFETCH : DEFAULT_QUERY_WRITE_PREFETCH;
            } else {
                return DEFAULT_QUERY_READ_PREFETCH;
            }
        }

        @Override
        public Query infer(boolean infer) {
            throw TypeDBException.of(REASONING_CANNOT_BE_TOGGLED_PER_QUERY);
        }

        @Override
        public Query traceInference(boolean traceInference) {
            throw TypeDBException.of(REASONER_TRACING_CANNOT_BE_TOGGLED_PER_QUERY);
        }

        @Override
        public Query transactionTimeoutMillis(long timeoutMillis) {
            throw TypeDBException.of(TRANSACTION_TIMEOUT_NOT_CONFIGURABLE, className(getClass()));
        }

        public Query prefetch(boolean prefetch) {
            this.prefetch = prefetch;
            return this;
        }
    }
}
