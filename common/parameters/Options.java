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
 */

package grakn.core.common.parameters;

import grakn.core.common.exception.GraknException;
import graql.lang.query.GraqlQuery;

import java.nio.file.Path;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_CANNOT_BE_TOGGLED_PER_QUERY;
import static grakn.core.common.exception.ErrorMessage.Reasoner.REASONING_CANNOT_BE_TOGGLED_PER_QUERY;

public abstract class Options<PARENT extends Options<?, ?>, SELF extends Options<?, ?>> {

    public static final int DEFAULT_RESPONSE_BATCH_SIZE = 50;
    public static final int DEFAULT_SESSION_IDLE_TIMEOUT_MILLIS = 10_000;
    public static final int DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS = 10_000;
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
    private Integer batchSize = null;
    private Integer sessionIdlTimeoutMillis = null;
    private Integer schemaLockAcquireTimeoutMillis = null;
    private Boolean readAnyReplica = null;

    protected Path graknDir = null;
    protected Path dataDir = null;
    protected Path logsDir = null;
    protected Boolean prefetch = null;

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

    public int responseBatchSize() {
        if (batchSize != null) return batchSize;
        else if (parent != null) return parent.responseBatchSize();
        else return DEFAULT_RESPONSE_BATCH_SIZE;
    }

    public SELF responseBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return getThis();
    }

    public int sessionIdleTimeoutMillis() {
        if (sessionIdlTimeoutMillis != null) return sessionIdlTimeoutMillis;
        else if (parent != null) return parent.sessionIdleTimeoutMillis();
        else return DEFAULT_SESSION_IDLE_TIMEOUT_MILLIS;
    }

    public SELF sessionIdleTimeoutMillis(int idleTimeoutMillis) {
        this.sessionIdlTimeoutMillis = idleTimeoutMillis;
        return getThis();
    }

    public int schemaLockTimeoutMillis() {
        if (schemaLockAcquireTimeoutMillis != null) return schemaLockAcquireTimeoutMillis;
        else if (parent != null) return parent.schemaLockTimeoutMillis();
        else return DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS;
    }

    public SELF schemaLockTimeoutMillis(int acquireSchemaLockTimeoutMillis) {
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

    public Path graknDir() {
        if (graknDir != null) return graknDir;
        else if (parent != null) return parent.graknDir();
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Path dataDir() {
        if (dataDir != null) return dataDir;
        else if (parent != null) return parent.dataDir();
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Path logsDir() {
        if (logsDir != null) return logsDir;
        else if (parent != null) return parent.logsDir();
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public static class Database extends Options<Options<?, ?>, Database> {

        @Override
        Database getThis() {
            return this;
        }

        public Database parent(Options<?, ?> parent) {
            throw GraknException.of(ILLEGAL_OPERATION);
        }

        public Database graknDir(Path graknDir) {
            this.graknDir = graknDir;
            return this;
        }

        public Database dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Database logsDir(Path logsDir) {
            this.logsDir = logsDir;
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
    }

    public static class Query extends Options<Transaction, Query> {

        private Boolean parallel = null;
        private GraqlQuery query = null;

        @Override
        Query getThis() {
            return this;
        }

        public Query query(GraqlQuery query) {
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
            throw GraknException.of(REASONING_CANNOT_BE_TOGGLED_PER_QUERY);
        }

        @Override
        public Query traceInference(boolean traceInference) {
            throw GraknException.of(REASONER_TRACING_CANNOT_BE_TOGGLED_PER_QUERY);
        }

        public Query prefetch(boolean prefetch) {
            this.prefetch = prefetch;
            return this;
        }

        public boolean parallel() {
            if (parallel != null) return parallel;
            return DEFAULT_PARALLEL;
        }

        public Query parallel(boolean parallel) {
            this.parallel = parallel;
            return this;
        }
    }
}
