/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.remote;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.ComputeQueryBuilder;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryParser;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.VarPattern;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;
import ai.grakn.util.CommonUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * @author Felix Chapman
 */
public class GraknRemoteTx implements GraknTx {

    private final GraknSession session;
    private final SynchronousObserver<TxRequest, TxResponse, StatusRuntimeException> observer;

    private GraknRemoteTx(GraknSession session, SynchronousObserver<TxRequest, TxResponse, StatusRuntimeException> observer) {
        this.session = session;
        this.observer = observer;
    }

    static GraknRemoteTx create(GraknRemoteSession session, GraknTxType txType) {
        GraknGrpc.GraknStub stub = session.stub();

        SynchronousObserver<TxRequest, TxResponse, StatusRuntimeException> observer = SynchronousObserver.create(stub::tx);

        observer.send(TxRequest.newBuilder().setOpen(Open.newBuilder().setKeyspace(GraknOuterClass.Keyspace.newBuilder().setValue(session.keyspace().getValue())).setTxType(convertTxType(txType))).build());
        StatusRuntimeException error = observer.receive().throwable();
        if (error != null) {
            throw convertStatusRuntimeException(error);
        }

        return new GraknRemoteTx(session, observer);
    }

    @Override
    public EntityType putEntityType(String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EntityType putEntityType(Label label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> AttributeType<V> putAttributeType(String label, AttributeType.DataType<V> dataType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Rule putRule(String label, Pattern when, Pattern then) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Rule putRule(Label label, Pattern when, Pattern then) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipType putRelationshipType(String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipType putRelationshipType(Label label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Role putRole(String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Role putRole(Label label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <T extends Type> T getType(Label label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public EntityType getEntityType(String label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public RelationshipType getRelationshipType(String label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Role getRole(String label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Rule getRule(String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraknAdmin admin() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraknSession session() {
        return session;
    }

    @Override
    public Keyspace keyspace() {
        return session.keyspace();
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryBuilder graql() {
        return new QueryBuilder() {
            @Override
            public Match match(Pattern... patterns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Match match(Collection<? extends Pattern> patterns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InsertQuery insert(VarPattern... vars) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InsertQuery insert(Collection<? extends VarPattern> vars) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DefineQuery define(VarPattern... varPatterns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DefineQuery define(Collection<? extends VarPattern> varPatterns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public UndefineQuery undefine(VarPattern... varPatterns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public UndefineQuery undefine(Collection<? extends VarPattern> varPatterns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ComputeQueryBuilder compute() {
                throw new UnsupportedOperationException();
            }

            @Override
            public QueryParser parser() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T extends Query<?>> T parse(String queryString) {
                throw new UnsupportedOperationException();
            }

            @Override
            public QueryBuilder infer(boolean infer) {
                throw new UnsupportedOperationException();
            }

            @Override
            public QueryBuilder materialise(boolean materialise) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T execute(Query<T> query) {
                observer.send(TxRequest.newBuilder().setExecQuery(GraknOuterClass.ExecQuery.newBuilder().setQuery(convertQuery(query))).build());
                return null;
            }
        };
    }

    @Override
    public void close() {
        observer.close();
    }

    @Override
    public void abort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws InvalidKBException {
        throw new UnsupportedOperationException();
    }

    private static TxType convertTxType(GraknTxType txType) {
        switch (txType) {
            case READ:
                return TxType.Read;
            case WRITE:
                return TxType.Write;
            case BATCH:
                return TxType.Batch;
            default:
                throw CommonUtil.unreachableStatement("Unrecognised tx type " + txType);
        }
    }

    private static GraknOuterClass.Query convertQuery(Query<?> query) {
        return GraknOuterClass.Query.newBuilder().setValue(query.toString()).build();
    }

    private static RuntimeException convertStatusRuntimeException(StatusRuntimeException error) {
        Status status = error.getStatus();
        if (status.getCode().equals(Status.Code.UNKNOWN)) {
            String message = status.getDescription();
            return new MyGraknException(message);
        } else {
            return error;
        }
    }

    private static class MyGraknException extends GraknException {

        protected MyGraknException(String error) {
            super(error);
        }
    }
}
