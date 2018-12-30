/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ai.grakn.client;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.QueryExecutor;
import ai.grakn.client.concept.RemoteConcept;
import ai.grakn.client.executor.RemoteQueryExecutor;
import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.client.rpc.ResponseReader;
import ai.grakn.client.rpc.Transceiver;
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
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.KeyspaceProto;
import ai.grakn.rpc.proto.KeyspaceServiceGrpc;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.rpc.proto.SessionServiceGrpc;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.AbstractIterator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Entry-point which communicates with a running Grakn server using gRPC.
 * For now, only a subset of {@link GraknSession} and {@link ai.grakn.GraknTx} features are supported.
 */
public final class Grakn {
    @Deprecated
    public static final SimpleURI DEFAULT_HTTP_URI = new SimpleURI("localhost:4567");

    private ManagedChannel channel;
    private KeyspaceServiceGrpc.KeyspaceServiceBlockingStub keyspaceBlockingStub;
    private Keyspace keyspace;
    private final OpenRequestBuilder openRequestBuilder;

    /**
     * Build an RPC Open request.
     * The Open request might need to be built in a different fashion (e.g. add authentication)
     */
    interface OpenRequestBuilder {
        SessionProto.Transaction.Req build(GraknTxType type, ai.grakn.Keyspace keyspace);
    }


    public Grakn(SimpleURI uri) {
        channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext(true).build();
        keyspaceBlockingStub = KeyspaceServiceGrpc.newBlockingStub(channel);
        keyspace = new Keyspace();
        openRequestBuilder = (type, keyspace) -> RequestBuilder.Transaction.open(keyspace, type);
    }

    Grakn (ManagedChannel channel, OpenRequestBuilder openRequestBuilder){
        this.channel = channel;
        keyspaceBlockingStub = KeyspaceServiceGrpc.newBlockingStub(channel);
        keyspace = new Keyspace();
        this.openRequestBuilder = openRequestBuilder;
    }

    public Grakn.Session session(ai.grakn.Keyspace keyspace) {
        return new Session(keyspace);
    }

    public Grakn.Keyspace keyspaces(){
        return keyspace;
    }

    /**
     * Remote implementation of {@link GraknSession} that communicates with a Grakn server using gRPC.
     *
     * @see Transaction
     * @see Grakn
     */
    public class Session implements GraknSession {

        private final ai.grakn.Keyspace keyspace;

        private Session(ai.grakn.Keyspace keyspace) {
            this.keyspace = keyspace;
        }

        SessionServiceGrpc.SessionServiceStub sessionStub() {
            return SessionServiceGrpc.newStub(channel);
        }

        KeyspaceServiceGrpc.KeyspaceServiceBlockingStub keyspaceBlockingStub() {
            return KeyspaceServiceGrpc.newBlockingStub(channel);
        }

        @Override
        public Grakn.Transaction transaction(GraknTxType type) {
            return new Transaction(this, type, openRequestBuilder.build(type, keyspace));
        }

        @Override
        public void close() throws GraknTxOperationException {
            channel.shutdown();
        }

        @Override
        public ai.grakn.Keyspace keyspace() {
            return keyspace;
        }
    }

    /**
     * Internal class used to handle keyspace related operations
     */

    public final class Keyspace {

        public void delete(ai.grakn.Keyspace keyspace){
            KeyspaceProto.Keyspace.Delete.Req request = RequestBuilder.Keyspace.delete(keyspace.getValue());
            keyspaceBlockingStub.delete(request);
        }
    }

    /**
     * Remote implementation of {@link GraknTx} and {@link GraknAdmin} that communicates with a Grakn server using gRPC.
     */
    public static final class Transaction implements GraknTx, GraknAdmin {

        private final Session session;
        private final GraknTxType type;
        private final Transceiver transceiver;

        private Transaction(Session session,  GraknTxType type, SessionProto.Transaction.Req openRequest) {
            this.session = session;
            this.type = type;
            this.transceiver = Transceiver.create(session.sessionStub());
            transceiver.send(openRequest);
            responseOrThrow();
        }

        @Override
        public GraknAdmin admin() {
            return this;
        }

        @Override
        public GraknTxType txType() {
            return type;
        }

        @Override
        public GraknSession session() {
            return session;
        }

        @Override
        public void close() {
            transceiver.close();
        }

        @Override
        public boolean isClosed() {
            return transceiver.isClosed();
        }

        @Override
        public QueryBuilder graql() {
            return new QueryBuilderImpl(this);
        }

        @Override
        public QueryExecutor queryExecutor() {
            return RemoteQueryExecutor.create(this);
        }


        private SessionProto.Transaction.Res responseOrThrow() {
            Transceiver.Response response;

            try {
                response = transceiver.receive();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // This is called from classes like Transaction, that impl methods which do not throw InterruptedException
                // Therefore, we have to wrap it in a RuntimeException.
                throw new RuntimeException(e);
            }

            switch (response.type()) {
                case OK:
                    return response.ok();
                case ERROR:
                    throw new RuntimeException(response.error().getMessage());
                case COMPLETED:
                default:
                    throw CommonUtil.unreachableStatement("Unexpected response " + response);
            }
        }

        @Override
        public void commit() throws InvalidKBException {
            transceiver.send(RequestBuilder.Transaction.commit());
            responseOrThrow();
            close();
        }

        public java.util.Iterator query(Query<?> query) {
            transceiver.send(RequestBuilder.Transaction.query(query.toString(), query.inferring()));
            SessionProto.Transaction.Res txResponse = responseOrThrow();
            int iteratorId = txResponse.getQueryIter().getId();
            return new Iterator<>(
                    this,
                    iteratorId,
                    response -> ResponseReader.answer(response.getQueryIterRes().getAnswer(), this)
            );
        }

        @Nullable
        @Override
        public <T extends Type> T getType(Label label) {
            SchemaConcept concept = getSchemaConcept(label);
            if (concept == null || !concept.isType()) return null;
            return (T) concept.asType();
        }

        @Nullable
        @Override
        public EntityType getEntityType(String label) {
            SchemaConcept concept = getSchemaConcept(Label.of(label));
            if (concept == null || !concept.isEntityType()) return null;
            return concept.asEntityType();
        }

        @Nullable
        @Override
        public RelationshipType getRelationshipType(String label) {
            SchemaConcept concept = getSchemaConcept(Label.of(label));
            if (concept == null || !concept.isRelationshipType()) return null;
            return concept.asRelationshipType();
        }

        @Nullable
        @Override
        public <V> AttributeType<V> getAttributeType(String label) {
            SchemaConcept concept = getSchemaConcept(Label.of(label));
            if (concept == null || !concept.isAttributeType()) return null;
            return concept.asAttributeType();
        }

        @Nullable
        @Override
        public Role getRole(String label) {
            SchemaConcept concept = getSchemaConcept(Label.of(label));
            if (concept == null || !concept.isRole()) return null;
            return concept.asRole();
        }

        @Nullable
        @Override
        public Rule getRule(String label) {
            SchemaConcept concept = getSchemaConcept(Label.of(label));
            if (concept == null || !concept.isRule()) return null;
            return concept.asRule();
        }

        @Nullable
        @Override
        public <T extends SchemaConcept> T getSchemaConcept(Label label) {
            transceiver.send(RequestBuilder.Transaction.getSchemaConcept(label));
            SessionProto.Transaction.Res response = responseOrThrow();
            switch (response.getGetSchemaConceptRes().getResCase()) {
                case NULL:
                    return null;
                default:
                    return (T) RemoteConcept.of(response.getGetSchemaConceptRes().getSchemaConcept(), this).asSchemaConcept();
            }
        }

        @Nullable
        @Override
        public <T extends Concept> T getConcept(ConceptId id) {
            transceiver.send(RequestBuilder.Transaction.getConcept(id));
            SessionProto.Transaction.Res response = responseOrThrow();
            switch (response.getGetConceptRes().getResCase()) {
                case NULL:
                    return null;
                default:
                    return (T) RemoteConcept.of(response.getGetConceptRes().getConcept(), this);
            }
        }

        @Override
        public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
            transceiver.send(RequestBuilder.Transaction.getAttributes(value));
            int iteratorId = responseOrThrow().getGetAttributesIter().getId();
            Iterable<Concept> iterable = () -> new Iterator<>(
                    this, iteratorId, response -> RemoteConcept.of(response.getGetAttributesIterRes().getAttribute(), this)
            );

            return StreamSupport.stream(iterable.spliterator(), false).map(Concept::<V>asAttribute).collect(toImmutableSet());
        }

        @Override
        public EntityType putEntityType(Label label) {
            transceiver.send(RequestBuilder.Transaction.putEntityType(label));
            return RemoteConcept.of(responseOrThrow().getPutEntityTypeRes().getEntityType(), this).asEntityType();
        }

        @Override
        public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
            transceiver.send(RequestBuilder.Transaction.putAttributeType(label, dataType));
            return RemoteConcept.of(responseOrThrow().getPutAttributeTypeRes().getAttributeType(), this).asAttributeType();
        }

        @Override
        public RelationshipType putRelationshipType(Label label) {
            transceiver.send(RequestBuilder.Transaction.putRelationshipType(label));
            return RemoteConcept.of(responseOrThrow().getPutRelationTypeRes().getRelationType(), this).asRelationshipType();
        }

        @Override
        public Role putRole(Label label) {
            transceiver.send(RequestBuilder.Transaction.putRole(label));
            return RemoteConcept.of(responseOrThrow().getPutRoleRes().getRole(), this).asRole();
        }

        @Override
        public Rule putRule(Label label, Pattern when, Pattern then) {
            transceiver.send(RequestBuilder.Transaction.putRule(label, when, then));
            return RemoteConcept.of(responseOrThrow().getPutRuleRes().getRule(), this).asRule();
        }

        @Override
        public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
            ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                    .setSchemaConceptSupsReq(ConceptProto.SchemaConcept.Sups.Req.getDefaultInstance()).build();

            SessionProto.Transaction.Res response = runConceptMethod(schemaConcept.id(), method);
            int iteratorId = response.getConceptMethodRes().getResponse().getSchemaConceptSupsIter().getId();

            Iterable<? extends Concept> iterable = () -> new Iterator<>(
                    this, iteratorId, res -> RemoteConcept.of(res.getConceptMethodIterRes().getSchemaConceptSupsIterRes().getSchemaConcept(), this)
            );

            Stream<? extends Concept> sups = StreamSupport.stream(iterable.spliterator(), false);
            return Objects.requireNonNull(sups).map(Concept::asSchemaConcept);
        }

        public SessionProto.Transaction.Res runConceptMethod(ConceptId id, ConceptProto.Method.Req method) {
            SessionProto.Transaction.ConceptMethod.Req conceptMethod = SessionProto.Transaction.ConceptMethod.Req.newBuilder()
                    .setId(id.getValue()).setMethod(method).build();
            SessionProto.Transaction.Req request = SessionProto.Transaction.Req.newBuilder().setConceptMethodReq(conceptMethod).build();

            transceiver.send(request);
            return responseOrThrow();
        }

        private SessionProto.Transaction.Iter.Res iterate(int iteratorId) {
            transceiver.send(RequestBuilder.Transaction.iterate(iteratorId));
            return responseOrThrow().getIterateRes();
        }

        /**
         * A client-side iterator over gRPC messages. Will send {@link SessionProto.Transaction.Iter.Req} messages until
         * {@link SessionProto.Transaction.Iter.Res} returns done as a message.
         *
         * @param <T> class type of objects being iterated
         */
        public static class Iterator<T> extends AbstractIterator<T> {
            private final int iteratorId;
            private Transaction tx;
            private Function<SessionProto.Transaction.Iter.Res, T> responseReader;

            public Iterator(Transaction tx, int iteratorId, Function<SessionProto.Transaction.Iter.Res, T> responseReader) {
                this.tx = tx;
                this.iteratorId = iteratorId;
                this.responseReader = responseReader;
            }

            @Override
            protected final T computeNext() {
                SessionProto.Transaction.Iter.Res response = tx.iterate(iteratorId);

                switch (response.getResCase()) {
                    case DONE:
                        return endOfData();
                    case RES_NOT_SET:
                        throw CommonUtil.unreachableStatement("Unexpected " + response);
                    default:
                        return responseReader.apply(response);
                }
            }
        }
    }
}
