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

package grakn.core.client;

import com.google.common.collect.AbstractIterator;
import grakn.benchmark.lib.clientinstrumentation.ClientTracingInstrumentationInterceptor;
import grakn.core.client.concept.RemoteConcept;
import grakn.core.client.exception.GraknClientException;
import grakn.core.client.executor.RemoteQueryExecutor;
import grakn.core.client.rpc.RequestBuilder;
import grakn.core.client.rpc.ResponseReader;
import grakn.core.client.rpc.Transceiver;
import grakn.core.commons.exception.Validator;
import grakn.core.commons.http.SimpleURI;
import grakn.core.commons.util.CommonUtil;
import grakn.core.graql.Pattern;
import grakn.core.graql.Query;
import grakn.core.graql.QueryBuilder;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.internal.query.QueryBuilderImpl;
import grakn.core.protocol.ConceptProto;
import grakn.core.protocol.KeyspaceProto;
import grakn.core.protocol.KeyspaceServiceGrpc;
import grakn.core.protocol.KeyspaceServiceGrpc.KeyspaceServiceBlockingStub;
import grakn.core.protocol.SessionProto;
import grakn.core.protocol.SessionServiceGrpc;
import grakn.core.server.QueryExecutor;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.exception.TransactionException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static grakn.core.commons.util.CommonUtil.toImmutableSet;

/**
 * Entry-point which communicates with a running Grakn server using gRPC.
 * For now, only a subset of {@link grakn.core.server.Session} and {@link grakn.core.server.Transaction} features are supported.
 */
public final class Grakn {
    public static final SimpleURI DEFAULT_URI = new SimpleURI("localhost:48555");

    private ManagedChannel channel;
    private Keyspaces keyspaces;

    public Grakn(SimpleURI uri) {
        // default: no benchmarking
        this(uri, false);
    }

    public Grakn(SimpleURI uri, boolean benchmark) {
        if (benchmark) {
            channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort())
                    .intercept(new ClientTracingInstrumentationInterceptor("client-java-instrumentation"))
                    .usePlaintext(true).build();
        } else {
            channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort())
                    .usePlaintext(true).build();
        }
        keyspaces = new Keyspaces(channel);
    }

    public Grakn(ManagedChannel channel) {
        this.channel = channel;
        keyspaces = new Keyspaces(channel);
    }

    public Session session(String keyspace) {
        return new Session(keyspace);
    }

    public Keyspaces keyspaces() {
        return keyspaces;
    }

    /**
     * Remote implementation of {@link grakn.core.server.Session} that communicates with a Grakn server using gRPC.
     *
     * @see Transaction
     * @see Grakn
     */
    public class Session implements grakn.core.server.Session {

        private final String keyspace;

        private Session(String keyspace) {
            if (!Validator.isValidKeyspaceName(keyspace)) {
                throw GraknClientException.invalidKeyspaceName(keyspace);
            }
            this.keyspace = keyspace;
        }

        @Override
        public Transaction transaction(grakn.core.server.Transaction.Type type) {
            return new Transaction(this, type);
        }

        @Override
        public void close() throws TransactionException {
            channel.shutdown();
        }

        @Override // TODO: remove this method once we no longer implement grakn.core.server.Session
        public grakn.core.server.keyspace.Keyspace keyspace() {
            return grakn.core.server.keyspace.Keyspace.of(keyspace);
        }
    }

    /**
     * Internal class used to handle keyspace related operations
     */

    public final class Keyspaces {

        private KeyspaceServiceBlockingStub keyspaceBlockingStub;

        private Keyspaces() {
            keyspaceBlockingStub = KeyspaceServiceGrpc.newBlockingStub(channel);
        }

        private Keyspaces(ManagedChannel channel) {
            keyspaceBlockingStub = KeyspaceServiceGrpc.newBlockingStub(channel);
        }

        public void delete(String name) {
            if (!Validator.isValidKeyspaceName(name)) {
                throw GraknClientException.invalidKeyspaceName(name);
            }
            KeyspaceProto.Keyspace.Delete.Req request = RequestBuilder.Keyspace.delete(name);
            keyspaceBlockingStub.delete(request);
        }
    }

    /**
     * Remote implementation of {@link grakn.core.server.Transaction} that communicates with a Grakn server using gRPC.
     */
    public final class Transaction implements grakn.core.server.Transaction {

        private final Session session;
        private final Type type;
        private final Transceiver transceiver;

        private Transaction(Session session, Type type) {
            this.session = session;
            this.type = type;
            this.transceiver = Transceiver.create(SessionServiceGrpc.newStub(channel));
            transceiver.send(RequestBuilder.Transaction.open(session.keyspace(), type));
            responseOrThrow();
        }

        @Override
        public Type txType() {
            return type;
        }

        @Override
        public grakn.core.server.Session session() {
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
        public <T extends grakn.core.graql.concept.Type> T getType(Label label) {
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

        public <T> Iterator<T> iterator(int iteratorId, Function<SessionProto.Transaction.Iter.Res, T> responseReader) {
            return new Iterator<>(this, iteratorId, responseReader);
        }

        /**
         * A client-side iterator over gRPC messages. Will send {@link SessionProto.Transaction.Iter.Req} messages until
         * {@link SessionProto.Transaction.Iter.Res} returns done as a message.
         *
         * @param <T> class type of objects being iterated
         */
        public class Iterator<T> extends AbstractIterator<T> {
            private final int iteratorId;
            private Transaction tx;
            private Function<SessionProto.Transaction.Iter.Res, T> responseReader;

            private Iterator(Transaction tx, int iteratorId, Function<SessionProto.Transaction.Iter.Res, T> responseReader) {
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
