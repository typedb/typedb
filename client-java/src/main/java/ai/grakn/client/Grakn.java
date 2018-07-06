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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.client;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.QueryExecutor;
import ai.grakn.client.executor.RemoteQueryExecutor;
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.client.rpc.RequestBuilder;
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
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.KeyspaceGrpc;
import ai.grakn.rpc.proto.KeyspaceProto;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.SessionGrpc;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.AbstractIterator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Entry-point and client equivalent of {@link ai.grakn.Grakn}. Communicates with a running Grakn server using gRPC.
 * In the future, this will likely become the default entry-point over {@link ai.grakn.Grakn}. For now, only a
 * subset of {@link GraknSession} and {@link ai.grakn.GraknTx} features are supported.
 */
public final class Grakn {

    private Grakn() {}

    public static Grakn.Session session(SimpleURI uri, Keyspace keyspace) {
        return new Session(uri, keyspace);
    }

    /**
     * Remote implementation of {@link GraknSession} that communicates with a Grakn server using gRPC.
     *
     * @see Transaction
     * @see Grakn
     */
    public static class Session implements GraknSession {

        private final Keyspace keyspace;
        private final SimpleURI uri;
        private final ManagedChannel channel;

        private Session(SimpleURI uri, Keyspace keyspace) {
            this.keyspace = keyspace;
            this.uri = uri;
            this.channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext(true).build();
        }

        SessionGrpc.SessionStub sessionStub() {
            return SessionGrpc.newStub(channel);
        }

        KeyspaceGrpc.KeyspaceBlockingStub keyspaceBlockingStub() {
            return KeyspaceGrpc.newBlockingStub(channel);
        }

        @Override
        public Transaction transaction(GraknTxType type) {
            return new Transaction(this, type);
        }

        @Override
        public void close() throws GraknTxOperationException {
            channel.shutdown();
        }

        @Override
        public String uri() {
            return uri.toString();
        }

        @Override
        public Keyspace keyspace() {
            return keyspace;
        }
    }

    /**
     * Remote implementation of {@link GraknTx} and {@link GraknAdmin} that communicates with a Grakn server using gRPC.
     */
    public static final class Transaction implements GraknTx, GraknAdmin {

        private final Session session;
        private final GraknTxType type;
        private final Transceiver transceiver;

        private Transaction(Session session, GraknTxType type) {
            this.session = session;
            this.type = type;
            this.transceiver = Transceiver.create(session.sessionStub());
            transceiver.send(RequestBuilder.Transaction.open(session.keyspace(), type));
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

        @Override // TODO: Move out of Transaction class
        public void delete() {
            KeyspaceProto.Delete.Req request = RequestBuilder.Keyspace.delete(keyspace().getValue());
            KeyspaceGrpc.KeyspaceBlockingStub stub = session.keyspaceBlockingStub();
            stub.delete(request);
            close();
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

            switch (txResponse.getQuery().getResCase()) {
                case NULL:
                    return Collections.emptyIterator();
                case ITERATORID:
                    IteratorProto.IteratorId iteratorId = txResponse.getQuery().getIteratorId();
                    return new Iterator<>(this, iteratorId, response -> ConceptBuilder.answer(response.getAnswer(), this));
                default:
                    throw CommonUtil.unreachableStatement("Unexpected " + txResponse);
            }
        }

        @Nullable
        @Override
        public <T extends Type> T getType(Label label) {
            return getSchemaConcept(label);
        }

        @Nullable
        @Override
        public EntityType getEntityType(String label) {
            return getSchemaConcept(Label.of(label));
        }

        @Nullable
        @Override
        public RelationshipType getRelationshipType(String label) {
            return getSchemaConcept(Label.of(label));
        }

        @Nullable
        @Override
        public <V> AttributeType<V> getAttributeType(String label) {
            return getSchemaConcept(Label.of(label));
        }

        @Nullable
        @Override
        public Role getRole(String label) {
            return getSchemaConcept(Label.of(label));
        }

        @Nullable
        @Override
        public Rule getRule(String label) {
            return getSchemaConcept(Label.of(label));
        }

        @Nullable
        @Override
        public <T extends SchemaConcept> T getSchemaConcept(Label label) {
            transceiver.send(RequestBuilder.Transaction.getSchemaConcept(label));
            SessionProto.Transaction.Res response = responseOrThrow();
            switch (response.getGetSchemaConcept().getResCase()) {
                case NULL:
                    return null;
                default:
                    return (T) ConceptBuilder.concept(response.getGetSchemaConcept().getConcept(), this);
            }
        }

        @Nullable
        @Override
        public <T extends Concept> T getConcept(ConceptId id) {
            transceiver.send(RequestBuilder.Transaction.getConcept(id));
            SessionProto.Transaction.Res response = responseOrThrow();
            switch (response.getGetConcept().getResCase()) {
                case NULL:
                    return null;
                default:
                    return (T) ConceptBuilder.concept(response.getGetConcept().getConcept(), this);
            }
        }

        @Override
        public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
            transceiver.send(RequestBuilder.Transaction.getAttributes(value));
            IteratorProto.IteratorId iteratorId = responseOrThrow().getIteratorId();
            Iterable<Concept> iterable = () -> new Iterator<>(
                    this, iteratorId, response -> ConceptBuilder.concept(response.getConcept(), this)
            );

            return StreamSupport.stream(iterable.spliterator(), false).map(Concept::<V>asAttribute).collect(toImmutableSet());
        }

        @Override
        public EntityType putEntityType(Label label) {
            transceiver.send(RequestBuilder.Transaction.putEntityType(label));
            return ConceptBuilder.concept(responseOrThrow().getPutEntityType().getConcept(), this).asEntityType();
        }

        @Override
        public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
            transceiver.send(RequestBuilder.Transaction.putAttributeType(label, dataType));
            return ConceptBuilder.concept(responseOrThrow().getPutAttributeType().getConcept(), this).asAttributeType();
        }

        @Override
        public RelationshipType putRelationshipType(Label label) {
            transceiver.send(RequestBuilder.Transaction.putRelationshipType(label));
            return ConceptBuilder.concept(responseOrThrow().getPutRelationshipType().getConcept(), this).asRelationshipType();
        }

        @Override
        public Role putRole(Label label) {
            transceiver.send(RequestBuilder.Transaction.putRole(label));
            return ConceptBuilder.concept(responseOrThrow().getPutRole().getConcept(), this).asRole();
        }

        @Override
        public Rule putRule(Label label, Pattern when, Pattern then) {
            transceiver.send(RequestBuilder.Transaction.putRule(label, when, then));
            return ConceptBuilder.concept(responseOrThrow().getPutRule().getConcept(), this).asRule();
        }

        @Override
        public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
            ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                    .setGetSuperConcepts(ConceptProto.GetSuperConcepts.Req.getDefaultInstance()).build();

            SessionProto.Transaction.Res response = runConceptMethod(schemaConcept.id(), method);
            IteratorProto.IteratorId iteratorId = response.getConceptMethod().getResponse().getGetSuperConcepts().getIteratorId();

            System.out.println("ooo" + iteratorId);

            Iterable<? extends Concept> iterable = () -> new Iterator<>(
                    this, iteratorId, res -> ConceptBuilder.concept(res.getConcept(), this)
            );

            Stream<? extends Concept> sups = StreamSupport.stream(iterable.spliterator(), false);
            return Objects.requireNonNull(sups).map(Concept::asSchemaConcept);
        }

        public SessionProto.Transaction.Res runConceptMethod(ConceptId id, ConceptProto.Method.Req method) {
            SessionProto.ConceptMethod.Req conceptMethod = SessionProto.ConceptMethod.Req.newBuilder()
                    .setId(id.getValue()).setMethod(method).build();
            SessionProto.Transaction.Req request = SessionProto.Transaction.Req.newBuilder().setConceptMethod(conceptMethod).build();

            transceiver.send(request);
            return responseOrThrow();
        }

        private SessionProto.Transaction.Res next(IteratorProto.IteratorId iteratorId) {
            transceiver.send(RequestBuilder.Transaction.next(iteratorId));
            return responseOrThrow();
        }

        /**
         * A client-side iterator over gRPC messages. Will send {@link IteratorProto.Next} messages until it receives a {@link SessionProto.Done} message.
         *
         * @param <T> class type of objects being iterated
         */
        public static class Iterator<T> extends AbstractIterator<T> {
            private final IteratorProto.IteratorId iteratorId;
            private Transaction tx;
            private Function<SessionProto.Transaction.Res, T> responseReader;

            public Iterator(Transaction tx, IteratorProto.IteratorId iteratorId, Function<SessionProto.Transaction.Res, T> responseReader) {
                this.tx = tx;
                this.iteratorId = iteratorId;
                this.responseReader = responseReader;
            }

            @Override
            protected final T computeNext() {
                SessionProto.Transaction.Res response = tx.next(iteratorId);

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
