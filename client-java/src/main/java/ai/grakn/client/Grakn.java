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
import ai.grakn.client.executor.RemoteQueryExecutor;
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.client.rpc.RequestIterator;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcIterator;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.SimpleURI;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
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

        GraknGrpc.GraknStub stubAsync() {
            return GraknGrpc.newStub(channel);
        }

        GraknGrpc.GraknBlockingStub stubBlocking() {
            return GraknGrpc.newBlockingStub(channel);
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
            this.transceiver = Transceiver.create(session.stubAsync());
            transceiver.send(RequestBuilder.open(session.keyspace(), type));
            responseOrThrow();
        }

        private GrpcGrakn.TxResponse responseOrThrow() {
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


        public GrpcGrakn.TxResponse next(GrpcIterator.IteratorId iteratorId) {
            transceiver.send(RequestBuilder.next(iteratorId));
            return responseOrThrow();
        }

        public GrpcGrakn.TxResponse runConceptMethod(ConceptId id, GrpcConcept.ConceptMethod method) {
            GrpcGrakn.RunConceptMethod.Builder runConceptMethod = GrpcGrakn.RunConceptMethod.newBuilder();
            runConceptMethod.setId(id.getValue());
            runConceptMethod.setMethod(method);
            GrpcGrakn.TxRequest conceptMethodRequest = GrpcGrakn.TxRequest.newBuilder().setRunConceptMethod(runConceptMethod).build();

            transceiver.send(conceptMethodRequest);
            return responseOrThrow();
        }

        @Override
        public EntityType putEntityType(Label label) {
            transceiver.send(RequestBuilder.putEntityType(label));
            return ConceptBuilder.concept(responseOrThrow().getConcept(), this).asEntityType();
        }

        @Override
        public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
            transceiver.send(RequestBuilder.putAttributeType(label, dataType));
            return ConceptBuilder.concept(responseOrThrow().getConcept(), this).asAttributeType();
        }

        @Override
        public Rule putRule(Label label, Pattern when, Pattern then) {
            transceiver.send(RequestBuilder.putRule(label, when, then));
            return ConceptBuilder.concept(responseOrThrow().getConcept(), this).asRule();
        }

        @Override
        public RelationshipType putRelationshipType(Label label) {
            transceiver.send(RequestBuilder.putRelationshipType(label));
            return ConceptBuilder.concept(responseOrThrow().getConcept(), this).asRelationshipType();
        }

        @Override
        public Role putRole(Label label) {
            transceiver.send(RequestBuilder.putRole(label));
            return ConceptBuilder.concept(responseOrThrow().getConcept(), this).asRole();
        }

        @Nullable
        @Override
        public <T extends Concept> T getConcept(ConceptId id) {
            transceiver.send(RequestBuilder.getConcept(id));
            GrpcGrakn.TxResponse response = responseOrThrow();
            if (response.getNoResult()) return null;
            return (T) ConceptBuilder.concept(response.getConcept(), this);
        }

        @Nullable
        @Override
        public <T extends SchemaConcept> T getSchemaConcept(Label label) {
            transceiver.send(RequestBuilder.getSchemaConcept(label));
            GrpcGrakn.TxResponse response = responseOrThrow();
            if (response.getNoResult()) return null;
            return (T) ConceptBuilder.concept(response.getConcept(), this);
        }

        @Nullable
        @Override
        public <T extends Type> T getType(Label label) {
            return getSchemaConcept(label);
        }

        @Override
        public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
            transceiver.send(RequestBuilder.getAttributesByValue(value));
            GrpcIterator.IteratorId iteratorId = responseOrThrow().getIteratorId();
            Iterable<Concept> iterable = () -> new RequestIterator<>(
                    this, iteratorId, response -> ConceptBuilder.concept(response.getConcept(), this)
            );

            return StreamSupport.stream(iterable.spliterator(), false).map(Concept::<V>asAttribute).collect(toImmutableSet());
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
        public boolean isClosed() {
            return transceiver.isClosed();
        }

        @Override
        public QueryBuilder graql() {
            return new QueryBuilderImpl(this);
        }

        @Override
        public void close() {
            transceiver.close();
        }

        @Override
        public void commit() throws InvalidKBException {
            transceiver.send(RequestBuilder.commit());
            responseOrThrow();
            close();
        }

        @Override
        public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
            GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
            method.setGetSuperConcepts(GrpcConcept.Unit.getDefaultInstance());
            GrpcIterator.IteratorId iteratorId = runConceptMethod(schemaConcept.getId(), method.build()).getConceptResponse().getIteratorId();
            Iterable<? extends Concept> iterable = () -> new RequestIterator<>(
                    this, iteratorId, res -> ConceptBuilder.concept(res.getConcept(), this)
            );

            Stream<? extends Concept> sups = StreamSupport.stream(iterable.spliterator(), false);
            return Objects.requireNonNull(sups).map(Concept::asSchemaConcept);
        }

        @Override
        public void delete() {
            GrpcGrakn.DeleteRequest request = RequestBuilder.delete(RequestBuilder.open(keyspace(), GraknTxType.WRITE).getOpen());
            session.stubBlocking().delete(request);
            close();
        }

        @Override
        public QueryExecutor queryExecutor() {
            return RemoteQueryExecutor.create(this);
        }

        public Iterator query(Query<?> query) {
            transceiver.send(RequestBuilder.query(query.toString(), query.inferring()));

            GrpcGrakn.TxResponse txResponse = responseOrThrow();

            switch (txResponse.getResponseCase()) {
                case ANSWER:
                    return Collections.singleton(ConceptBuilder.answer(txResponse.getAnswer(), this)).iterator();
                case DONE:
                    return Collections.emptyIterator();
                case ITERATORID:
                    GrpcIterator.IteratorId iteratorId = txResponse.getIteratorId();
                    return new RequestIterator<>(this, iteratorId, response -> ConceptBuilder.answer(response.getAnswer(), this));
                default:
                    throw CommonUtil.unreachableStatement("Unexpected " + txResponse);
            }
        }

    }
}
