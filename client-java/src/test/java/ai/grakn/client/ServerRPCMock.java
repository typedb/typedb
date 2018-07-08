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

import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.rpc.proto.KeyspaceProto;
import ai.grakn.rpc.proto.KeyspaceServiceGrpc;
import ai.grakn.rpc.proto.KeyspaceServiceGrpc.KeyspaceServiceImplBase;
import ai.grakn.rpc.proto.SessionGrpc.SessionImplBase;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.rpc.proto.SessionProto.Transaction;
import ai.grakn.test.rule.CompositeTestRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.rules.TestRule;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Semi-mocked gRPC server that can handle transactions.
 *
 * <p>
 *     The gRPC server itself is "real" and can be connected to using the {@link #channel()}. However, the
 *     {@link #sessionService()} and {@link #requestListener()} are both mock objects and should be used with
 *     {@link org.mockito.Mockito#verify(Object)}.
 * </p>
 * <p>
 *     By default, the server will return a "done" {@link Transaction.Res} to every message. And will respond
 *     with {@link StreamObserver#onCompleted()} when receiving a {@link StreamObserver#onCompleted()} from the client.
 * </p>
 * <p>
 *     In order to mock additional responses, use the method {@link #setResponse(Transaction.Req, Transaction.Res...)}.
 * </p>
 *
 * @author Felix Chapman
 */
public final class ServerRPCMock extends CompositeTestRule {

    private int iteratorIdCounter = 0;
    private final ServerIteratorsMock rpcIterators = ServerIteratorsMock.create();
    private final GrpcServerRule serverRule = new GrpcServerRule().directExecutor();
    private final SessionImplBase sessionService = mock(SessionImplBase.class);
    private final KeyspaceServiceImplBase keyspaceService = mock(KeyspaceServiceGrpc.KeyspaceServiceImplBase.class);

    private @Nullable StreamObserver<Transaction.Res> serverResponses = null;

    @SuppressWarnings("unchecked") // safe because mock
    private StreamObserver<Transaction.Req> reqeustListener = mock(StreamObserver.class);

    private ServerRPCMock() {
    }

    public static ServerRPCMock create() {
        return new ServerRPCMock();
    }

    public ManagedChannel channel() {
        return serverRule.getChannel();
    }

    SessionImplBase sessionService() {
        return sessionService;
    }

    KeyspaceServiceImplBase keyspaceService() {
        return keyspaceService;
    }

    public StreamObserver<Transaction.Req> requestListener() {
        return reqeustListener;
    }

    public ServerIteratorsMock IteratorProtos() {
        return rpcIterators;
    }

    public void setResponse(Transaction.Req request, Transaction.Res... responses) {
        setResponse(request, Arrays.asList(responses));
    }

    public void setResponseSequence(Transaction.Req request, Transaction.Res... responses) {
        setResponseHandlers(request, Collections.singletonList(TxResponseHandler.sequence(this, responses)));
    }

    public void setResponse(Transaction.Req request, Throwable throwable) {
        setResponseHandlers(request, ImmutableList.of(TxResponseHandler.onError(throwable)));
    }

    private void setResponse(Transaction.Req request, List<Transaction.Res> responses) {
        setResponseHandlers(request, Lists.transform(responses, TxResponseHandler::onNext));
    }

    private void setResponseHandlers(Transaction.Req request, List<TxResponseHandler> responses) {
        Supplier<TxResponseHandler> next;

        // If there is only one mocked response, just return it again and again
        if (responses.size() > 1) {
            Iterator<TxResponseHandler> iterator = responses.iterator();
            next = iterator::next;
        } else {
            next = () -> Iterables.getOnlyElement(responses);
        }

        doAnswer(args -> {
            if (serverResponses == null) {
                throw new IllegalArgumentException("Set-up of rule not called");
            }
            next.get().handle(serverResponses);
            return null;
        }).when(reqeustListener).onNext(request);
    }

    private interface TxResponseHandler {
        static TxResponseHandler onNext(Transaction.Res response) {
            return streamObserver -> streamObserver.onNext(response);
        }

        static TxResponseHandler onError(Throwable throwable) {
            return streamObserver -> streamObserver.onError(throwable);
        }

        static TxResponseHandler sequence(ServerRPCMock server, Transaction.Res... responses) {
            int iteratorId = server.IteratorProtos().add(Iterators.forArray(responses));

            return streamObserver -> {
                List<Transaction.Res> responsesList =
                        ImmutableList.<Transaction.Res>builder().add(responses).add(done()).build();

                server.setResponse(RequestBuilder.Transaction.next(iteratorId), responsesList);
                streamObserver.onNext(SessionProto.Transaction.Res.newBuilder().setIteratorId(iteratorId).build());
            };
        }

        void handle(StreamObserver<Transaction.Res> streamObserver);
    }

    @Override
    protected List<TestRule> testRules() {
        return ImmutableList.of(serverRule);
    }

    @Override
    protected void before() throws Throwable {
        doAnswer(args -> {
            serverResponses = args.getArgument(0);
            return reqeustListener;
        }).when(sessionService).transaction(any());

        doAnswer(args -> {
            StreamObserver<KeyspaceProto.Keyspace.Delete.Res> response = args.getArgument(1);
            response.onNext(KeyspaceProto.Keyspace.Delete.Res.newBuilder().build());
            response.onCompleted();
            return null;
        }).when(keyspaceService).delete(any(), any());

        // Return a default "done" response to every message from the client
        doAnswer(args -> {
            if (serverResponses == null) {
                throw new IllegalArgumentException("Set-up of rule not called");
            }

            Transaction.Req request = args.getArgument(0);

            Optional<Transaction.Res> next = rpcIterators.next(request.getNext().getIteratorId());
            serverResponses.onNext(next.orElse(done()));

            return null;
        }).when(reqeustListener).onNext(any());

        // Return a default "complete" response to every "complete" message from the client
        doAnswer(args -> {
            if (serverResponses == null) {
                throw new IllegalArgumentException("Set-up of rule not called");
            }
            serverResponses.onCompleted();
            return null;
        }).when(reqeustListener).onCompleted();

        serverRule.getServiceRegistry().addService(sessionService);
        serverRule.getServiceRegistry().addService(keyspaceService);
    }

    @Override
    protected void after() {
        if (serverResponses != null) {
            try {
                serverResponses.onCompleted();
            } catch (IllegalStateException e) {
                // this occurs if something has already ended the call
            }
        }
    }

    private static SessionProto.Transaction.Res done() {
        return SessionProto.Transaction.Res.newBuilder().setDone(SessionProto.Transaction.Done.getDefaultInstance()).build();
    }

    /**
     * Contains a mutable map of iterators of {@link Transaction.Res}s for gRPC. These iterators are used for returning
     * lazy, streaming responses such as for Graql query results.
     *
     * @author Felix Chapman
     */
    public static class ServerIteratorsMock {
        private final AtomicInteger iteratorIdCounter = new AtomicInteger();
        private final Map<Integer, Iterator<Transaction.Res>> iterators = new ConcurrentHashMap<>();

        private ServerIteratorsMock() {
        }

        public static ServerIteratorsMock create() {
            return new ServerIteratorsMock();
        }

        /**
         * Register a new iterator and return the ID of the iterator
         */
        public int add(Iterator<Transaction.Res> iterator) {
            int iteratorId = iteratorIdCounter.getAndIncrement();

            iterators.put(iteratorId, iterator);
            return iteratorId;
        }

        /**
         * Return the next response from an iterator. Will return a {@link SessionProto.Transaction.Done} response if the iterator is exhausted.
         */
        public Optional<Transaction.Res> next(int iteratorId) {
            return Optional.ofNullable(iterators.get(iteratorId)).map(iterator -> {
                Transaction.Res response;

                if (iterator.hasNext()) {
                    response = iterator.next();
                } else {
                    response = done();
                    stop(iteratorId);
                }

                return response;
            });
        }

        /**
         * Stop an iterator
         */
        public void stop(int iteratorId) {
            iterators.remove(iteratorId);
        }
    }
}
